#![coverage(off)]

use rustc_hash::FxHashMap;
use yachtsql_ir::{Expr, LogicalPlan, WhenClause};

use super::Planner;
use crate::CatalogProvider;

impl<'a, C: CatalogProvider> Planner<'a, C> {
    pub(super) fn substitute_params_in_plan(
        plan: LogicalPlan,
        bindings: &FxHashMap<String, Expr>,
    ) -> LogicalPlan {
        match plan {
            LogicalPlan::Project {
                input,
                expressions,
                schema,
            } => LogicalPlan::Project {
                input: Box::new(Self::substitute_params_in_plan(*input, bindings)),
                expressions: expressions
                    .into_iter()
                    .map(|e| Self::substitute_params_in_expr(e, bindings))
                    .collect(),
                schema,
            },
            LogicalPlan::Filter { input, predicate } => LogicalPlan::Filter {
                input: Box::new(Self::substitute_params_in_plan(*input, bindings)),
                predicate: Self::substitute_params_in_expr(predicate, bindings),
            },
            LogicalPlan::Values { values, schema } => LogicalPlan::Values {
                values: values
                    .into_iter()
                    .map(|row| {
                        row.into_iter()
                            .map(|e| Self::substitute_params_in_expr(e, bindings))
                            .collect()
                    })
                    .collect(),
                schema,
            },
            other => other,
        }
    }

    pub(super) fn substitute_params_in_expr(
        expr: Expr,
        bindings: &FxHashMap<String, Expr>,
    ) -> Expr {
        match expr {
            Expr::Column { ref name, .. } => {
                if let Some(replacement) = bindings.get(&name.to_uppercase()) {
                    replacement.clone()
                } else {
                    expr
                }
            }
            Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
                left: Box::new(Self::substitute_params_in_expr(*left, bindings)),
                op,
                right: Box::new(Self::substitute_params_in_expr(*right, bindings)),
            },
            Expr::UnaryOp { op, expr: inner } => Expr::UnaryOp {
                op,
                expr: Box::new(Self::substitute_params_in_expr(*inner, bindings)),
            },
            Expr::ScalarFunction { name, args } => Expr::ScalarFunction {
                name,
                args: args
                    .into_iter()
                    .map(|e| Self::substitute_params_in_expr(e, bindings))
                    .collect(),
            },
            Expr::Case {
                operand,
                when_clauses,
                else_result,
            } => Expr::Case {
                operand: operand.map(|e| Box::new(Self::substitute_params_in_expr(*e, bindings))),
                when_clauses: when_clauses
                    .into_iter()
                    .map(|wc| WhenClause {
                        condition: Self::substitute_params_in_expr(wc.condition, bindings),
                        result: Self::substitute_params_in_expr(wc.result, bindings),
                    })
                    .collect(),
                else_result: else_result
                    .map(|e| Box::new(Self::substitute_params_in_expr(*e, bindings))),
            },
            Expr::Cast {
                expr: inner,
                data_type,
                safe,
            } => Expr::Cast {
                expr: Box::new(Self::substitute_params_in_expr(*inner, bindings)),
                data_type,
                safe,
            },
            Expr::InList {
                expr: inner,
                list,
                negated,
            } => Expr::InList {
                expr: Box::new(Self::substitute_params_in_expr(*inner, bindings)),
                list: list
                    .into_iter()
                    .map(|e| Self::substitute_params_in_expr(e, bindings))
                    .collect(),
                negated,
            },
            Expr::IsNull {
                expr: inner,
                negated,
            } => Expr::IsNull {
                expr: Box::new(Self::substitute_params_in_expr(*inner, bindings)),
                negated,
            },
            Expr::Between {
                expr: inner,
                negated,
                low,
                high,
            } => Expr::Between {
                expr: Box::new(Self::substitute_params_in_expr(*inner, bindings)),
                negated,
                low: Box::new(Self::substitute_params_in_expr(*low, bindings)),
                high: Box::new(Self::substitute_params_in_expr(*high, bindings)),
            },
            Expr::Struct { fields } => Expr::Struct {
                fields: fields
                    .into_iter()
                    .map(|(name, e)| (name, Self::substitute_params_in_expr(e, bindings)))
                    .collect(),
            },
            Expr::Array {
                elements,
                element_type,
            } => Expr::Array {
                elements: elements
                    .into_iter()
                    .map(|e| Self::substitute_params_in_expr(e, bindings))
                    .collect(),
                element_type,
            },
            Expr::Alias { expr: inner, name } => Expr::Alias {
                expr: Box::new(Self::substitute_params_in_expr(*inner, bindings)),
                name,
            },
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use rustc_hash::FxHashMap;
    use yachtsql_common::types::{DataType, Schema};
    use yachtsql_ir::{
        BinaryOp, Expr, Literal, LogicalPlan, PlanField, PlanSchema, ScalarFunction, UnaryOp,
        WhenClause,
    };

    use super::super::Planner;
    use crate::{CatalogProvider, FunctionDefinition, ViewDefinition};

    struct MockCatalog;

    impl CatalogProvider for MockCatalog {
        fn get_table_schema(&self, _name: &str) -> Option<Schema> {
            None
        }

        fn get_view(&self, _name: &str) -> Option<ViewDefinition> {
            None
        }

        fn get_function(&self, _name: &str) -> Option<FunctionDefinition> {
            None
        }
    }

    fn make_bindings() -> FxHashMap<String, Expr> {
        let mut bindings = FxHashMap::default();
        bindings.insert("PARAM1".to_string(), Expr::literal_i64(42));
        bindings.insert("PARAM2".to_string(), Expr::literal_string("hello"));
        bindings
    }

    #[test]
    fn test_substitute_params_in_expr_column_match() {
        let bindings = make_bindings();
        let expr = Expr::Column {
            table: None,
            name: "param1".to_string(),
            index: None,
        };
        let result = Planner::<MockCatalog>::substitute_params_in_expr(expr, &bindings);
        assert_eq!(result, Expr::literal_i64(42));
    }

    #[test]
    fn test_substitute_params_in_expr_column_no_match() {
        let bindings = make_bindings();
        let expr = Expr::Column {
            table: Some("t".to_string()),
            name: "other_col".to_string(),
            index: Some(0),
        };
        let result = Planner::<MockCatalog>::substitute_params_in_expr(expr.clone(), &bindings);
        assert_eq!(result, expr);
    }

    #[test]
    fn test_substitute_params_in_expr_binary_op() {
        let bindings = make_bindings();
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column {
                table: None,
                name: "param1".to_string(),
                index: None,
            }),
            op: BinaryOp::Add,
            right: Box::new(Expr::literal_i64(10)),
        };
        let result = Planner::<MockCatalog>::substitute_params_in_expr(expr, &bindings);
        assert_eq!(
            result,
            Expr::BinaryOp {
                left: Box::new(Expr::literal_i64(42)),
                op: BinaryOp::Add,
                right: Box::new(Expr::literal_i64(10)),
            }
        );
    }

    #[test]
    fn test_substitute_params_in_expr_unary_op() {
        let bindings = make_bindings();
        let expr = Expr::UnaryOp {
            op: UnaryOp::Minus,
            expr: Box::new(Expr::Column {
                table: None,
                name: "param1".to_string(),
                index: None,
            }),
        };
        let result = Planner::<MockCatalog>::substitute_params_in_expr(expr, &bindings);
        assert_eq!(
            result,
            Expr::UnaryOp {
                op: UnaryOp::Minus,
                expr: Box::new(Expr::literal_i64(42)),
            }
        );
    }

    #[test]
    fn test_substitute_params_in_expr_scalar_function() {
        let bindings = make_bindings();
        let expr = Expr::ScalarFunction {
            name: ScalarFunction::Abs,
            args: vec![Expr::Column {
                table: None,
                name: "param1".to_string(),
                index: None,
            }],
        };
        let result = Planner::<MockCatalog>::substitute_params_in_expr(expr, &bindings);
        assert_eq!(
            result,
            Expr::ScalarFunction {
                name: ScalarFunction::Abs,
                args: vec![Expr::literal_i64(42)],
            }
        );
    }

    #[test]
    fn test_substitute_params_in_expr_case_with_operand() {
        let bindings = make_bindings();
        let expr = Expr::Case {
            operand: Some(Box::new(Expr::Column {
                table: None,
                name: "param1".to_string(),
                index: None,
            })),
            when_clauses: vec![WhenClause {
                condition: Expr::literal_i64(42),
                result: Expr::Column {
                    table: None,
                    name: "param2".to_string(),
                    index: None,
                },
            }],
            else_result: Some(Box::new(Expr::literal_string("default"))),
        };
        let result = Planner::<MockCatalog>::substitute_params_in_expr(expr, &bindings);
        assert_eq!(
            result,
            Expr::Case {
                operand: Some(Box::new(Expr::literal_i64(42))),
                when_clauses: vec![WhenClause {
                    condition: Expr::literal_i64(42),
                    result: Expr::literal_string("hello"),
                }],
                else_result: Some(Box::new(Expr::literal_string("default"))),
            }
        );
    }

    #[test]
    fn test_substitute_params_in_expr_case_without_operand() {
        let bindings = make_bindings();
        let expr = Expr::Case {
            operand: None,
            when_clauses: vec![WhenClause {
                condition: Expr::Column {
                    table: None,
                    name: "param1".to_string(),
                    index: None,
                },
                result: Expr::literal_i64(1),
            }],
            else_result: None,
        };
        let result = Planner::<MockCatalog>::substitute_params_in_expr(expr, &bindings);
        assert_eq!(
            result,
            Expr::Case {
                operand: None,
                when_clauses: vec![WhenClause {
                    condition: Expr::literal_i64(42),
                    result: Expr::literal_i64(1),
                }],
                else_result: None,
            }
        );
    }

    #[test]
    fn test_substitute_params_in_expr_cast() {
        let bindings = make_bindings();
        let expr = Expr::Cast {
            expr: Box::new(Expr::Column {
                table: None,
                name: "param1".to_string(),
                index: None,
            }),
            data_type: DataType::String,
            safe: false,
        };
        let result = Planner::<MockCatalog>::substitute_params_in_expr(expr, &bindings);
        assert_eq!(
            result,
            Expr::Cast {
                expr: Box::new(Expr::literal_i64(42)),
                data_type: DataType::String,
                safe: false,
            }
        );
    }

    #[test]
    fn test_substitute_params_in_expr_in_list() {
        let bindings = make_bindings();
        let expr = Expr::InList {
            expr: Box::new(Expr::Column {
                table: None,
                name: "param1".to_string(),
                index: None,
            }),
            list: vec![
                Expr::literal_i64(1),
                Expr::Column {
                    table: None,
                    name: "param1".to_string(),
                    index: None,
                },
            ],
            negated: true,
        };
        let result = Planner::<MockCatalog>::substitute_params_in_expr(expr, &bindings);
        assert_eq!(
            result,
            Expr::InList {
                expr: Box::new(Expr::literal_i64(42)),
                list: vec![Expr::literal_i64(1), Expr::literal_i64(42),],
                negated: true,
            }
        );
    }

    #[test]
    fn test_substitute_params_in_expr_is_null() {
        let bindings = make_bindings();
        let expr = Expr::IsNull {
            expr: Box::new(Expr::Column {
                table: None,
                name: "param1".to_string(),
                index: None,
            }),
            negated: false,
        };
        let result = Planner::<MockCatalog>::substitute_params_in_expr(expr, &bindings);
        assert_eq!(
            result,
            Expr::IsNull {
                expr: Box::new(Expr::literal_i64(42)),
                negated: false,
            }
        );
    }

    #[test]
    fn test_substitute_params_in_expr_between() {
        let bindings = make_bindings();
        let expr = Expr::Between {
            expr: Box::new(Expr::Column {
                table: None,
                name: "param1".to_string(),
                index: None,
            }),
            negated: false,
            low: Box::new(Expr::literal_i64(10)),
            high: Box::new(Expr::Column {
                table: None,
                name: "param1".to_string(),
                index: None,
            }),
        };
        let result = Planner::<MockCatalog>::substitute_params_in_expr(expr, &bindings);
        assert_eq!(
            result,
            Expr::Between {
                expr: Box::new(Expr::literal_i64(42)),
                negated: false,
                low: Box::new(Expr::literal_i64(10)),
                high: Box::new(Expr::literal_i64(42)),
            }
        );
    }

    #[test]
    fn test_substitute_params_in_expr_struct() {
        let bindings = make_bindings();
        let expr = Expr::Struct {
            fields: vec![
                (
                    Some("a".to_string()),
                    Expr::Column {
                        table: None,
                        name: "param1".to_string(),
                        index: None,
                    },
                ),
                (None, Expr::literal_i64(100)),
            ],
        };
        let result = Planner::<MockCatalog>::substitute_params_in_expr(expr, &bindings);
        assert_eq!(
            result,
            Expr::Struct {
                fields: vec![
                    (Some("a".to_string()), Expr::literal_i64(42)),
                    (None, Expr::literal_i64(100)),
                ],
            }
        );
    }

    #[test]
    fn test_substitute_params_in_expr_array() {
        let bindings = make_bindings();
        let expr = Expr::Array {
            elements: vec![
                Expr::Column {
                    table: None,
                    name: "param1".to_string(),
                    index: None,
                },
                Expr::literal_i64(2),
            ],
            element_type: Some(DataType::Int64),
        };
        let result = Planner::<MockCatalog>::substitute_params_in_expr(expr, &bindings);
        assert_eq!(
            result,
            Expr::Array {
                elements: vec![Expr::literal_i64(42), Expr::literal_i64(2),],
                element_type: Some(DataType::Int64),
            }
        );
    }

    #[test]
    fn test_substitute_params_in_expr_alias() {
        let bindings = make_bindings();
        let expr = Expr::Alias {
            expr: Box::new(Expr::Column {
                table: None,
                name: "param1".to_string(),
                index: None,
            }),
            name: "aliased".to_string(),
        };
        let result = Planner::<MockCatalog>::substitute_params_in_expr(expr, &bindings);
        assert_eq!(
            result,
            Expr::Alias {
                expr: Box::new(Expr::literal_i64(42)),
                name: "aliased".to_string(),
            }
        );
    }

    #[test]
    fn test_substitute_params_in_expr_literal_unchanged() {
        let bindings = make_bindings();
        let expr = Expr::Literal(Literal::Int64(100));
        let result = Planner::<MockCatalog>::substitute_params_in_expr(expr.clone(), &bindings);
        assert_eq!(result, expr);
    }

    #[test]
    fn test_substitute_params_in_plan_project() {
        let bindings = make_bindings();
        let schema = PlanSchema::from_fields(vec![PlanField::new("col1", DataType::Int64)]);
        let input = LogicalPlan::Empty {
            schema: PlanSchema::new(),
        };
        let plan = LogicalPlan::Project {
            input: Box::new(input),
            expressions: vec![Expr::Column {
                table: None,
                name: "param1".to_string(),
                index: None,
            }],
            schema: schema.clone(),
        };
        let result = Planner::<MockCatalog>::substitute_params_in_plan(plan, &bindings);
        let LogicalPlan::Project { expressions, .. } = result else {
            unreachable!("Expected Project plan");
        };
        assert_eq!(expressions, vec![Expr::literal_i64(42)]);
    }

    #[test]
    fn test_substitute_params_in_plan_filter() {
        let bindings = make_bindings();
        let input = LogicalPlan::Empty {
            schema: PlanSchema::new(),
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(input),
            predicate: Expr::BinaryOp {
                left: Box::new(Expr::Column {
                    table: None,
                    name: "param1".to_string(),
                    index: None,
                }),
                op: BinaryOp::Gt,
                right: Box::new(Expr::literal_i64(0)),
            },
        };
        let result = Planner::<MockCatalog>::substitute_params_in_plan(plan, &bindings);
        let LogicalPlan::Filter { predicate, .. } = result else {
            unreachable!("Expected Filter plan");
        };
        assert_eq!(
            predicate,
            Expr::BinaryOp {
                left: Box::new(Expr::literal_i64(42)),
                op: BinaryOp::Gt,
                right: Box::new(Expr::literal_i64(0)),
            }
        );
    }

    #[test]
    fn test_substitute_params_in_plan_values() {
        let bindings = make_bindings();
        let schema = PlanSchema::from_fields(vec![
            PlanField::new("col1", DataType::Int64),
            PlanField::new("col2", DataType::String),
        ]);
        let plan = LogicalPlan::Values {
            values: vec![
                vec![
                    Expr::Column {
                        table: None,
                        name: "param1".to_string(),
                        index: None,
                    },
                    Expr::Column {
                        table: None,
                        name: "param2".to_string(),
                        index: None,
                    },
                ],
                vec![Expr::literal_i64(1), Expr::literal_string("world")],
            ],
            schema,
        };
        let result = Planner::<MockCatalog>::substitute_params_in_plan(plan, &bindings);
        let LogicalPlan::Values { values, .. } = result else {
            unreachable!("Expected Values plan");
        };
        assert_eq!(
            values,
            vec![
                vec![Expr::literal_i64(42), Expr::literal_string("hello")],
                vec![Expr::literal_i64(1), Expr::literal_string("world")],
            ]
        );
    }

    #[test]
    fn test_substitute_params_in_plan_other_unchanged() {
        let bindings = make_bindings();
        let plan = LogicalPlan::Empty {
            schema: PlanSchema::new(),
        };
        let result = Planner::<MockCatalog>::substitute_params_in_plan(plan.clone(), &bindings);
        assert_eq!(result, plan);
    }

    #[test]
    fn test_substitute_params_nested_plan() {
        let bindings = make_bindings();
        let schema = PlanSchema::from_fields(vec![PlanField::new("col1", DataType::Int64)]);
        let inner = LogicalPlan::Filter {
            input: Box::new(LogicalPlan::Empty {
                schema: PlanSchema::new(),
            }),
            predicate: Expr::Column {
                table: None,
                name: "param1".to_string(),
                index: None,
            },
        };
        let outer = LogicalPlan::Project {
            input: Box::new(inner),
            expressions: vec![Expr::Column {
                table: None,
                name: "param2".to_string(),
                index: None,
            }],
            schema,
        };
        let result = Planner::<MockCatalog>::substitute_params_in_plan(outer, &bindings);
        let LogicalPlan::Project {
            input, expressions, ..
        } = result
        else {
            unreachable!("Expected Project plan");
        };
        assert_eq!(expressions, vec![Expr::literal_string("hello")]);
        let LogicalPlan::Filter { predicate, .. } = *input else {
            unreachable!("Expected Filter plan");
        };
        assert_eq!(predicate, Expr::literal_i64(42));
    }

    #[test]
    fn test_substitute_params_case_insensitive() {
        let bindings = make_bindings();
        let expr = Expr::Column {
            table: None,
            name: "PARAM1".to_string(),
            index: None,
        };
        let result = Planner::<MockCatalog>::substitute_params_in_expr(expr, &bindings);
        assert_eq!(result, Expr::literal_i64(42));

        let expr2 = Expr::Column {
            table: None,
            name: "Param2".to_string(),
            index: None,
        };
        let result2 = Planner::<MockCatalog>::substitute_params_in_expr(expr2, &bindings);
        assert_eq!(result2, Expr::literal_string("hello"));
    }

    #[test]
    fn test_substitute_params_empty_bindings() {
        let bindings = FxHashMap::default();
        let expr = Expr::Column {
            table: None,
            name: "param1".to_string(),
            index: None,
        };
        let result = Planner::<MockCatalog>::substitute_params_in_expr(expr.clone(), &bindings);
        assert_eq!(result, expr);
    }

    #[test]
    fn test_substitute_params_deeply_nested_expr() {
        let bindings = make_bindings();
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::UnaryOp {
                op: UnaryOp::Minus,
                expr: Box::new(Expr::Cast {
                    expr: Box::new(Expr::Column {
                        table: None,
                        name: "param1".to_string(),
                        index: None,
                    }),
                    data_type: DataType::Float64,
                    safe: true,
                }),
            }),
            op: BinaryOp::Add,
            right: Box::new(Expr::Column {
                table: None,
                name: "param1".to_string(),
                index: None,
            }),
        };
        let result = Planner::<MockCatalog>::substitute_params_in_expr(expr, &bindings);
        assert_eq!(
            result,
            Expr::BinaryOp {
                left: Box::new(Expr::UnaryOp {
                    op: UnaryOp::Minus,
                    expr: Box::new(Expr::Cast {
                        expr: Box::new(Expr::literal_i64(42)),
                        data_type: DataType::Float64,
                        safe: true,
                    }),
                }),
                op: BinaryOp::Add,
                right: Box::new(Expr::literal_i64(42)),
            }
        );
    }
}
