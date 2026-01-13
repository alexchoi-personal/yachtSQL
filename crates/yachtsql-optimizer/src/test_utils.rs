use rustc_hash::FxHashMap;
use yachtsql_common::types::DataType;
use yachtsql_parser::{CatalogProvider, FunctionDefinition, ViewDefinition, parse_and_plan};
use yachtsql_storage::{Field, Schema};

use crate::{PhysicalPlan, optimize};

pub(crate) struct MockCatalog {
    tables: FxHashMap<String, Schema>,
}

impl MockCatalog {
    pub(crate) fn new() -> Self {
        Self {
            tables: FxHashMap::default(),
        }
    }

    pub(crate) fn with_table(mut self, name: &str, schema: Schema) -> Self {
        self.tables.insert(name.to_uppercase(), schema);
        self
    }
}

impl CatalogProvider for MockCatalog {
    fn get_table_schema(&self, name: &str) -> Option<Schema> {
        self.tables.get(&name.to_uppercase()).cloned()
    }

    fn get_view(&self, _name: &str) -> Option<ViewDefinition> {
        None
    }

    fn get_function(&self, _name: &str) -> Option<FunctionDefinition> {
        None
    }
}

fn orders_schema() -> Schema {
    Schema::from_fields(vec![
        Field::nullable("id", DataType::Int64),
        Field::nullable("customer_id", DataType::Int64),
        Field::nullable("amount", DataType::Float64),
        Field::nullable("status", DataType::String),
    ])
}

fn customers_schema() -> Schema {
    Schema::from_fields(vec![
        Field::nullable("id", DataType::Int64),
        Field::nullable("name", DataType::String),
        Field::nullable("country", DataType::String),
    ])
}

fn products_schema() -> Schema {
    Schema::from_fields(vec![
        Field::nullable("id", DataType::Int64),
        Field::nullable("name", DataType::String),
        Field::nullable("price", DataType::Float64),
        Field::nullable("category", DataType::String),
    ])
}

pub(crate) fn test_catalog() -> MockCatalog {
    MockCatalog::new()
        .with_table("orders", orders_schema())
        .with_table("customers", customers_schema())
        .with_table("products", products_schema())
}

pub(crate) fn optimize_sql<C: CatalogProvider>(sql: &str, catalog: &C) -> PhysicalPlan {
    let logical = parse_and_plan(sql, catalog).expect("failed to parse SQL");
    optimize(&logical).expect("failed to optimize plan")
}

pub(crate) fn optimize_sql_default(sql: &str) -> PhysicalPlan {
    optimize_sql(sql, &test_catalog())
}

macro_rules! assert_plan {
    ($plan:expr, _) => {};

    ($plan:expr, TableScan { table_name: $name:expr }) => {
        match &$plan {
            PhysicalPlan::TableScan { table_name, .. } => {
                assert_eq!(table_name, $name, "TableScan table_name mismatch");
            }
            other => panic!(
                "Expected TableScan, got {}\n\nActual plan:\n{}",
                $crate::test_utils::plan_node_name(other),
                $crate::test_utils::format_plan_tree(other, 0)
            ),
        }
    };

    ($plan:expr, TableScan { table_name: $name:expr, projection: $proj:expr }) => {
        match &$plan {
            PhysicalPlan::TableScan {
                table_name,
                projection,
                ..
            } => {
                assert_eq!(table_name, $name, "TableScan table_name mismatch");
                assert_eq!(projection, &$proj, "TableScan projection mismatch");
            }
            other => panic!(
                "Expected TableScan, got {}\n\nActual plan:\n{}",
                $crate::test_utils::plan_node_name(other),
                $crate::test_utils::format_plan_tree(other, 0)
            ),
        }
    };

    ($plan:expr, Filter { input: ($($input:tt)+) }) => {
        match &$plan {
            PhysicalPlan::Filter { input: _input, .. } => {
                assert_plan!(**_input, $($input)+);
            }
            other => panic!(
                "Expected Filter, got {}\n\nActual plan:\n{}",
                $crate::test_utils::plan_node_name(other),
                $crate::test_utils::format_plan_tree(other, 0)
            ),
        }
    };

    ($plan:expr, Filter { input: ($($input:tt)+), predicate: _ }) => {
        match &$plan {
            PhysicalPlan::Filter { input: _input, .. } => {
                assert_plan!(**_input, $($input)+);
            }
            other => panic!(
                "Expected Filter, got {}\n\nActual plan:\n{}",
                $crate::test_utils::plan_node_name(other),
                $crate::test_utils::format_plan_tree(other, 0)
            ),
        }
    };

    ($plan:expr, Filter { input: ($($input:tt)+), predicate: $pred_check:expr }) => {
        match &$plan {
            PhysicalPlan::Filter { input, predicate } => {
                assert!($pred_check(predicate), "Filter predicate check failed: {:?}", predicate);
                assert_plan!(**input, $($input)+);
            }
            other => panic!(
                "Expected Filter, got {}\n\nActual plan:\n{}",
                $crate::test_utils::plan_node_name(other),
                $crate::test_utils::format_plan_tree(other, 0)
            ),
        }
    };

    ($plan:expr, Project { input: ($($input:tt)+) }) => {
        match &$plan {
            PhysicalPlan::Project { input: _input, .. } => {
                assert_plan!(**_input, $($input)+);
            }
            other => panic!(
                "Expected Project, got {}\n\nActual plan:\n{}",
                $crate::test_utils::plan_node_name(other),
                $crate::test_utils::format_plan_tree(other, 0)
            ),
        }
    };

    ($plan:expr, Project { input: ($($input:tt)+), expressions: $expr_check:expr }) => {
        match &$plan {
            PhysicalPlan::Project { input, expressions, .. } => {
                assert!($expr_check(expressions), "Project expressions check failed: {:?}", expressions);
                assert_plan!(**input, $($input)+);
            }
            other => panic!(
                "Expected Project, got {}\n\nActual plan:\n{}",
                $crate::test_utils::plan_node_name(other),
                $crate::test_utils::format_plan_tree(other, 0)
            ),
        }
    };

    ($plan:expr, Project { input: ($($input:tt)+), projections: [$($col:expr),+ $(,)?] }) => {
        match &$plan {
            PhysicalPlan::Project { input, expressions, .. } => {
                let expected_cols: Vec<&str> = vec![$($col),+];
                assert!(
                    $crate::test_utils::expressions_match_columns(expressions, &expected_cols),
                    "Project projections mismatch: expected {:?}, got {:?}",
                    expected_cols, expressions
                );
                assert_plan!(**input, $($input)+);
            }
            other => panic!(
                "Expected Project, got {}\n\nActual plan:\n{}",
                $crate::test_utils::plan_node_name(other),
                $crate::test_utils::format_plan_tree(other, 0)
            ),
        }
    };

    ($plan:expr, HashJoin { left: ($($left:tt)+), right: ($($right:tt)+), join_type: $jt:expr }) => {
        match &$plan {
            PhysicalPlan::HashJoin {
                left: _left,
                right: _right,
                join_type,
                ..
            } => {
                assert_eq!(*join_type, $jt, "HashJoin join_type mismatch");
                assert_plan!(**_left, $($left)+);
                assert_plan!(**_right, $($right)+);
            }
            other => panic!(
                "Expected HashJoin, got {}\n\nActual plan:\n{}",
                $crate::test_utils::plan_node_name(other),
                $crate::test_utils::format_plan_tree(other, 0)
            ),
        }
    };

    ($plan:expr, HashJoin { left: ($($left:tt)+), right: ($($right:tt)+) }) => {
        match &$plan {
            PhysicalPlan::HashJoin { left: _left, right: _right, .. } => {
                assert_plan!(**_left, $($left)+);
                assert_plan!(**_right, $($right)+);
            }
            other => panic!(
                "Expected HashJoin, got {}\n\nActual plan:\n{}",
                $crate::test_utils::plan_node_name(other),
                $crate::test_utils::format_plan_tree(other, 0)
            ),
        }
    };

    ($plan:expr, HashJoin {
        left: ($($left:tt)+),
        right: ($($right:tt)+),
        join_type: $jt:expr,
        join_on: [$(($lk:expr, $rk:expr)),+ $(,)?]
    }) => {
        match &$plan {
            PhysicalPlan::HashJoin {
                left,
                right,
                join_type,
                left_keys,
                right_keys,
                ..
            } => {
                assert_eq!(*join_type, $jt, "HashJoin join_type mismatch");
                $(
                    assert!(
                        $crate::test_utils::join_keys_match(left_keys, right_keys, $lk, $rk),
                        "HashJoin join_on missing ('{}', '{}'): left_keys={:?}, right_keys={:?}",
                        $lk, $rk, left_keys, right_keys
                    );
                )+
                assert_plan!(**left, $($left)+);
                assert_plan!(**right, $($right)+);
            }
            other => panic!(
                "Expected HashJoin, got {}\n\nActual plan:\n{}",
                $crate::test_utils::plan_node_name(other),
                $crate::test_utils::format_plan_tree(other, 0)
            ),
        }
    };

    ($plan:expr, NestedLoopJoin { left: ($($left:tt)+), right: ($($right:tt)+), join_type: $jt:expr }) => {
        match &$plan {
            PhysicalPlan::NestedLoopJoin {
                left,
                right,
                join_type,
                ..
            } => {
                assert_eq!(*join_type, $jt, "NestedLoopJoin join_type mismatch");
                assert_plan!(**left, $($left)+);
                assert_plan!(**right, $($right)+);
            }
            other => panic!(
                "Expected NestedLoopJoin, got {}\n\nActual plan:\n{}",
                $crate::test_utils::plan_node_name(other),
                $crate::test_utils::format_plan_tree(other, 0)
            ),
        }
    };

    ($plan:expr, NestedLoopJoin {
        left: ($($left:tt)+),
        right: ($($right:tt)+),
        join_type: $jt:expr,
        condition: ($lc:expr, $op:expr, $rc:expr)
    }) => {
        match &$plan {
            PhysicalPlan::NestedLoopJoin {
                left,
                right,
                join_type,
                condition,
                ..
            } => {
                assert_eq!(*join_type, $jt, "NestedLoopJoin join_type mismatch");
                assert!(
                    condition.as_ref().is_some_and(|e| $crate::test_utils::is_binary_op_columns(e, $lc, $op, $rc)),
                    "NestedLoopJoin condition mismatch: expected ({} {} {}), got {:?}",
                    $lc, $op, $rc, condition
                );
                assert_plan!(**left, $($left)+);
                assert_plan!(**right, $($right)+);
            }
            other => panic!(
                "Expected NestedLoopJoin, got {}\n\nActual plan:\n{}",
                $crate::test_utils::plan_node_name(other),
                $crate::test_utils::format_plan_tree(other, 0)
            ),
        }
    };

    ($plan:expr, NestedLoopJoin {
        left: ($($left:tt)+),
        right: ($($right:tt)+),
        join_type: $jt:expr,
        condition: $cond_check:expr
    }) => {
        match &$plan {
            PhysicalPlan::NestedLoopJoin {
                left,
                right,
                join_type,
                condition,
                ..
            } => {
                assert_eq!(*join_type, $jt, "NestedLoopJoin join_type mismatch");
                assert!($cond_check(condition), "NestedLoopJoin condition check failed: {:?}", condition);
                assert_plan!(**left, $($left)+);
                assert_plan!(**right, $($right)+);
            }
            other => panic!(
                "Expected NestedLoopJoin, got {}\n\nActual plan:\n{}",
                $crate::test_utils::plan_node_name(other),
                $crate::test_utils::format_plan_tree(other, 0)
            ),
        }
    };

    ($plan:expr, CrossJoin { left: ($($left:tt)+), right: ($($right:tt)+) }) => {
        match &$plan {
            PhysicalPlan::CrossJoin { left, right, .. } => {
                assert_plan!(**left, $($left)+);
                assert_plan!(**right, $($right)+);
            }
            other => panic!(
                "Expected CrossJoin, got {}\n\nActual plan:\n{}",
                $crate::test_utils::plan_node_name(other),
                $crate::test_utils::format_plan_tree(other, 0)
            ),
        }
    };

    ($plan:expr, Sort { input: ($($input:tt)+) }) => {
        match &$plan {
            PhysicalPlan::Sort { input, .. } => {
                assert_plan!(**input, $($input)+);
            }
            other => panic!(
                "Expected Sort, got {}\n\nActual plan:\n{}",
                $crate::test_utils::plan_node_name(other),
                $crate::test_utils::format_plan_tree(other, 0)
            ),
        }
    };

    ($plan:expr, Sort { input: ($($input:tt)+), sort_exprs: $sort_check:expr }) => {
        match &$plan {
            PhysicalPlan::Sort { input, sort_exprs, .. } => {
                assert!($sort_check(sort_exprs), "Sort sort_exprs check failed: {:?}", sort_exprs);
                assert_plan!(**input, $($input)+);
            }
            other => panic!(
                "Expected Sort, got {}\n\nActual plan:\n{}",
                $crate::test_utils::plan_node_name(other),
                $crate::test_utils::format_plan_tree(other, 0)
            ),
        }
    };

    ($plan:expr, Limit { input: ($($input:tt)+) }) => {
        match &$plan {
            PhysicalPlan::Limit { input, .. } => {
                assert_plan!(**input, $($input)+);
            }
            other => panic!(
                "Expected Limit, got {}\n\nActual plan:\n{}",
                $crate::test_utils::plan_node_name(other),
                $crate::test_utils::format_plan_tree(other, 0)
            ),
        }
    };

    ($plan:expr, Limit { input: ($($input:tt)+), limit: $lim:expr }) => {
        match &$plan {
            PhysicalPlan::Limit { input, limit, .. } => {
                assert_eq!(*limit, $lim, "Limit value mismatch");
                assert_plan!(**input, $($input)+);
            }
            other => panic!(
                "Expected Limit, got {}\n\nActual plan:\n{}",
                $crate::test_utils::plan_node_name(other),
                $crate::test_utils::format_plan_tree(other, 0)
            ),
        }
    };

    ($plan:expr, Limit { input: ($($input:tt)+), limit: $lim:expr, offset: $off:expr }) => {
        match &$plan {
            PhysicalPlan::Limit { input, limit, offset } => {
                assert_eq!(*limit, $lim, "Limit value mismatch");
                assert_eq!(*offset, $off, "Limit offset mismatch");
                assert_plan!(**input, $($input)+);
            }
            other => panic!(
                "Expected Limit, got {}\n\nActual plan:\n{}",
                $crate::test_utils::plan_node_name(other),
                $crate::test_utils::format_plan_tree(other, 0)
            ),
        }
    };

    ($plan:expr, TopN { input: ($($input:tt)+), limit: $lim:expr }) => {
        match &$plan {
            PhysicalPlan::TopN { input, limit, .. } => {
                assert_eq!(*limit, $lim, "TopN limit mismatch");
                assert_plan!(**input, $($input)+);
            }
            other => panic!(
                "Expected TopN, got {}\n\nActual plan:\n{}",
                $crate::test_utils::plan_node_name(other),
                $crate::test_utils::format_plan_tree(other, 0)
            ),
        }
    };

    ($plan:expr, TopN { input: ($($input:tt)+) }) => {
        match &$plan {
            PhysicalPlan::TopN { input, .. } => {
                assert_plan!(**input, $($input)+);
            }
            other => panic!(
                "Expected TopN, got {}\n\nActual plan:\n{}",
                $crate::test_utils::plan_node_name(other),
                $crate::test_utils::format_plan_tree(other, 0)
            ),
        }
    };

    ($plan:expr, TopN { input: ($($input:tt)+), limit: $lim:expr, sort_exprs: $sort_check:expr }) => {
        match &$plan {
            PhysicalPlan::TopN { input, limit, sort_exprs, .. } => {
                assert_eq!(*limit, $lim, "TopN limit mismatch");
                assert!($sort_check(sort_exprs), "TopN sort_exprs check failed: {:?}", sort_exprs);
                assert_plan!(**input, $($input)+);
            }
            other => panic!(
                "Expected TopN, got {}\n\nActual plan:\n{}",
                $crate::test_utils::plan_node_name(other),
                $crate::test_utils::format_plan_tree(other, 0)
            ),
        }
    };

    ($plan:expr, HashAggregate { input: ($($input:tt)+) }) => {
        match &$plan {
            PhysicalPlan::HashAggregate { input: _input, .. } => {
                assert_plan!(**_input, $($input)+);
            }
            other => panic!(
                "Expected HashAggregate, got {}\n\nActual plan:\n{}",
                $crate::test_utils::plan_node_name(other),
                $crate::test_utils::format_plan_tree(other, 0)
            ),
        }
    };

    ($plan:expr, HashAggregate { input: ($($input:tt)+), group_by: [$($col:expr),+ $(,)?] }) => {
        match &$plan {
            PhysicalPlan::HashAggregate { input, group_by, .. } => {
                let expected_cols: Vec<&str> = vec![$($col),+];
                assert!(
                    $crate::test_utils::expressions_match_columns(group_by, &expected_cols),
                    "HashAggregate group_by mismatch: expected {:?}, got {:?}",
                    expected_cols, group_by
                );
                assert_plan!(**input, $($input)+);
            }
            other => panic!(
                "Expected HashAggregate, got {}\n\nActual plan:\n{}",
                $crate::test_utils::plan_node_name(other),
                $crate::test_utils::format_plan_tree(other, 0)
            ),
        }
    };

    ($plan:expr, HashAggregate { input: ($($input:tt)+), group_by: [$($col:expr),+ $(,)?], aggregates: [$($agg:expr),+ $(,)?] }) => {
        match &$plan {
            PhysicalPlan::HashAggregate { input, group_by, aggregates, .. } => {
                let expected_cols: Vec<&str> = vec![$($col),+];
                assert!(
                    $crate::test_utils::expressions_match_columns(group_by, &expected_cols),
                    "HashAggregate group_by mismatch: expected {:?}, got {:?}",
                    expected_cols, group_by
                );
                let expected_aggs: Vec<&str> = vec![$($agg),+];
                assert!(
                    $crate::test_utils::aggregates_match(aggregates, &expected_aggs),
                    "HashAggregate aggregates mismatch: expected {:?}, got {:?}",
                    expected_aggs, aggregates
                );
                assert_plan!(**input, $($input)+);
            }
            other => panic!(
                "Expected HashAggregate, got {}\n\nActual plan:\n{}",
                $crate::test_utils::plan_node_name(other),
                $crate::test_utils::format_plan_tree(other, 0)
            ),
        }
    };

    ($plan:expr, HashAggregate { input: ($($input:tt)+), group_exprs: $grp_check:expr }) => {
        match &$plan {
            PhysicalPlan::HashAggregate { input, group_by, .. } => {
                assert!($grp_check(group_by), "HashAggregate group_exprs check failed: {:?}", group_by);
                assert_plan!(**input, $($input)+);
            }
            other => panic!(
                "Expected HashAggregate, got {}\n\nActual plan:\n{}",
                $crate::test_utils::plan_node_name(other),
                $crate::test_utils::format_plan_tree(other, 0)
            ),
        }
    };

    ($plan:expr, HashAggregate { input: ($($input:tt)+), group_exprs: $grp_check:expr, aggregates: $agg_check:expr }) => {
        match &$plan {
            PhysicalPlan::HashAggregate { input, group_by, aggregates, .. } => {
                assert!($grp_check(group_by), "HashAggregate group_exprs check failed: {:?}", group_by);
                assert!($agg_check(aggregates), "HashAggregate aggregates check failed: {:?}", aggregates);
                assert_plan!(**input, $($input)+);
            }
            other => panic!(
                "Expected HashAggregate, got {}\n\nActual plan:\n{}",
                $crate::test_utils::plan_node_name(other),
                $crate::test_utils::format_plan_tree(other, 0)
            ),
        }
    };

    ($plan:expr, Distinct { input: ($($input:tt)+) }) => {
        match &$plan {
            PhysicalPlan::Distinct { input } => {
                assert_plan!(**input, $($input)+);
            }
            other => panic!(
                "Expected Distinct, got {}\n\nActual plan:\n{}",
                $crate::test_utils::plan_node_name(other),
                $crate::test_utils::format_plan_tree(other, 0)
            ),
        }
    };

    ($plan:expr, Union { inputs: [$($inp:tt),+ $(,)?] }) => {
        match &$plan {
            PhysicalPlan::Union { inputs, .. } => {
                let expected: Vec<&str> = vec![$($inp),+];
                assert_eq!(inputs.len(), expected.len(), "Union inputs count mismatch");
            }
            other => panic!(
                "Expected Union, got {}\n\nActual plan:\n{}",
                $crate::test_utils::plan_node_name(other),
                $crate::test_utils::format_plan_tree(other, 0)
            ),
        }
    };

    ($plan:expr, Window { input: ($($input:tt)+) }) => {
        match &$plan {
            PhysicalPlan::Window { input, .. } => {
                assert_plan!(**input, $($input)+);
            }
            other => panic!(
                "Expected Window, got {}\n\nActual plan:\n{}",
                $crate::test_utils::plan_node_name(other),
                $crate::test_utils::format_plan_tree(other, 0)
            ),
        }
    };

    ($plan:expr, Empty) => {
        match &$plan {
            PhysicalPlan::Empty { .. } => {}
            other => panic!(
                "Expected Empty, got {}\n\nActual plan:\n{}",
                $crate::test_utils::plan_node_name(other),
                $crate::test_utils::format_plan_tree(other, 0)
            ),
        }
    };
}

pub(crate) use assert_plan;

pub(crate) fn is_eq_column_literal(
    expr: &yachtsql_ir::Expr,
    col_name: &str,
    lit_val: &str,
) -> bool {
    use yachtsql_ir::{BinaryOp, Expr, Literal};
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOp::Eq,
            right,
        } => {
            let col_match = matches!(left.as_ref(), Expr::Column { name, .. } if name == col_name)
                || matches!(right.as_ref(), Expr::Column { name, .. } if name == col_name);
            let lit_match = matches!(left.as_ref(), Expr::Literal(Literal::String(s)) if s == lit_val)
                || matches!(right.as_ref(), Expr::Literal(Literal::String(s)) if s == lit_val);
            col_match && lit_match
        }
        _ => false,
    }
}

pub(crate) fn is_gt_column_literal(expr: &yachtsql_ir::Expr, col_name: &str, lit_val: i64) -> bool {
    use yachtsql_ir::{BinaryOp, Expr, Literal};
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOp::Gt,
            right,
        } => {
            let col_match = matches!(left.as_ref(), Expr::Column { name, .. } if name == col_name);
            let lit_match =
                matches!(right.as_ref(), Expr::Literal(Literal::Int64(v)) if *v == lit_val);
            col_match && lit_match
        }
        _ => false,
    }
}

pub(crate) fn is_column(expr: &yachtsql_ir::Expr, col_name: &str) -> bool {
    matches!(expr, yachtsql_ir::Expr::Column { name, .. } if name == col_name)
}

pub(crate) fn join_keys_match(
    left_keys: &[yachtsql_ir::Expr],
    right_keys: &[yachtsql_ir::Expr],
    left_col: &str,
    right_col: &str,
) -> bool {
    left_keys
        .iter()
        .zip(right_keys.iter())
        .any(|(lk, rk)| is_column(lk, left_col) && is_column(rk, right_col))
}

pub(crate) fn is_binary_op_columns(
    expr: &yachtsql_ir::Expr,
    left_col: &str,
    op_str: &str,
    right_col: &str,
) -> bool {
    use yachtsql_ir::{BinaryOp, Expr};
    let expected_op = match op_str {
        "=" => BinaryOp::Eq,
        "!=" | "<>" => BinaryOp::NotEq,
        ">" => BinaryOp::Gt,
        "<" => BinaryOp::Lt,
        ">=" => BinaryOp::GtEq,
        "<=" => BinaryOp::LtEq,
        _ => return false,
    };
    match expr {
        Expr::BinaryOp { left, op, right } if *op == expected_op => {
            is_column(left, left_col) && is_column(right, right_col)
        }
        _ => false,
    }
}

pub(crate) fn get_expression_name(expr: &yachtsql_ir::Expr) -> Option<String> {
    use yachtsql_ir::Expr;
    match expr {
        Expr::Column { name, .. } => Some(name.clone()),
        Expr::Alias { name, .. } => Some(name.clone()),
        Expr::Aggregate { func, .. } => Some(format!("{:?}", func).to_uppercase()),
        _ => None,
    }
}

pub(crate) fn expressions_match_columns(exprs: &[yachtsql_ir::Expr], expected: &[&str]) -> bool {
    if exprs.len() != expected.len() {
        return false;
    }
    exprs
        .iter()
        .zip(expected.iter())
        .all(|(expr, expected_name)| {
            get_expression_name(expr)
                .map(|name| name.eq_ignore_ascii_case(expected_name))
                .unwrap_or(false)
        })
}

pub(crate) fn aggregates_match(exprs: &[yachtsql_ir::Expr], expected: &[&str]) -> bool {
    use yachtsql_ir::Expr;
    if exprs.len() != expected.len() {
        return false;
    }
    exprs
        .iter()
        .zip(expected.iter())
        .all(|(expr, expected_name)| match expr {
            Expr::Aggregate { func, .. } => {
                format!("{:?}", func).eq_ignore_ascii_case(expected_name)
            }
            Expr::Alias { expr: inner, name } => {
                name.eq_ignore_ascii_case(expected_name)
                    || matches!(inner.as_ref(), Expr::Aggregate { func, .. }
                        if format!("{:?}", func).eq_ignore_ascii_case(expected_name))
            }
            _ => false,
        })
}

pub(crate) fn plan_node_name(plan: &crate::PhysicalPlan) -> &'static str {
    use crate::PhysicalPlan;
    match plan {
        PhysicalPlan::TableScan { .. } => "TableScan",
        PhysicalPlan::Sample { .. } => "Sample",
        PhysicalPlan::Filter { .. } => "Filter",
        PhysicalPlan::Project { .. } => "Project",
        PhysicalPlan::NestedLoopJoin { .. } => "NestedLoopJoin",
        PhysicalPlan::CrossJoin { .. } => "CrossJoin",
        PhysicalPlan::HashJoin { .. } => "HashJoin",
        PhysicalPlan::HashAggregate { .. } => "HashAggregate",
        PhysicalPlan::Sort { .. } => "Sort",
        PhysicalPlan::Limit { .. } => "Limit",
        PhysicalPlan::TopN { .. } => "TopN",
        PhysicalPlan::Distinct { .. } => "Distinct",
        PhysicalPlan::Union { .. } => "Union",
        PhysicalPlan::Intersect { .. } => "Intersect",
        PhysicalPlan::Except { .. } => "Except",
        PhysicalPlan::Window { .. } => "Window",
        PhysicalPlan::Unnest { .. } => "Unnest",
        PhysicalPlan::Qualify { .. } => "Qualify",
        PhysicalPlan::WithCte { .. } => "WithCte",
        PhysicalPlan::Values { .. } => "Values",
        PhysicalPlan::Empty { .. } => "Empty",
        PhysicalPlan::Insert { .. } => "Insert",
        PhysicalPlan::Update { .. } => "Update",
        PhysicalPlan::Delete { .. } => "Delete",
        PhysicalPlan::Merge { .. } => "Merge",
        PhysicalPlan::CreateTable { .. } => "CreateTable",
        PhysicalPlan::DropTable { .. } => "DropTable",
        PhysicalPlan::AlterTable { .. } => "AlterTable",
        PhysicalPlan::Truncate { .. } => "Truncate",
        PhysicalPlan::CreateView { .. } => "CreateView",
        PhysicalPlan::DropView { .. } => "DropView",
        PhysicalPlan::CreateSchema { .. } => "CreateSchema",
        PhysicalPlan::DropSchema { .. } => "DropSchema",
        PhysicalPlan::UndropSchema { .. } => "UndropSchema",
        PhysicalPlan::AlterSchema { .. } => "AlterSchema",
        PhysicalPlan::CreateFunction { .. } => "CreateFunction",
        PhysicalPlan::DropFunction { .. } => "DropFunction",
        PhysicalPlan::CreateProcedure { .. } => "CreateProcedure",
        PhysicalPlan::DropProcedure { .. } => "DropProcedure",
        PhysicalPlan::Call { .. } => "Call",
        PhysicalPlan::ExportData { .. } => "ExportData",
        PhysicalPlan::LoadData { .. } => "LoadData",
        PhysicalPlan::Declare { .. } => "Declare",
        PhysicalPlan::SetVariable { .. } => "SetVariable",
        PhysicalPlan::SetMultipleVariables { .. } => "SetMultipleVariables",
        PhysicalPlan::If { .. } => "If",
        PhysicalPlan::While { .. } => "While",
        PhysicalPlan::Loop { .. } => "Loop",
        PhysicalPlan::Block { .. } => "Block",
        PhysicalPlan::Repeat { .. } => "Repeat",
        PhysicalPlan::For { .. } => "For",
        PhysicalPlan::Return { .. } => "Return",
        PhysicalPlan::Raise { .. } => "Raise",
        PhysicalPlan::ExecuteImmediate { .. } => "ExecuteImmediate",
        PhysicalPlan::Break { .. } => "Break",
        PhysicalPlan::Continue { .. } => "Continue",
        PhysicalPlan::CreateSnapshot { .. } => "CreateSnapshot",
        PhysicalPlan::DropSnapshot { .. } => "DropSnapshot",
        PhysicalPlan::Assert { .. } => "Assert",
        PhysicalPlan::Grant { .. } => "Grant",
        PhysicalPlan::Revoke { .. } => "Revoke",
        PhysicalPlan::BeginTransaction => "BeginTransaction",
        PhysicalPlan::Commit => "Commit",
        PhysicalPlan::Rollback => "Rollback",
        PhysicalPlan::TryCatch { .. } => "TryCatch",
        PhysicalPlan::GapFill { .. } => "GapFill",
        PhysicalPlan::Explain { .. } => "Explain",
    }
}

pub(crate) fn format_plan_tree(plan: &crate::PhysicalPlan, indent: usize) -> String {
    use crate::PhysicalPlan;
    let prefix = "  ".repeat(indent);
    let mut result = String::new();

    match plan {
        PhysicalPlan::TableScan { table_name, .. } => {
            result.push_str(&format!(
                "{}TableScan {{ table: \"{}\" }}",
                prefix, table_name
            ));
        }
        PhysicalPlan::Filter { input, predicate } => {
            result.push_str(&format!(
                "{}Filter {{ predicate: {} }}\n",
                prefix,
                format_expr_short(predicate)
            ));
            result.push_str(&format_plan_tree(input, indent + 1));
        }
        PhysicalPlan::Project {
            input, expressions, ..
        } => {
            let expr_names: Vec<String> = expressions.iter().map(format_expr_short).collect();
            result.push_str(&format!(
                "{}Project {{ cols: [{}] }}\n",
                prefix,
                expr_names.join(", ")
            ));
            result.push_str(&format_plan_tree(input, indent + 1));
        }
        PhysicalPlan::HashJoin {
            left,
            right,
            join_type,
            left_keys,
            right_keys,
            ..
        } => {
            let lk: Vec<String> = left_keys.iter().map(format_expr_short).collect();
            let rk: Vec<String> = right_keys.iter().map(format_expr_short).collect();
            result.push_str(&format!(
                "{}HashJoin {{ type: {:?}, on: [{}] = [{}] }}\n",
                prefix,
                join_type,
                lk.join(", "),
                rk.join(", ")
            ));
            result.push_str(&format!("{}├─ left:\n", prefix));
            result.push_str(&format_plan_tree(left, indent + 2));
            result.push('\n');
            result.push_str(&format!("{}└─ right:\n", prefix));
            result.push_str(&format_plan_tree(right, indent + 2));
        }
        PhysicalPlan::NestedLoopJoin {
            left,
            right,
            join_type,
            condition,
            ..
        } => {
            let cond_str = condition
                .as_ref()
                .map(format_expr_short)
                .unwrap_or_else(|| "none".to_string());
            result.push_str(&format!(
                "{}NestedLoopJoin {{ type: {:?}, cond: {} }}\n",
                prefix, join_type, cond_str
            ));
            result.push_str(&format!("{}├─ left:\n", prefix));
            result.push_str(&format_plan_tree(left, indent + 2));
            result.push('\n');
            result.push_str(&format!("{}└─ right:\n", prefix));
            result.push_str(&format_plan_tree(right, indent + 2));
        }
        PhysicalPlan::CrossJoin { left, right, .. } => {
            result.push_str(&format!("{}CrossJoin\n", prefix));
            result.push_str(&format!("{}├─ left:\n", prefix));
            result.push_str(&format_plan_tree(left, indent + 2));
            result.push('\n');
            result.push_str(&format!("{}└─ right:\n", prefix));
            result.push_str(&format_plan_tree(right, indent + 2));
        }
        PhysicalPlan::HashAggregate {
            input,
            group_by,
            aggregates,
            ..
        } => {
            let grp: Vec<String> = group_by.iter().map(format_expr_short).collect();
            let aggs: Vec<String> = aggregates.iter().map(format_expr_short).collect();
            result.push_str(&format!(
                "{}HashAggregate {{ group_by: [{}], aggs: [{}] }}\n",
                prefix,
                grp.join(", "),
                aggs.join(", ")
            ));
            result.push_str(&format_plan_tree(input, indent + 1));
        }
        PhysicalPlan::Sort {
            input, sort_exprs, ..
        } => {
            let sorts: Vec<String> = sort_exprs
                .iter()
                .map(|s| format_expr_short(&s.expr))
                .collect();
            result.push_str(&format!(
                "{}Sort {{ by: [{}] }}\n",
                prefix,
                sorts.join(", ")
            ));
            result.push_str(&format_plan_tree(input, indent + 1));
        }
        PhysicalPlan::TopN { input, limit, .. } => {
            result.push_str(&format!("{}TopN {{ limit: {} }}\n", prefix, limit));
            result.push_str(&format_plan_tree(input, indent + 1));
        }
        PhysicalPlan::Limit {
            input,
            limit,
            offset,
        } => {
            result.push_str(&format!(
                "{}Limit {{ limit: {:?}, offset: {:?} }}\n",
                prefix, limit, offset
            ));
            result.push_str(&format_plan_tree(input, indent + 1));
        }
        PhysicalPlan::Distinct { input } => {
            result.push_str(&format!("{}Distinct\n", prefix));
            result.push_str(&format_plan_tree(input, indent + 1));
        }
        PhysicalPlan::Union { inputs, all, .. } => {
            result.push_str(&format!("{}Union {{ all: {} }}\n", prefix, all));
            for (i, inp) in inputs.iter().enumerate() {
                result.push_str(&format!("{}[{}]:\n", prefix, i));
                result.push_str(&format_plan_tree(inp, indent + 1));
                if i < inputs.len() - 1 {
                    result.push('\n');
                }
            }
        }
        PhysicalPlan::Window { input, .. } => {
            result.push_str(&format!("{}Window\n", prefix));
            result.push_str(&format_plan_tree(input, indent + 1));
        }
        PhysicalPlan::WithCte { body, ctes, .. } => {
            result.push_str(&format!("{}WithCte {{ {} CTEs }}\n", prefix, ctes.len()));
            result.push_str(&format_plan_tree(body, indent + 1));
        }
        other => {
            result.push_str(&format!("{}{}", prefix, plan_node_name(other)));
        }
    }

    result
}

fn format_binary_op(op: &yachtsql_ir::BinaryOp) -> &'static str {
    use yachtsql_ir::BinaryOp;
    match op {
        BinaryOp::Eq => "=",
        BinaryOp::NotEq => "!=",
        BinaryOp::Lt => "<",
        BinaryOp::LtEq => "<=",
        BinaryOp::Gt => ">",
        BinaryOp::GtEq => ">=",
        BinaryOp::Add => "+",
        BinaryOp::Sub => "-",
        BinaryOp::Mul => "*",
        BinaryOp::Div => "/",
        BinaryOp::Mod => "%",
        BinaryOp::And => "AND",
        BinaryOp::Or => "OR",
        BinaryOp::BitwiseAnd => "&",
        BinaryOp::BitwiseOr => "|",
        BinaryOp::BitwiseXor => "^",
        BinaryOp::ShiftLeft => "<<",
        BinaryOp::ShiftRight => ">>",
        BinaryOp::Concat => "||",
    }
}

fn format_expr_short(expr: &yachtsql_ir::Expr) -> String {
    use yachtsql_ir::Expr;
    match expr {
        Expr::Column { table, name, .. } => {
            if let Some(t) = table {
                format!("{}.{}", t, name)
            } else {
                name.clone()
            }
        }
        Expr::Literal(lit) => format!("{:?}", lit),
        Expr::Alias { name, expr: inner } => {
            format!("{} AS {}", format_expr_short(inner), name)
        }
        Expr::Aggregate { func, args, .. } => {
            let arg_strs: Vec<String> = args.iter().map(format_expr_short).collect();
            format!("{:?}({})", func, arg_strs.join(", "))
        }
        Expr::BinaryOp { left, op, right } => {
            format!(
                "{} {} {}",
                format_expr_short(left),
                format_binary_op(op),
                format_expr_short(right)
            )
        }
        Expr::ScalarSubquery(_) => "<ScalarSubquery>".to_string(),
        Expr::Exists { negated, .. } => if *negated {
            "NOT EXISTS(...)"
        } else {
            "EXISTS(...)"
        }
        .to_string(),
        Expr::InSubquery { negated, .. } => {
            if *negated { "NOT IN(...)" } else { "IN(...)" }.to_string()
        }
        _ => format!(
            "<{}>",
            std::any::type_name::<Expr>()
                .split("::")
                .last()
                .unwrap_or("Expr")
        ),
    }
}
