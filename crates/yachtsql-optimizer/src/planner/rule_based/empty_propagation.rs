#![coverage(off)]

use yachtsql_ir::JoinType;

use crate::PhysicalPlan;

fn is_empty(plan: &PhysicalPlan) -> bool {
    match plan {
        PhysicalPlan::Empty { schema } => !schema.fields.is_empty(),
        _ => false,
    }
}

pub fn apply_empty_propagation(plan: PhysicalPlan) -> PhysicalPlan {
    match plan {
        PhysicalPlan::Filter { input, predicate } => {
            let optimized_input = apply_empty_propagation(*input);

            if is_empty(&optimized_input) {
                return optimized_input;
            }

            PhysicalPlan::Filter {
                input: Box::new(optimized_input),
                predicate,
            }
        }

        PhysicalPlan::Project {
            input,
            expressions,
            schema,
        } => {
            let optimized_input = apply_empty_propagation(*input);

            if is_empty(&optimized_input) {
                return PhysicalPlan::Empty { schema };
            }

            PhysicalPlan::Project {
                input: Box::new(optimized_input),
                expressions,
                schema,
            }
        }

        PhysicalPlan::HashAggregate {
            input,
            group_by,
            aggregates,
            schema,
            grouping_sets,
            hints,
        } => {
            let optimized_input = apply_empty_propagation(*input);

            PhysicalPlan::HashAggregate {
                input: Box::new(optimized_input),
                group_by,
                aggregates,
                schema,
                grouping_sets,
                hints,
            }
        }

        PhysicalPlan::HashJoin {
            left,
            right,
            join_type,
            left_keys,
            right_keys,
            schema,
            parallel,
            hints,
        } => {
            let optimized_left = apply_empty_propagation(*left);
            let optimized_right = apply_empty_propagation(*right);

            match join_type {
                JoinType::Inner | JoinType::Cross => {
                    if is_empty(&optimized_left) || is_empty(&optimized_right) {
                        return PhysicalPlan::Empty { schema };
                    }
                }
                JoinType::Left => {
                    if is_empty(&optimized_left) {
                        return PhysicalPlan::Empty { schema };
                    }
                }
                JoinType::Right => {
                    if is_empty(&optimized_right) {
                        return PhysicalPlan::Empty { schema };
                    }
                }
                JoinType::Full => {}
            }

            PhysicalPlan::HashJoin {
                left: Box::new(optimized_left),
                right: Box::new(optimized_right),
                join_type,
                left_keys,
                right_keys,
                schema,
                parallel,
                hints,
            }
        }

        PhysicalPlan::NestedLoopJoin {
            left,
            right,
            join_type,
            condition,
            schema,
            parallel,
            hints,
        } => {
            let optimized_left = apply_empty_propagation(*left);
            let optimized_right = apply_empty_propagation(*right);

            match join_type {
                JoinType::Inner | JoinType::Cross => {
                    if is_empty(&optimized_left) || is_empty(&optimized_right) {
                        return PhysicalPlan::Empty { schema };
                    }
                }
                JoinType::Left => {
                    if is_empty(&optimized_left) {
                        return PhysicalPlan::Empty { schema };
                    }
                }
                JoinType::Right => {
                    if is_empty(&optimized_right) {
                        return PhysicalPlan::Empty { schema };
                    }
                }
                JoinType::Full => {}
            }

            PhysicalPlan::NestedLoopJoin {
                left: Box::new(optimized_left),
                right: Box::new(optimized_right),
                join_type,
                condition,
                schema,
                parallel,
                hints,
            }
        }

        PhysicalPlan::CrossJoin {
            left,
            right,
            schema,
            parallel,
            hints,
        } => {
            let optimized_left = apply_empty_propagation(*left);
            let optimized_right = apply_empty_propagation(*right);

            if is_empty(&optimized_left) || is_empty(&optimized_right) {
                return PhysicalPlan::Empty { schema };
            }

            PhysicalPlan::CrossJoin {
                left: Box::new(optimized_left),
                right: Box::new(optimized_right),
                schema,
                parallel,
                hints,
            }
        }

        PhysicalPlan::Sort {
            input,
            sort_exprs,
            hints,
        } => {
            let optimized_input = apply_empty_propagation(*input);

            if is_empty(&optimized_input) {
                return optimized_input;
            }

            PhysicalPlan::Sort {
                input: Box::new(optimized_input),
                sort_exprs,
                hints,
            }
        }

        PhysicalPlan::TopN {
            input,
            sort_exprs,
            limit,
        } => {
            let optimized_input = apply_empty_propagation(*input);

            if is_empty(&optimized_input) {
                return optimized_input;
            }

            PhysicalPlan::TopN {
                input: Box::new(optimized_input),
                sort_exprs,
                limit,
            }
        }

        PhysicalPlan::Limit {
            input,
            limit,
            offset,
        } => {
            let optimized_input = apply_empty_propagation(*input);

            if is_empty(&optimized_input) {
                return optimized_input;
            }

            if limit == Some(0) {
                return PhysicalPlan::Empty {
                    schema: optimized_input.schema().clone(),
                };
            }

            PhysicalPlan::Limit {
                input: Box::new(optimized_input),
                limit,
                offset,
            }
        }

        PhysicalPlan::Distinct { input } => {
            let optimized_input = apply_empty_propagation(*input);

            if is_empty(&optimized_input) {
                return optimized_input;
            }

            PhysicalPlan::Distinct {
                input: Box::new(optimized_input),
            }
        }

        PhysicalPlan::Union {
            inputs,
            all,
            schema,
            parallel,
            hints,
        } => {
            let optimized_inputs: Vec<_> = inputs
                .into_iter()
                .map(apply_empty_propagation)
                .filter(|p| !is_empty(p))
                .collect();

            if optimized_inputs.is_empty() {
                return PhysicalPlan::Empty { schema };
            }

            if optimized_inputs.len() == 1 {
                return optimized_inputs.into_iter().next().unwrap();
            }

            PhysicalPlan::Union {
                inputs: optimized_inputs,
                all,
                schema,
                parallel,
                hints,
            }
        }

        PhysicalPlan::Intersect {
            left,
            right,
            all,
            schema,
            parallel,
            hints,
        } => {
            let optimized_left = apply_empty_propagation(*left);
            let optimized_right = apply_empty_propagation(*right);

            if is_empty(&optimized_left) || is_empty(&optimized_right) {
                return PhysicalPlan::Empty { schema };
            }

            PhysicalPlan::Intersect {
                left: Box::new(optimized_left),
                right: Box::new(optimized_right),
                all,
                schema,
                parallel,
                hints,
            }
        }

        PhysicalPlan::Except {
            left,
            right,
            all,
            schema,
            parallel,
            hints,
        } => {
            let optimized_left = apply_empty_propagation(*left);
            let optimized_right = apply_empty_propagation(*right);

            if is_empty(&optimized_left) {
                return PhysicalPlan::Empty { schema };
            }

            if is_empty(&optimized_right) {
                return optimized_left;
            }

            PhysicalPlan::Except {
                left: Box::new(optimized_left),
                right: Box::new(optimized_right),
                all,
                schema,
                parallel,
                hints,
            }
        }

        PhysicalPlan::Window {
            input,
            window_exprs,
            schema,
            hints,
        } => {
            let optimized_input = apply_empty_propagation(*input);

            if is_empty(&optimized_input) {
                return PhysicalPlan::Empty { schema };
            }

            PhysicalPlan::Window {
                input: Box::new(optimized_input),
                window_exprs,
                schema,
                hints,
            }
        }

        PhysicalPlan::WithCte {
            ctes,
            body,
            parallel_ctes,
            hints,
        } => {
            let optimized_body = apply_empty_propagation(*body);

            PhysicalPlan::WithCte {
                ctes,
                body: Box::new(optimized_body),
                parallel_ctes,
                hints,
            }
        }

        PhysicalPlan::Unnest {
            input,
            columns,
            schema,
        } => {
            let optimized_input = apply_empty_propagation(*input);

            if is_empty(&optimized_input) {
                return PhysicalPlan::Empty { schema };
            }

            PhysicalPlan::Unnest {
                input: Box::new(optimized_input),
                columns,
                schema,
            }
        }

        PhysicalPlan::Qualify { input, predicate } => {
            let optimized_input = apply_empty_propagation(*input);

            if is_empty(&optimized_input) {
                return optimized_input;
            }

            PhysicalPlan::Qualify {
                input: Box::new(optimized_input),
                predicate,
            }
        }

        PhysicalPlan::Sample {
            input,
            sample_type,
            sample_value,
        } => {
            let optimized_input = apply_empty_propagation(*input);

            if is_empty(&optimized_input) {
                return optimized_input;
            }

            PhysicalPlan::Sample {
                input: Box::new(optimized_input),
                sample_type,
                sample_value,
            }
        }

        PhysicalPlan::GapFill {
            input,
            ts_column,
            bucket_width,
            value_columns,
            partitioning_columns,
            origin,
            input_schema,
            schema,
        } => {
            let optimized_input = apply_empty_propagation(*input);

            if is_empty(&optimized_input) {
                return PhysicalPlan::Empty { schema };
            }

            PhysicalPlan::GapFill {
                input: Box::new(optimized_input),
                ts_column,
                bucket_width,
                value_columns,
                partitioning_columns,
                origin,
                input_schema,
                schema,
            }
        }

        PhysicalPlan::Explain {
            input,
            analyze,
            logical_plan_text,
            physical_plan_text,
        } => PhysicalPlan::Explain {
            input: Box::new(apply_empty_propagation(*input)),
            analyze,
            logical_plan_text,
            physical_plan_text,
        },

        other => other,
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_common::types::DataType;
    use yachtsql_ir::{Expr, Literal, PlanField, PlanSchema};

    use super::*;
    use crate::ExecutionHints;

    fn make_schema(num_columns: usize) -> PlanSchema {
        let fields = (0..num_columns)
            .map(|i| PlanField::new(format!("col{}", i), DataType::Int64))
            .collect();
        PlanSchema::from_fields(fields)
    }

    fn make_scan(table_name: &str, num_columns: usize) -> PhysicalPlan {
        PhysicalPlan::TableScan {
            table_name: table_name.to_string(),
            schema: make_schema(num_columns),
            projection: None,
            row_count: None,
        }
    }

    fn make_empty(num_columns: usize) -> PhysicalPlan {
        PhysicalPlan::Empty {
            schema: make_schema(num_columns),
        }
    }

    #[test]
    fn propagates_empty_through_filter() {
        let empty = make_empty(3);
        let plan = PhysicalPlan::Filter {
            input: Box::new(empty),
            predicate: Expr::Literal(Literal::Bool(true)),
        };

        let result = apply_empty_propagation(plan);

        assert!(matches!(result, PhysicalPlan::Empty { .. }));
    }

    #[test]
    fn propagates_empty_through_project() {
        let empty = make_empty(3);
        let schema = make_schema(2);
        let plan = PhysicalPlan::Project {
            input: Box::new(empty),
            expressions: vec![],
            schema: schema.clone(),
        };

        let result = apply_empty_propagation(plan);

        match result {
            PhysicalPlan::Empty { schema: s } => {
                assert_eq!(s.fields.len(), 2);
            }
            _ => panic!("Expected Empty plan"),
        }
    }

    #[test]
    fn propagates_empty_through_inner_join_left() {
        let empty = make_empty(3);
        let scan = make_scan("t", 3);
        let schema = make_schema(6);

        let plan = PhysicalPlan::HashJoin {
            left: Box::new(empty),
            right: Box::new(scan),
            join_type: JoinType::Inner,
            left_keys: vec![],
            right_keys: vec![],
            schema,
            parallel: false,
            hints: ExecutionHints::default(),
        };

        let result = apply_empty_propagation(plan);

        assert!(matches!(result, PhysicalPlan::Empty { .. }));
    }

    #[test]
    fn propagates_empty_through_inner_join_right() {
        let scan = make_scan("t", 3);
        let empty = make_empty(3);
        let schema = make_schema(6);

        let plan = PhysicalPlan::HashJoin {
            left: Box::new(scan),
            right: Box::new(empty),
            join_type: JoinType::Inner,
            left_keys: vec![],
            right_keys: vec![],
            schema,
            parallel: false,
            hints: ExecutionHints::default(),
        };

        let result = apply_empty_propagation(plan);

        assert!(matches!(result, PhysicalPlan::Empty { .. }));
    }

    #[test]
    fn keeps_left_join_with_empty_right() {
        let scan = make_scan("t", 3);
        let empty = make_empty(3);
        let schema = make_schema(6);

        let plan = PhysicalPlan::HashJoin {
            left: Box::new(scan),
            right: Box::new(empty),
            join_type: JoinType::Left,
            left_keys: vec![],
            right_keys: vec![],
            schema,
            parallel: false,
            hints: ExecutionHints::default(),
        };

        let result = apply_empty_propagation(plan);

        assert!(matches!(result, PhysicalPlan::HashJoin { .. }));
    }

    #[test]
    fn propagates_empty_through_left_join_left() {
        let empty = make_empty(3);
        let scan = make_scan("t", 3);
        let schema = make_schema(6);

        let plan = PhysicalPlan::HashJoin {
            left: Box::new(empty),
            right: Box::new(scan),
            join_type: JoinType::Left,
            left_keys: vec![],
            right_keys: vec![],
            schema,
            parallel: false,
            hints: ExecutionHints::default(),
        };

        let result = apply_empty_propagation(plan);

        assert!(matches!(result, PhysicalPlan::Empty { .. }));
    }

    #[test]
    fn removes_empty_from_union() {
        let scan = make_scan("t", 3);
        let empty = make_empty(3);
        let schema = make_schema(3);

        let plan = PhysicalPlan::Union {
            inputs: vec![scan.clone(), empty],
            all: true,
            schema,
            parallel: false,
            hints: ExecutionHints::default(),
        };

        let result = apply_empty_propagation(plan);

        assert!(matches!(result, PhysicalPlan::TableScan { .. }));
    }

    #[test]
    fn converts_all_empty_union_to_empty() {
        let empty1 = make_empty(3);
        let empty2 = make_empty(3);
        let schema = make_schema(3);

        let plan = PhysicalPlan::Union {
            inputs: vec![empty1, empty2],
            all: true,
            schema,
            parallel: false,
            hints: ExecutionHints::default(),
        };

        let result = apply_empty_propagation(plan);

        assert!(matches!(result, PhysicalPlan::Empty { .. }));
    }

    #[test]
    fn propagates_empty_through_sort() {
        let empty = make_empty(3);
        let plan = PhysicalPlan::Sort {
            input: Box::new(empty),
            sort_exprs: vec![],
            hints: ExecutionHints::default(),
        };

        let result = apply_empty_propagation(plan);

        assert!(matches!(result, PhysicalPlan::Empty { .. }));
    }

    #[test]
    fn converts_limit_0_to_empty() {
        let scan = make_scan("t", 3);
        let plan = PhysicalPlan::Limit {
            input: Box::new(scan),
            limit: Some(0),
            offset: None,
        };

        let result = apply_empty_propagation(plan);

        assert!(matches!(result, PhysicalPlan::Empty { .. }));
    }

    #[test]
    fn returns_left_when_except_right_is_empty() {
        let scan = make_scan("t", 3);
        let empty = make_empty(3);
        let schema = make_schema(3);

        let plan = PhysicalPlan::Except {
            left: Box::new(scan.clone()),
            right: Box::new(empty),
            all: false,
            schema,
            parallel: false,
            hints: ExecutionHints::default(),
        };

        let result = apply_empty_propagation(plan);

        assert!(matches!(result, PhysicalPlan::TableScan { .. }));
    }

    #[test]
    fn propagates_empty_intersect() {
        let scan = make_scan("t", 3);
        let empty = make_empty(3);
        let schema = make_schema(3);

        let plan = PhysicalPlan::Intersect {
            left: Box::new(scan),
            right: Box::new(empty),
            all: false,
            schema,
            parallel: false,
            hints: ExecutionHints::default(),
        };

        let result = apply_empty_propagation(plan);

        assert!(matches!(result, PhysicalPlan::Empty { .. }));
    }
}
