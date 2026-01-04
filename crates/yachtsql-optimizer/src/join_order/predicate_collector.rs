use yachtsql_ir::{BinaryOp, Expr, JoinType, Literal, LogicalPlan};

use super::cost_model::CostModel;
use super::join_graph::{JoinEdge, JoinGraph, JoinRelation};

fn get_column_distinct_count(expr: &Expr, cost_model: &CostModel) -> Option<usize> {
    match expr {
        Expr::Column {
            table: Some(table_name),
            name,
            ..
        } => cost_model
            .get_column_stats(table_name, name)
            .map(|stats| stats.distinct_count),
        _ => None,
    }
}

fn get_null_ratio(expr: &Expr, cost_model: &CostModel) -> Option<f64> {
    match expr {
        Expr::Column {
            table: Some(table_name),
            name,
            ..
        } => {
            let table_stats = cost_model.get_table_stats(table_name)?;
            let column_stats = table_stats.column_stats.get(name)?;
            let row_count = table_stats.row_count;
            if row_count == 0 {
                return Some(0.0);
            }
            Some(column_stats.null_count as f64 / row_count as f64)
        }
        _ => None,
    }
}

fn is_prefix_pattern(pattern: &Expr) -> bool {
    match pattern {
        Expr::Literal(Literal::String(s)) => !s.starts_with('%') && !s.starts_with('_'),
        _ => false,
    }
}

pub fn estimate_selectivity(predicate: &Expr, cost_model: &CostModel) -> f64 {
    match predicate {
        Expr::BinaryOp {
            left,
            op: BinaryOp::Eq,
            right,
        } => {
            let left_distinct = get_column_distinct_count(left, cost_model);
            let right_distinct = get_column_distinct_count(right, cost_model);

            match (left_distinct, right_distinct) {
                (Some(ld), Some(rd)) if ld.max(rd) > 0 => 1.0 / ld.max(rd) as f64,
                (Some(d), None) | (None, Some(d)) if d > 0 => 1.0 / d as f64,
                _ => 0.1,
            }
        }

        Expr::BinaryOp {
            op: BinaryOp::Lt | BinaryOp::Gt | BinaryOp::LtEq | BinaryOp::GtEq,
            ..
        } => 0.3,

        Expr::Like {
            pattern, negated, ..
        } => {
            let base = if is_prefix_pattern(pattern) { 0.1 } else { 0.5 };
            if *negated { 1.0 - base } else { base }
        }

        Expr::InList { list, negated, .. } => {
            let list_selectivity = (list.len() as f64 * 0.1).min(0.5);
            if *negated {
                1.0 - list_selectivity
            } else {
                list_selectivity
            }
        }

        Expr::IsNull { expr, negated } => {
            let null_ratio = get_null_ratio(expr, cost_model).unwrap_or(0.01);
            if *negated {
                1.0 - null_ratio
            } else {
                null_ratio
            }
        }

        _ => 0.1,
    }
}

pub struct PredicateCollector;

impl PredicateCollector {
    pub fn build_join_graph(plan: &LogicalPlan, cost_model: &CostModel) -> Option<JoinGraph> {
        let mut graph = JoinGraph::new();
        let mut predicates = Vec::new();

        if !Self::collect_joins(plan, &mut graph, &mut predicates, cost_model) {
            return None;
        }

        if graph.relations().len() < 2 {
            return None;
        }

        for predicate in predicates {
            if let Some(edge) = Self::build_edge(&graph, &predicate, cost_model) {
                graph.add_edge(edge);
            }
        }

        Some(graph)
    }

    fn collect_joins(
        plan: &LogicalPlan,
        graph: &mut JoinGraph,
        predicates: &mut Vec<Expr>,
        cost_model: &CostModel,
    ) -> bool {
        match plan {
            LogicalPlan::Join {
                left,
                right,
                join_type,
                condition,
                ..
            } => {
                if *join_type != JoinType::Inner {
                    return false;
                }

                if !Self::collect_joins(left, graph, predicates, cost_model) {
                    return false;
                }
                if !Self::collect_joins(right, graph, predicates, cost_model) {
                    return false;
                }

                if let Some(cond) = condition {
                    Self::extract_predicates(cond, predicates);
                }

                true
            }

            LogicalPlan::Scan {
                table_name, schema, ..
            } => {
                let row_count = cost_model.estimate_base_cardinality(table_name);
                let relation = JoinRelation {
                    id: 0,
                    table_name: Some(table_name.clone()),
                    original_position: 0,
                    plan: plan.clone(),
                    schema: schema.clone(),
                    row_count_estimate: row_count,
                };
                graph.add_relation(relation);
                true
            }

            LogicalPlan::Filter { input, predicate } => {
                if !Self::collect_joins(input, graph, predicates, cost_model) {
                    return false;
                }
                Self::extract_predicates(predicate, predicates);
                true
            }

            _ => false,
        }
    }

    pub fn find_join_subtree(plan: &LogicalPlan) -> &LogicalPlan {
        match plan {
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Distinct { input, .. } => Self::find_join_subtree(input),
            _ => plan,
        }
    }

    fn extract_predicates(expr: &Expr, predicates: &mut Vec<Expr>) {
        match expr {
            Expr::BinaryOp {
                left,
                op: BinaryOp::And,
                right,
            } => {
                Self::extract_predicates(left, predicates);
                Self::extract_predicates(right, predicates);
            }
            other => {
                predicates.push(other.clone());
            }
        }
    }

    fn build_edge(graph: &JoinGraph, predicate: &Expr, cost_model: &CostModel) -> Option<JoinEdge> {
        if let Expr::BinaryOp {
            left,
            op: BinaryOp::Eq,
            right,
        } = predicate
        {
            let left_rel = Self::find_relation_for_expr(graph, left)?;
            let right_rel = Self::find_relation_for_expr(graph, right)?;

            if left_rel != right_rel {
                let selectivity = estimate_selectivity(predicate, cost_model);
                return Some(JoinEdge {
                    left_relation: left_rel,
                    right_relation: right_rel,
                    predicate: predicate.clone(),
                    selectivity_estimate: selectivity,
                });
            }
        }
        None
    }

    pub fn has_non_equality_join_predicates(plan: &LogicalPlan) -> bool {
        let mut predicates = Vec::new();
        Self::collect_join_predicates(plan, &mut predicates);
        predicates.iter().any(|p| !Self::is_equality_predicate(p))
    }

    fn collect_join_predicates(plan: &LogicalPlan, predicates: &mut Vec<Expr>) {
        match plan {
            LogicalPlan::Join {
                left,
                right,
                condition,
                ..
            } => {
                if let Some(cond) = condition {
                    Self::extract_predicates(cond, predicates);
                }
                Self::collect_join_predicates(left, predicates);
                Self::collect_join_predicates(right, predicates);
            }
            LogicalPlan::Filter { input, .. } => {
                Self::collect_join_predicates(input, predicates);
            }
            _ => {}
        }
    }

    fn is_equality_predicate(expr: &Expr) -> bool {
        matches!(
            expr,
            Expr::BinaryOp {
                op: BinaryOp::Eq,
                ..
            }
        )
    }

    fn find_relation_for_expr(graph: &JoinGraph, expr: &Expr) -> Option<usize> {
        let mut columns = Vec::new();
        Self::collect_column_refs(expr, &mut columns);

        for table_name in columns {
            for (idx, rel) in graph.relations().iter().enumerate() {
                if rel.table_name.as_ref() == Some(&table_name) {
                    return Some(idx);
                }
                for field in &rel.schema.fields {
                    if field.table.as_ref() == Some(&table_name) {
                        return Some(idx);
                    }
                }
            }
        }
        None
    }

    fn collect_column_refs(expr: &Expr, tables: &mut Vec<String>) {
        match expr {
            Expr::Column {
                table: Some(table_name),
                ..
            } => {
                tables.push(table_name.clone());
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::collect_column_refs(left, tables);
                Self::collect_column_refs(right, tables);
            }
            Expr::UnaryOp { expr, .. } => {
                Self::collect_column_refs(expr, tables);
            }
            Expr::ScalarFunction { args, .. } => {
                for arg in args {
                    Self::collect_column_refs(arg, tables);
                }
            }
            Expr::Cast { expr, .. } => {
                Self::collect_column_refs(expr, tables);
            }
            Expr::Aggregate { args, .. } => {
                for arg in args {
                    Self::collect_column_refs(arg, tables);
                }
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use rustc_hash::FxHashMap;

    use super::*;
    use crate::stats::{ColumnStats, TableStats};

    fn create_cost_model_with_stats() -> CostModel {
        let mut table_stats = FxHashMap::default();

        let mut orders_stats = TableStats::new(1000);
        orders_stats.column_stats.insert(
            "id".to_string(),
            ColumnStats {
                distinct_count: 1000,
                null_count: 0,
                min_value: None,
                max_value: None,
            },
        );
        orders_stats.column_stats.insert(
            "customer_id".to_string(),
            ColumnStats {
                distinct_count: 100,
                null_count: 50,
                min_value: None,
                max_value: None,
            },
        );
        table_stats.insert("ORDERS".to_string(), orders_stats);

        let mut customers_stats = TableStats::new(100);
        customers_stats.column_stats.insert(
            "id".to_string(),
            ColumnStats {
                distinct_count: 100,
                null_count: 0,
                min_value: None,
                max_value: None,
            },
        );
        table_stats.insert("CUSTOMERS".to_string(), customers_stats);

        CostModel::with_stats(table_stats)
    }

    #[test]
    fn test_estimate_selectivity_equality_with_stats() {
        let cost_model = create_cost_model_with_stats();

        let predicate = Expr::BinaryOp {
            left: Box::new(Expr::Column {
                table: Some("orders".to_string()),
                name: "customer_id".to_string(),
                index: None,
            }),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Column {
                table: Some("customers".to_string()),
                name: "id".to_string(),
                index: None,
            }),
        };

        let selectivity = estimate_selectivity(&predicate, &cost_model);
        assert!((selectivity - 0.01).abs() < 0.0001);
    }

    #[test]
    fn test_estimate_selectivity_equality_no_stats() {
        let cost_model = CostModel::new();

        let predicate = Expr::BinaryOp {
            left: Box::new(Expr::Column {
                table: Some("unknown_table".to_string()),
                name: "col".to_string(),
                index: None,
            }),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Literal(Literal::Int64(42))),
        };

        let selectivity = estimate_selectivity(&predicate, &cost_model);
        assert!((selectivity - 0.1).abs() < 0.0001);
    }

    #[test]
    fn test_estimate_selectivity_range_predicates() {
        let cost_model = CostModel::new();

        let lt_predicate = Expr::BinaryOp {
            left: Box::new(Expr::Column {
                table: Some("orders".to_string()),
                name: "amount".to_string(),
                index: None,
            }),
            op: BinaryOp::Lt,
            right: Box::new(Expr::Literal(Literal::Int64(100))),
        };
        assert!((estimate_selectivity(&lt_predicate, &cost_model) - 0.3).abs() < 0.0001);

        let gt_predicate = Expr::BinaryOp {
            left: Box::new(Expr::Column {
                table: Some("orders".to_string()),
                name: "amount".to_string(),
                index: None,
            }),
            op: BinaryOp::Gt,
            right: Box::new(Expr::Literal(Literal::Int64(100))),
        };
        assert!((estimate_selectivity(&gt_predicate, &cost_model) - 0.3).abs() < 0.0001);

        let lteq_predicate = Expr::BinaryOp {
            left: Box::new(Expr::Column {
                table: Some("orders".to_string()),
                name: "amount".to_string(),
                index: None,
            }),
            op: BinaryOp::LtEq,
            right: Box::new(Expr::Literal(Literal::Int64(100))),
        };
        assert!((estimate_selectivity(&lteq_predicate, &cost_model) - 0.3).abs() < 0.0001);

        let gteq_predicate = Expr::BinaryOp {
            left: Box::new(Expr::Column {
                table: Some("orders".to_string()),
                name: "amount".to_string(),
                index: None,
            }),
            op: BinaryOp::GtEq,
            right: Box::new(Expr::Literal(Literal::Int64(100))),
        };
        assert!((estimate_selectivity(&gteq_predicate, &cost_model) - 0.3).abs() < 0.0001);
    }

    #[test]
    fn test_estimate_selectivity_like_prefix() {
        let cost_model = CostModel::new();

        let predicate = Expr::Like {
            expr: Box::new(Expr::Column {
                table: Some("customers".to_string()),
                name: "name".to_string(),
                index: None,
            }),
            pattern: Box::new(Expr::Literal(Literal::String("John%".to_string()))),
            negated: false,
            case_insensitive: false,
        };

        let selectivity = estimate_selectivity(&predicate, &cost_model);
        assert!((selectivity - 0.1).abs() < 0.0001);
    }

    #[test]
    fn test_estimate_selectivity_like_no_prefix() {
        let cost_model = CostModel::new();

        let predicate = Expr::Like {
            expr: Box::new(Expr::Column {
                table: Some("customers".to_string()),
                name: "name".to_string(),
                index: None,
            }),
            pattern: Box::new(Expr::Literal(Literal::String("%Smith".to_string()))),
            negated: false,
            case_insensitive: false,
        };

        let selectivity = estimate_selectivity(&predicate, &cost_model);
        assert!((selectivity - 0.5).abs() < 0.0001);
    }

    #[test]
    fn test_estimate_selectivity_like_underscore_prefix() {
        let cost_model = CostModel::new();

        let predicate = Expr::Like {
            expr: Box::new(Expr::Column {
                table: Some("customers".to_string()),
                name: "code".to_string(),
                index: None,
            }),
            pattern: Box::new(Expr::Literal(Literal::String("_ABC".to_string()))),
            negated: false,
            case_insensitive: false,
        };

        let selectivity = estimate_selectivity(&predicate, &cost_model);
        assert!((selectivity - 0.5).abs() < 0.0001);
    }

    #[test]
    fn test_estimate_selectivity_like_negated() {
        let cost_model = CostModel::new();

        let predicate = Expr::Like {
            expr: Box::new(Expr::Column {
                table: Some("customers".to_string()),
                name: "name".to_string(),
                index: None,
            }),
            pattern: Box::new(Expr::Literal(Literal::String("John%".to_string()))),
            negated: true,
            case_insensitive: false,
        };

        let selectivity = estimate_selectivity(&predicate, &cost_model);
        assert!((selectivity - 0.9).abs() < 0.0001);
    }

    #[test]
    fn test_estimate_selectivity_in_list_small() {
        let cost_model = CostModel::new();

        let predicate = Expr::InList {
            expr: Box::new(Expr::Column {
                table: Some("orders".to_string()),
                name: "status".to_string(),
                index: None,
            }),
            list: vec![
                Expr::Literal(Literal::String("pending".to_string())),
                Expr::Literal(Literal::String("processing".to_string())),
            ],
            negated: false,
        };

        let selectivity = estimate_selectivity(&predicate, &cost_model);
        assert!((selectivity - 0.2).abs() < 0.0001);
    }

    #[test]
    fn test_estimate_selectivity_in_list_large() {
        let cost_model = CostModel::new();

        let predicate = Expr::InList {
            expr: Box::new(Expr::Column {
                table: Some("orders".to_string()),
                name: "status".to_string(),
                index: None,
            }),
            list: vec![
                Expr::Literal(Literal::String("a".to_string())),
                Expr::Literal(Literal::String("b".to_string())),
                Expr::Literal(Literal::String("c".to_string())),
                Expr::Literal(Literal::String("d".to_string())),
                Expr::Literal(Literal::String("e".to_string())),
                Expr::Literal(Literal::String("f".to_string())),
            ],
            negated: false,
        };

        let selectivity = estimate_selectivity(&predicate, &cost_model);
        assert!((selectivity - 0.5).abs() < 0.0001);
    }

    #[test]
    fn test_estimate_selectivity_in_list_negated() {
        let cost_model = CostModel::new();

        let predicate = Expr::InList {
            expr: Box::new(Expr::Column {
                table: Some("orders".to_string()),
                name: "status".to_string(),
                index: None,
            }),
            list: vec![Expr::Literal(Literal::String("cancelled".to_string()))],
            negated: true,
        };

        let selectivity = estimate_selectivity(&predicate, &cost_model);
        assert!((selectivity - 0.9).abs() < 0.0001);
    }

    #[test]
    fn test_estimate_selectivity_is_null_with_stats() {
        let cost_model = create_cost_model_with_stats();

        let predicate = Expr::IsNull {
            expr: Box::new(Expr::Column {
                table: Some("orders".to_string()),
                name: "customer_id".to_string(),
                index: None,
            }),
            negated: false,
        };

        let selectivity = estimate_selectivity(&predicate, &cost_model);
        assert!((selectivity - 0.05).abs() < 0.0001);
    }

    #[test]
    fn test_estimate_selectivity_is_not_null_with_stats() {
        let cost_model = create_cost_model_with_stats();

        let predicate = Expr::IsNull {
            expr: Box::new(Expr::Column {
                table: Some("orders".to_string()),
                name: "customer_id".to_string(),
                index: None,
            }),
            negated: true,
        };

        let selectivity = estimate_selectivity(&predicate, &cost_model);
        assert!((selectivity - 0.95).abs() < 0.0001);
    }

    #[test]
    fn test_estimate_selectivity_is_null_no_stats() {
        let cost_model = CostModel::new();

        let predicate = Expr::IsNull {
            expr: Box::new(Expr::Column {
                table: Some("unknown".to_string()),
                name: "col".to_string(),
                index: None,
            }),
            negated: false,
        };

        let selectivity = estimate_selectivity(&predicate, &cost_model);
        assert!((selectivity - 0.01).abs() < 0.0001);
    }

    #[test]
    fn test_estimate_selectivity_is_not_null_no_stats() {
        let cost_model = CostModel::new();

        let predicate = Expr::IsNull {
            expr: Box::new(Expr::Column {
                table: Some("unknown".to_string()),
                name: "col".to_string(),
                index: None,
            }),
            negated: true,
        };

        let selectivity = estimate_selectivity(&predicate, &cost_model);
        assert!((selectivity - 0.99).abs() < 0.0001);
    }

    #[test]
    fn test_estimate_selectivity_default_fallback() {
        let cost_model = CostModel::new();

        let predicate = Expr::BinaryOp {
            left: Box::new(Expr::Column {
                table: Some("orders".to_string()),
                name: "a".to_string(),
                index: None,
            }),
            op: BinaryOp::And,
            right: Box::new(Expr::Column {
                table: Some("orders".to_string()),
                name: "b".to_string(),
                index: None,
            }),
        };

        let selectivity = estimate_selectivity(&predicate, &cost_model);
        assert!((selectivity - 0.1).abs() < 0.0001);
    }

    #[test]
    fn test_estimate_selectivity_equality_one_side_stats() {
        let cost_model = create_cost_model_with_stats();

        let predicate = Expr::BinaryOp {
            left: Box::new(Expr::Column {
                table: Some("orders".to_string()),
                name: "customer_id".to_string(),
                index: None,
            }),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Literal(Literal::Int64(42))),
        };

        let selectivity = estimate_selectivity(&predicate, &cost_model);
        assert!((selectivity - 0.01).abs() < 0.0001);
    }

    #[test]
    fn test_is_prefix_pattern() {
        assert!(is_prefix_pattern(&Expr::Literal(Literal::String(
            "John%".to_string()
        ))));
        assert!(is_prefix_pattern(&Expr::Literal(Literal::String(
            "ABC".to_string()
        ))));
        assert!(!is_prefix_pattern(&Expr::Literal(Literal::String(
            "%Smith".to_string()
        ))));
        assert!(!is_prefix_pattern(&Expr::Literal(Literal::String(
            "_ABC".to_string()
        ))));

        assert!(!is_prefix_pattern(&Expr::Column {
            table: Some("t".to_string()),
            name: "c".to_string(),
            index: None,
        }));
    }

    #[test]
    fn test_get_column_distinct_count() {
        let cost_model = create_cost_model_with_stats();

        let expr = Expr::Column {
            table: Some("orders".to_string()),
            name: "customer_id".to_string(),
            index: None,
        };
        assert_eq!(get_column_distinct_count(&expr, &cost_model), Some(100));

        let expr_no_table = Expr::Column {
            table: None,
            name: "customer_id".to_string(),
            index: None,
        };
        assert_eq!(get_column_distinct_count(&expr_no_table, &cost_model), None);

        let literal = Expr::Literal(Literal::Int64(42));
        assert_eq!(get_column_distinct_count(&literal, &cost_model), None);
    }

    #[test]
    fn test_get_null_ratio() {
        let cost_model = create_cost_model_with_stats();

        let expr = Expr::Column {
            table: Some("orders".to_string()),
            name: "customer_id".to_string(),
            index: None,
        };
        let ratio = get_null_ratio(&expr, &cost_model);
        assert!(ratio.is_some());
        assert!((ratio.unwrap() - 0.05).abs() < 0.0001);

        let expr_no_nulls = Expr::Column {
            table: Some("orders".to_string()),
            name: "id".to_string(),
            index: None,
        };
        let ratio = get_null_ratio(&expr_no_nulls, &cost_model);
        assert!(ratio.is_some());
        assert!((ratio.unwrap() - 0.0).abs() < 0.0001);

        let expr_unknown = Expr::Column {
            table: Some("unknown".to_string()),
            name: "col".to_string(),
            index: None,
        };
        assert!(get_null_ratio(&expr_unknown, &cost_model).is_none());
    }

    #[test]
    fn test_get_null_ratio_zero_row_count() {
        let mut table_stats = FxHashMap::default();
        let mut empty_table = TableStats::new(0);
        empty_table.column_stats.insert(
            "col".to_string(),
            ColumnStats {
                distinct_count: 0,
                null_count: 0,
                min_value: None,
                max_value: None,
            },
        );
        table_stats.insert("EMPTY".to_string(), empty_table);
        let cost_model = CostModel::with_stats(table_stats);

        let expr = Expr::Column {
            table: Some("empty".to_string()),
            name: "col".to_string(),
            index: None,
        };
        let ratio = get_null_ratio(&expr, &cost_model);
        assert!(ratio.is_some());
        assert!((ratio.unwrap() - 0.0).abs() < 0.0001);
    }
}
