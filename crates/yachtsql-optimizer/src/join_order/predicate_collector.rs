#![coverage(off)]

use yachtsql_ir::{BinaryOp, Expr, JoinType, LogicalPlan};

use super::cost_model::CostModel;
use super::join_graph::{JoinEdge, JoinGraph, JoinRelation};

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
            if let Some(edge) = Self::build_edge(&graph, &predicate) {
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

    fn build_edge(graph: &JoinGraph, predicate: &Expr) -> Option<JoinEdge> {
        if let Expr::BinaryOp {
            left,
            op: BinaryOp::Eq,
            right,
        } = predicate
        {
            let left_rel = Self::find_relation_for_expr(graph, left)?;
            let right_rel = Self::find_relation_for_expr(graph, right)?;

            if left_rel != right_rel {
                return Some(JoinEdge {
                    left_relation: left_rel,
                    right_relation: right_rel,
                    predicate: predicate.clone(),
                    selectivity_estimate: 0.1,
                });
            }
        }
        None
    }

    fn find_relation_for_expr(graph: &JoinGraph, expr: &Expr) -> Option<usize> {
        if let Expr::Column {
            table: Some(table_name),
            ..
        } = expr
        {
            for (idx, rel) in graph.relations().iter().enumerate() {
                if rel.table_name.as_ref() == Some(table_name) {
                    return Some(idx);
                }
            }
        }
        None
    }
}
