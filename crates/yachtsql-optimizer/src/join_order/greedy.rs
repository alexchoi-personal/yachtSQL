#![coverage(off)]

use rustc_hash::{FxHashMap, FxHashSet};
use yachtsql_ir::{Expr, JoinType, LogicalPlan, PlanSchema};

use super::cost_model::{CostModel, JoinCost};
use super::join_graph::{JoinGraph, RelationId};
use crate::planner::predicate::combine_predicates;

fn remap_column_indices(
    expr: &Expr,
    table_offsets: &FxHashMap<String, usize>,
    local_offsets: &FxHashMap<String, FxHashMap<String, usize>>,
) -> Expr {
    match expr {
        Expr::Column {
            table: Some(tbl),
            name,
            ..
        } => {
            let base_offset = table_offsets.get(tbl).copied().unwrap_or(0);
            let local_offset = local_offsets
                .get(tbl)
                .and_then(|m| m.get(name))
                .copied()
                .unwrap_or(0);
            Expr::Column {
                table: Some(tbl.clone()),
                name: name.clone(),
                index: Some(base_offset + local_offset),
            }
        }
        Expr::Column {
            table: None,
            name,
            index,
        } => Expr::Column {
            table: None,
            name: name.clone(),
            index: *index,
        },
        Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
            left: Box::new(remap_column_indices(left, table_offsets, local_offsets)),
            op: *op,
            right: Box::new(remap_column_indices(right, table_offsets, local_offsets)),
        },
        other => other.clone(),
    }
}

pub struct GreedyJoinReorderer {
    cost_model: CostModel,
}

impl GreedyJoinReorderer {
    pub fn new(cost_model: CostModel) -> Self {
        Self { cost_model }
    }

    pub fn reorder(&self, graph: &JoinGraph, original_schema: &PlanSchema) -> LogicalPlan {
        let mut available: FxHashSet<RelationId> = (0..graph.relations().len()).collect();

        let first_id = self.find_smallest_relation(graph, &available);
        available.remove(&first_id);

        let first_rel = graph
            .get_relation(first_id)
            .expect("invariant: relation id must exist in graph");
        let mut current_relations: Vec<RelationId> = vec![first_id];
        let mut current_plan = first_rel.plan.clone();
        let mut current_row_count = first_rel.row_count_estimate;

        let mut table_offsets: FxHashMap<String, usize> = FxHashMap::default();
        let mut local_offsets: FxHashMap<String, FxHashMap<String, usize>> = FxHashMap::default();

        Self::add_relation_offsets(first_rel, &mut table_offsets, &mut local_offsets, 0);
        let mut current_offset = first_rel.schema.fields.len();

        while !available.is_empty() {
            let (next_id, join_cost, predicates) =
                self.find_best_next(graph, &current_relations, current_row_count, &available);

            let next_rel = graph
                .get_relation(next_id)
                .expect("invariant: relation id must exist in graph");
            Self::add_relation_offsets(
                next_rel,
                &mut table_offsets,
                &mut local_offsets,
                current_offset,
            );

            let remapped_predicates: Vec<Expr> = predicates
                .into_iter()
                .map(|p| remap_column_indices(&p, &table_offsets, &local_offsets))
                .collect();
            let condition = combine_predicates(remapped_predicates);

            let new_schema = Self::merge_schemas(current_plan.schema(), &next_rel.schema);
            current_plan = LogicalPlan::Join {
                left: Box::new(current_plan),
                right: Box::new(next_rel.plan.clone()),
                join_type: JoinType::Inner,
                condition,
                schema: new_schema,
            };

            current_relations.push(next_id);
            current_row_count = join_cost.output_rows;
            current_offset += next_rel.schema.fields.len();
            available.remove(&next_id);
        }

        self.maybe_add_schema_restoration_projection(
            graph,
            &current_relations,
            current_plan,
            original_schema,
        )
    }

    fn add_relation_offsets(
        rel: &super::join_graph::JoinRelation,
        table_offsets: &mut FxHashMap<String, usize>,
        local_offsets: &mut FxHashMap<String, FxHashMap<String, usize>>,
        base_offset: usize,
    ) {
        for (idx, field) in rel.schema.fields.iter().enumerate() {
            if let Some(ref table) = field.table {
                table_offsets.entry(table.clone()).or_insert(base_offset);
                local_offsets
                    .entry(table.clone())
                    .or_default()
                    .insert(field.name.clone(), idx);
            }
        }
    }

    fn merge_schemas(left: &PlanSchema, right: &PlanSchema) -> PlanSchema {
        let mut fields = left.fields.clone();
        fields.extend(right.fields.clone());
        PlanSchema { fields }
    }

    fn maybe_add_schema_restoration_projection(
        &self,
        graph: &JoinGraph,
        join_order: &[RelationId],
        plan: LogicalPlan,
        original_schema: &PlanSchema,
    ) -> LogicalPlan {
        let is_original_order = join_order.iter().enumerate().all(|(idx, &rel_id)| {
            graph
                .get_relation(rel_id)
                .map(|r| r.original_position == idx)
                .unwrap_or(false)
        });

        if is_original_order {
            return plan;
        }

        let mut reordered_to_original: Vec<(usize, usize)> = Vec::new();
        let mut reordered_offset = 0;

        for &rel_id in join_order {
            let rel = graph
                .get_relation(rel_id)
                .expect("invariant: relation id must exist in graph");
            let original_offset = self.compute_original_offset(graph, rel.original_position);

            for col_idx in 0..rel.schema.fields.len() {
                reordered_to_original.push((reordered_offset + col_idx, original_offset + col_idx));
            }
            reordered_offset += rel.schema.fields.len();
        }

        reordered_to_original.sort_by_key(|&(_, orig)| orig);

        let expressions: Vec<Expr> = reordered_to_original
            .iter()
            .map(|&(reordered_idx, _)| {
                let field = &plan.schema().fields[reordered_idx];
                Expr::Column {
                    table: field.table.clone(),
                    name: field.name.clone(),
                    index: Some(reordered_idx),
                }
            })
            .collect();

        LogicalPlan::Project {
            input: Box::new(plan),
            expressions,
            schema: original_schema.clone(),
        }
    }

    fn compute_original_offset(&self, graph: &JoinGraph, position: usize) -> usize {
        graph.relations()[..position]
            .iter()
            .map(|r| r.schema.fields.len())
            .sum()
    }

    fn find_best_next(
        &self,
        graph: &JoinGraph,
        current_relations: &[RelationId],
        current_row_count: usize,
        available: &FxHashSet<RelationId>,
    ) -> (RelationId, JoinCost, Vec<Expr>) {
        let mut best: Option<(RelationId, JoinCost, usize, Vec<Expr>)> = None;

        for &candidate_id in available {
            let candidate = graph
                .get_relation(candidate_id)
                .expect("invariant: relation id must exist in graph");

            let mut applicable_edges = Vec::new();
            for &rel_id in current_relations {
                applicable_edges.extend(graph.get_edges_between(rel_id, candidate_id));
            }

            let cost = self.cost_model.estimate_join_cost(
                current_row_count,
                candidate.row_count_estimate,
                &applicable_edges,
            );

            let predicates: Vec<Expr> = applicable_edges
                .iter()
                .map(|e| e.predicate.clone())
                .collect();

            let should_update = match &best {
                None => true,
                Some((_, best_cost, best_pos, _)) => {
                    cost.total_cost < best_cost.total_cost
                        || (cost.total_cost == best_cost.total_cost
                            && candidate.original_position < *best_pos)
                }
            };

            if should_update {
                best = Some((candidate_id, cost, candidate.original_position, predicates));
            }
        }

        let (id, cost, _, predicates) =
            best.expect("invariant: available set is non-empty, must find a candidate");
        (id, cost, predicates)
    }

    fn find_smallest_relation(
        &self,
        graph: &JoinGraph,
        available: &FxHashSet<RelationId>,
    ) -> RelationId {
        available
            .iter()
            .min_by_key(|&&id| {
                graph
                    .get_relation(id)
                    .map(|r| (r.row_count_estimate, r.original_position))
                    .unwrap_or((usize::MAX, usize::MAX))
            })
            .copied()
            .expect("invariant: available set must be non-empty")
    }
}
