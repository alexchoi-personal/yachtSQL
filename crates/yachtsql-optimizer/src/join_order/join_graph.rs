#![coverage(off)]

use yachtsql_ir::{Expr, LogicalPlan, PlanSchema};

pub type RelationId = usize;

#[derive(Debug, Clone)]
pub struct JoinRelation {
    pub id: RelationId,
    pub table_name: Option<String>,
    pub original_position: usize,
    pub plan: LogicalPlan,
    pub schema: PlanSchema,
    pub row_count_estimate: usize,
}

#[derive(Debug, Clone)]
pub struct JoinEdge {
    pub left_relation: RelationId,
    pub right_relation: RelationId,
    pub predicate: Expr,
    pub selectivity_estimate: f64,
}

#[derive(Debug)]
pub struct JoinGraph {
    relations: Vec<JoinRelation>,
    edges: Vec<JoinEdge>,
    adjacency: Vec<Vec<usize>>,
}

impl JoinGraph {
    pub fn new() -> Self {
        Self {
            relations: Vec::new(),
            edges: Vec::new(),
            adjacency: Vec::new(),
        }
    }

    pub fn add_relation(&mut self, mut relation: JoinRelation) -> RelationId {
        let id = self.relations.len();
        relation.id = id;
        relation.original_position = id;
        self.relations.push(relation);
        self.adjacency.push(Vec::new());
        id
    }

    pub fn add_edge(&mut self, edge: JoinEdge) {
        let edge_idx = self.edges.len();
        self.adjacency[edge.left_relation].push(edge_idx);
        self.adjacency[edge.right_relation].push(edge_idx);
        self.edges.push(edge);
    }

    pub fn get_edges_between(&self, r1: RelationId, r2: RelationId) -> Vec<&JoinEdge> {
        self.adjacency[r1]
            .iter()
            .filter_map(|&edge_idx| {
                let edge = &self.edges[edge_idx];
                if (edge.left_relation == r1 && edge.right_relation == r2)
                    || (edge.left_relation == r2 && edge.right_relation == r1)
                {
                    Some(edge)
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn get_relation(&self, id: RelationId) -> Option<&JoinRelation> {
        self.relations.get(id)
    }

    pub fn relations(&self) -> &[JoinRelation] {
        &self.relations
    }
}

impl Default for JoinGraph {
    fn default() -> Self {
        Self::new()
    }
}
