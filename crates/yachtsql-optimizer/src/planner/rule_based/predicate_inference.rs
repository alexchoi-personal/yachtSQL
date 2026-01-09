#![coverage(off)]

use yachtsql_ir::{BinaryOp, Expr};

use crate::PhysicalPlan;
use crate::planner::predicate::{combine_predicates, split_and_predicates};

struct EquivalenceClass {
    members: Vec<Expr>,
    constant: Option<Expr>,
}

impl EquivalenceClass {
    fn new() -> Self {
        Self {
            members: Vec::new(),
            constant: None,
        }
    }

    fn add_member(&mut self, expr: Expr) {
        if !self.members.iter().any(|m| exprs_equal(m, &expr)) {
            self.members.push(expr);
        }
    }

    fn set_constant(&mut self, expr: Expr) {
        if self.constant.is_none() {
            self.constant = Some(expr);
        }
    }

    fn contains(&self, expr: &Expr) -> bool {
        self.members.iter().any(|m| exprs_equal(m, expr))
    }

    fn merge_from(&mut self, other: EquivalenceClass) {
        for member in other.members {
            self.add_member(member);
        }
        if self.constant.is_none() {
            self.constant = other.constant;
        }
    }
}

fn is_literal(expr: &Expr) -> bool {
    matches!(expr, Expr::Literal(_))
}

fn exprs_equal(a: &Expr, b: &Expr) -> bool {
    match (a, b) {
        (
            Expr::Column {
                table: t1,
                name: n1,
                index: i1,
            },
            Expr::Column {
                table: t2,
                name: n2,
                index: i2,
            },
        ) => t1 == t2 && n1 == n2 && i1 == i2,
        (Expr::Literal(l1), Expr::Literal(l2)) => l1 == l2,
        (
            Expr::BinaryOp {
                left: l1,
                op: o1,
                right: r1,
            },
            Expr::BinaryOp {
                left: l2,
                op: o2,
                right: r2,
            },
        ) => o1 == o2 && exprs_equal(l1, l2) && exprs_equal(r1, r2),
        (Expr::UnaryOp { op: o1, expr: e1 }, Expr::UnaryOp { op: o2, expr: e2 }) => {
            o1 == o2 && exprs_equal(e1, e2)
        }
        (
            Expr::Cast {
                expr: e1,
                data_type: d1,
                safe: s1,
            },
            Expr::Cast {
                expr: e2,
                data_type: d2,
                safe: s2,
            },
        ) => d1 == d2 && s1 == s2 && exprs_equal(e1, e2),
        (
            Expr::ScalarFunction { name: n1, args: a1 },
            Expr::ScalarFunction { name: n2, args: a2 },
        ) => {
            n1 == n2
                && a1.len() == a2.len()
                && a1.iter().zip(a2.iter()).all(|(x, y)| exprs_equal(x, y))
        }
        (
            Expr::IsNull {
                expr: e1,
                negated: n1,
            },
            Expr::IsNull {
                expr: e2,
                negated: n2,
            },
        ) => n1 == n2 && exprs_equal(e1, e2),
        _ => false,
    }
}

fn build_equivalence_classes(predicates: &[Expr]) -> Vec<EquivalenceClass> {
    let mut classes: Vec<EquivalenceClass> = Vec::new();

    for pred in predicates {
        if let Expr::BinaryOp {
            left,
            op: BinaryOp::Eq,
            right,
        } = pred
        {
            let left_is_literal = is_literal(left);
            let right_is_literal = is_literal(right);

            match (left_is_literal, right_is_literal) {
                (true, true) => {}
                (true, false) => {
                    let class_idx = find_class_for_expr(&classes, right);
                    match class_idx {
                        Some(idx) => {
                            classes[idx].set_constant(left.as_ref().clone());
                        }
                        None => {
                            let mut new_class = EquivalenceClass::new();
                            new_class.add_member(right.as_ref().clone());
                            new_class.set_constant(left.as_ref().clone());
                            classes.push(new_class);
                        }
                    }
                }
                (false, true) => {
                    let class_idx = find_class_for_expr(&classes, left);
                    match class_idx {
                        Some(idx) => {
                            classes[idx].set_constant(right.as_ref().clone());
                        }
                        None => {
                            let mut new_class = EquivalenceClass::new();
                            new_class.add_member(left.as_ref().clone());
                            new_class.set_constant(right.as_ref().clone());
                            classes.push(new_class);
                        }
                    }
                }
                (false, false) => {
                    let left_class_idx = find_class_for_expr(&classes, left);
                    let right_class_idx = find_class_for_expr(&classes, right);

                    match (left_class_idx, right_class_idx) {
                        (None, None) => {
                            let mut new_class = EquivalenceClass::new();
                            new_class.add_member(left.as_ref().clone());
                            new_class.add_member(right.as_ref().clone());
                            classes.push(new_class);
                        }
                        (Some(idx), None) => {
                            classes[idx].add_member(right.as_ref().clone());
                        }
                        (None, Some(idx)) => {
                            classes[idx].add_member(left.as_ref().clone());
                        }
                        (Some(left_idx), Some(right_idx)) => {
                            if left_idx != right_idx {
                                let right_class = classes.remove(right_idx);
                                let adjusted_left_idx = if left_idx > right_idx {
                                    left_idx - 1
                                } else {
                                    left_idx
                                };
                                classes[adjusted_left_idx].merge_from(right_class);
                            }
                        }
                    }
                }
            }
        }
    }

    classes
}

fn find_class_for_expr(classes: &[EquivalenceClass], expr: &Expr) -> Option<usize> {
    classes.iter().position(|c| c.contains(expr))
}

fn is_range_predicate(pred: &Expr) -> bool {
    match pred {
        Expr::BinaryOp { op, .. } => matches!(
            op,
            BinaryOp::Gt | BinaryOp::Lt | BinaryOp::GtEq | BinaryOp::LtEq
        ),
        Expr::Between { .. } => true,
        Expr::InList { .. } => true,
        _ => false,
    }
}

fn get_range_predicate_subject(pred: &Expr) -> Option<&Expr> {
    match pred {
        Expr::BinaryOp {
            left,
            op: BinaryOp::Gt | BinaryOp::Lt | BinaryOp::GtEq | BinaryOp::LtEq,
            right,
        } => {
            if is_literal(right) {
                Some(left.as_ref())
            } else if is_literal(left) {
                Some(right.as_ref())
            } else {
                None
            }
        }
        Expr::Between { expr, .. } => Some(expr.as_ref()),
        Expr::InList { expr, .. } => Some(expr.as_ref()),
        _ => None,
    }
}

fn substitute_expr_in_predicate(pred: &Expr, old_expr: &Expr, new_expr: &Expr) -> Expr {
    match pred {
        Expr::BinaryOp { left, op, right } => {
            let new_left = if exprs_equal(left, old_expr) {
                new_expr.clone()
            } else {
                left.as_ref().clone()
            };
            let new_right = if exprs_equal(right, old_expr) {
                new_expr.clone()
            } else {
                right.as_ref().clone()
            };
            Expr::BinaryOp {
                left: Box::new(new_left),
                op: *op,
                right: Box::new(new_right),
            }
        }
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let new_expr_inner = if exprs_equal(expr, old_expr) {
                new_expr.clone()
            } else {
                expr.as_ref().clone()
            };
            Expr::Between {
                expr: Box::new(new_expr_inner),
                low: low.clone(),
                high: high.clone(),
                negated: *negated,
            }
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let new_expr_inner = if exprs_equal(expr, old_expr) {
                new_expr.clone()
            } else {
                expr.as_ref().clone()
            };
            Expr::InList {
                expr: Box::new(new_expr_inner),
                list: list.clone(),
                negated: *negated,
            }
        }
        other => other.clone(),
    }
}

fn derive_new_predicates(classes: &[EquivalenceClass], predicates: &[Expr]) -> Vec<Expr> {
    let mut new_predicates: Vec<Expr> = Vec::new();

    for class in classes {
        if let Some(constant) = &class.constant {
            for member in &class.members {
                let eq_pred = Expr::BinaryOp {
                    left: Box::new(member.clone()),
                    op: BinaryOp::Eq,
                    right: Box::new(constant.clone()),
                };
                if !predicate_exists(&eq_pred, predicates)
                    && !predicate_exists(&eq_pred, &new_predicates)
                {
                    new_predicates.push(eq_pred);
                }
            }
        }
    }

    for pred in predicates {
        if !is_range_predicate(pred) {
            continue;
        }

        let subject = match get_range_predicate_subject(pred) {
            Some(s) => s,
            None => continue,
        };

        for class in classes {
            if !class.contains(subject) {
                continue;
            }

            for member in &class.members {
                if exprs_equal(member, subject) {
                    continue;
                }

                let new_pred = substitute_expr_in_predicate(pred, subject, member);

                if !predicate_exists(&new_pred, predicates)
                    && !predicate_exists(&new_pred, &new_predicates)
                {
                    new_predicates.push(new_pred);
                }
            }
        }
    }

    new_predicates
}

fn predicate_exists(pred: &Expr, predicates: &[Expr]) -> bool {
    predicates.iter().any(|p| exprs_equal(p, pred))
}

fn apply_inference_to_predicate(predicate: Expr) -> Expr {
    let predicates = split_and_predicates(&predicate);
    let classes = build_equivalence_classes(&predicates);
    let new_predicates = derive_new_predicates(&classes, &predicates);

    if new_predicates.is_empty() {
        return predicate;
    }

    let mut all_predicates = predicates;
    all_predicates.extend(new_predicates);

    combine_predicates(all_predicates).unwrap_or(predicate)
}

pub fn apply_predicate_inference(plan: PhysicalPlan) -> PhysicalPlan {
    match plan {
        PhysicalPlan::Filter { input, predicate } => {
            let optimized_input = apply_predicate_inference(*input);
            let optimized_predicate = apply_inference_to_predicate(predicate);

            PhysicalPlan::Filter {
                input: Box::new(optimized_input),
                predicate: optimized_predicate,
            }
        }

        PhysicalPlan::Project {
            input,
            expressions,
            schema,
        } => PhysicalPlan::Project {
            input: Box::new(apply_predicate_inference(*input)),
            expressions,
            schema,
        },

        PhysicalPlan::HashAggregate {
            input,
            group_by,
            aggregates,
            schema,
            grouping_sets,
            hints,
        } => PhysicalPlan::HashAggregate {
            input: Box::new(apply_predicate_inference(*input)),
            group_by,
            aggregates,
            schema,
            grouping_sets,
            hints,
        },

        PhysicalPlan::HashJoin {
            left,
            right,
            join_type,
            left_keys,
            right_keys,
            schema,
            parallel,
            hints,
        } => PhysicalPlan::HashJoin {
            left: Box::new(apply_predicate_inference(*left)),
            right: Box::new(apply_predicate_inference(*right)),
            join_type,
            left_keys,
            right_keys,
            schema,
            parallel,
            hints,
        },

        PhysicalPlan::NestedLoopJoin {
            left,
            right,
            join_type,
            condition,
            schema,
            parallel,
            hints,
        } => PhysicalPlan::NestedLoopJoin {
            left: Box::new(apply_predicate_inference(*left)),
            right: Box::new(apply_predicate_inference(*right)),
            join_type,
            condition,
            schema,
            parallel,
            hints,
        },

        PhysicalPlan::CrossJoin {
            left,
            right,
            schema,
            parallel,
            hints,
        } => PhysicalPlan::CrossJoin {
            left: Box::new(apply_predicate_inference(*left)),
            right: Box::new(apply_predicate_inference(*right)),
            schema,
            parallel,
            hints,
        },

        PhysicalPlan::Sort {
            input,
            sort_exprs,
            hints,
        } => PhysicalPlan::Sort {
            input: Box::new(apply_predicate_inference(*input)),
            sort_exprs,
            hints,
        },

        PhysicalPlan::TopN {
            input,
            sort_exprs,
            limit,
        } => PhysicalPlan::TopN {
            input: Box::new(apply_predicate_inference(*input)),
            sort_exprs,
            limit,
        },

        PhysicalPlan::Limit {
            input,
            limit,
            offset,
        } => PhysicalPlan::Limit {
            input: Box::new(apply_predicate_inference(*input)),
            limit,
            offset,
        },

        PhysicalPlan::Distinct { input } => PhysicalPlan::Distinct {
            input: Box::new(apply_predicate_inference(*input)),
        },

        PhysicalPlan::Union {
            inputs,
            all,
            schema,
            parallel,
            hints,
        } => PhysicalPlan::Union {
            inputs: inputs.into_iter().map(apply_predicate_inference).collect(),
            all,
            schema,
            parallel,
            hints,
        },

        PhysicalPlan::Intersect {
            left,
            right,
            all,
            schema,
            parallel,
            hints,
        } => PhysicalPlan::Intersect {
            left: Box::new(apply_predicate_inference(*left)),
            right: Box::new(apply_predicate_inference(*right)),
            all,
            schema,
            parallel,
            hints,
        },

        PhysicalPlan::Except {
            left,
            right,
            all,
            schema,
            parallel,
            hints,
        } => PhysicalPlan::Except {
            left: Box::new(apply_predicate_inference(*left)),
            right: Box::new(apply_predicate_inference(*right)),
            all,
            schema,
            parallel,
            hints,
        },

        PhysicalPlan::Window {
            input,
            window_exprs,
            schema,
            hints,
        } => PhysicalPlan::Window {
            input: Box::new(apply_predicate_inference(*input)),
            window_exprs,
            schema,
            hints,
        },

        PhysicalPlan::WithCte {
            ctes,
            body,
            parallel_ctes,
            hints,
        } => PhysicalPlan::WithCte {
            ctes,
            body: Box::new(apply_predicate_inference(*body)),
            parallel_ctes,
            hints,
        },

        PhysicalPlan::Unnest {
            input,
            columns,
            schema,
        } => PhysicalPlan::Unnest {
            input: Box::new(apply_predicate_inference(*input)),
            columns,
            schema,
        },

        PhysicalPlan::Qualify { input, predicate } => {
            let optimized_input = apply_predicate_inference(*input);
            let optimized_predicate = apply_inference_to_predicate(predicate);

            PhysicalPlan::Qualify {
                input: Box::new(optimized_input),
                predicate: optimized_predicate,
            }
        }

        PhysicalPlan::Sample {
            input,
            sample_type,
            sample_value,
        } => PhysicalPlan::Sample {
            input: Box::new(apply_predicate_inference(*input)),
            sample_type,
            sample_value,
        },

        PhysicalPlan::Insert {
            table_name,
            columns,
            source,
        } => PhysicalPlan::Insert {
            table_name,
            columns,
            source: Box::new(apply_predicate_inference(*source)),
        },

        PhysicalPlan::CreateTable {
            table_name,
            columns,
            if_not_exists,
            or_replace,
            query,
        } => PhysicalPlan::CreateTable {
            table_name,
            columns,
            if_not_exists,
            or_replace,
            query: query.map(|q| Box::new(apply_predicate_inference(*q))),
        },

        PhysicalPlan::CreateView {
            name,
            query,
            query_sql,
            column_aliases,
            or_replace,
            if_not_exists,
        } => PhysicalPlan::CreateView {
            name,
            query: Box::new(apply_predicate_inference(*query)),
            query_sql,
            column_aliases,
            or_replace,
            if_not_exists,
        },

        PhysicalPlan::Merge {
            target_table,
            source,
            on,
            clauses,
        } => PhysicalPlan::Merge {
            target_table,
            source: Box::new(apply_predicate_inference(*source)),
            on,
            clauses,
        },

        PhysicalPlan::Update {
            table_name,
            alias,
            assignments,
            from,
            filter,
        } => PhysicalPlan::Update {
            table_name,
            alias,
            assignments,
            from: from.map(|f| Box::new(apply_predicate_inference(*f))),
            filter,
        },

        PhysicalPlan::ExportData { options, query } => PhysicalPlan::ExportData {
            options,
            query: Box::new(apply_predicate_inference(*query)),
        },

        PhysicalPlan::For {
            variable,
            query,
            body,
        } => PhysicalPlan::For {
            variable,
            query: Box::new(apply_predicate_inference(*query)),
            body: body.into_iter().map(apply_predicate_inference).collect(),
        },

        PhysicalPlan::If {
            condition,
            then_branch,
            else_branch,
        } => PhysicalPlan::If {
            condition,
            then_branch: then_branch
                .into_iter()
                .map(apply_predicate_inference)
                .collect(),
            else_branch: else_branch
                .map(|b| b.into_iter().map(apply_predicate_inference).collect()),
        },

        PhysicalPlan::While {
            condition,
            body,
            label,
        } => PhysicalPlan::While {
            condition,
            body: body.into_iter().map(apply_predicate_inference).collect(),
            label,
        },

        PhysicalPlan::Loop { body, label } => PhysicalPlan::Loop {
            body: body.into_iter().map(apply_predicate_inference).collect(),
            label,
        },

        PhysicalPlan::Block { body, label } => PhysicalPlan::Block {
            body: body.into_iter().map(apply_predicate_inference).collect(),
            label,
        },

        PhysicalPlan::Repeat {
            body,
            until_condition,
        } => PhysicalPlan::Repeat {
            body: body.into_iter().map(apply_predicate_inference).collect(),
            until_condition,
        },

        PhysicalPlan::CreateProcedure {
            name,
            args,
            body,
            or_replace,
            if_not_exists,
        } => PhysicalPlan::CreateProcedure {
            name,
            args,
            body: body.into_iter().map(apply_predicate_inference).collect(),
            or_replace,
            if_not_exists,
        },

        PhysicalPlan::TryCatch {
            try_block,
            catch_block,
        } => PhysicalPlan::TryCatch {
            try_block: try_block
                .into_iter()
                .map(|(p, sql)| (apply_predicate_inference(p), sql))
                .collect(),
            catch_block: catch_block
                .into_iter()
                .map(apply_predicate_inference)
                .collect(),
        },

        PhysicalPlan::GapFill {
            input,
            ts_column,
            bucket_width,
            value_columns,
            partitioning_columns,
            origin,
            input_schema,
            schema,
        } => PhysicalPlan::GapFill {
            input: Box::new(apply_predicate_inference(*input)),
            ts_column,
            bucket_width,
            value_columns,
            partitioning_columns,
            origin,
            input_schema,
            schema,
        },

        PhysicalPlan::Explain {
            input,
            analyze,
            logical_plan_text,
            physical_plan_text,
        } => PhysicalPlan::Explain {
            input: Box::new(apply_predicate_inference(*input)),
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
    use yachtsql_ir::{Literal, PlanField, PlanSchema};

    use super::*;

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

    fn make_column(name: &str, index: usize) -> Expr {
        Expr::Column {
            table: None,
            name: name.to_string(),
            index: Some(index),
        }
    }

    fn make_eq(left: Expr, right: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOp::Eq,
            right: Box::new(right),
        }
    }

    fn make_gt(left: Expr, right: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOp::Gt,
            right: Box::new(right),
        }
    }

    fn make_lt(left: Expr, right: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOp::Lt,
            right: Box::new(right),
        }
    }

    fn make_and(left: Expr, right: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOp::And,
            right: Box::new(right),
        }
    }

    fn make_literal(value: i64) -> Expr {
        Expr::Literal(Literal::Int64(value))
    }

    fn count_predicates(expr: &Expr) -> usize {
        split_and_predicates(expr).len()
    }

    #[test]
    fn infers_constant_from_equality_chain() {
        let scan = make_scan("t", 3);
        let col_a = make_column("a", 0);
        let col_b = make_column("b", 1);

        let a_eq_b = make_eq(col_a.clone(), col_b.clone());
        let b_eq_5 = make_eq(col_b.clone(), make_literal(5));
        let predicate = make_and(a_eq_b, b_eq_5);

        let plan = PhysicalPlan::Filter {
            input: Box::new(scan),
            predicate,
        };

        let result = apply_predicate_inference(plan);

        match result {
            PhysicalPlan::Filter { predicate, .. } => {
                let preds = split_and_predicates(&predicate);
                assert!(preds.len() >= 3);

                let has_a_eq_5 = preds.iter().any(|p| {
                    matches!(p, Expr::BinaryOp { left, op: BinaryOp::Eq, right }
                        if exprs_equal(left, &col_a) && matches!(right.as_ref(), Expr::Literal(Literal::Int64(5))))
                });
                assert!(has_a_eq_5);
            }
            _ => panic!("Expected Filter plan"),
        }
    }

    #[test]
    fn infers_range_predicate_transitivity() {
        let scan = make_scan("t", 3);
        let col_a = make_column("a", 0);
        let col_b = make_column("b", 1);

        let a_eq_b = make_eq(col_a.clone(), col_b.clone());
        let a_gt_10 = make_gt(col_a.clone(), make_literal(10));
        let predicate = make_and(a_eq_b, a_gt_10);

        let plan = PhysicalPlan::Filter {
            input: Box::new(scan),
            predicate,
        };

        let result = apply_predicate_inference(plan);

        match result {
            PhysicalPlan::Filter { predicate, .. } => {
                let preds = split_and_predicates(&predicate);
                assert!(preds.len() >= 3);

                let has_b_gt_10 = preds.iter().any(|p| {
                    matches!(p, Expr::BinaryOp { left, op: BinaryOp::Gt, right }
                        if exprs_equal(left, &col_b) && matches!(right.as_ref(), Expr::Literal(Literal::Int64(10))))
                });
                assert!(has_b_gt_10);
            }
            _ => panic!("Expected Filter plan"),
        }
    }

    #[test]
    fn no_inference_without_equality() {
        let scan = make_scan("t", 3);
        let col_a = make_column("a", 0);

        let a_gt_10 = make_gt(col_a.clone(), make_literal(10));

        let plan = PhysicalPlan::Filter {
            input: Box::new(scan),
            predicate: a_gt_10,
        };

        let result = apply_predicate_inference(plan);

        match result {
            PhysicalPlan::Filter { predicate, .. } => {
                assert_eq!(count_predicates(&predicate), 1);
            }
            _ => panic!("Expected Filter plan"),
        }
    }

    #[test]
    fn handles_multiple_equivalence_classes() {
        let scan = make_scan("t", 4);
        let col_a = make_column("a", 0);
        let col_b = make_column("b", 1);
        let col_c = make_column("c", 2);
        let col_d = make_column("d", 3);

        let a_eq_b = make_eq(col_a.clone(), col_b.clone());
        let c_eq_d = make_eq(col_c.clone(), col_d.clone());
        let a_eq_1 = make_eq(col_a.clone(), make_literal(1));
        let c_eq_2 = make_eq(col_c.clone(), make_literal(2));

        let pred1 = make_and(a_eq_b, c_eq_d);
        let pred2 = make_and(a_eq_1, c_eq_2);
        let predicate = make_and(pred1, pred2);

        let plan = PhysicalPlan::Filter {
            input: Box::new(scan),
            predicate,
        };

        let result = apply_predicate_inference(plan);

        match result {
            PhysicalPlan::Filter { predicate, .. } => {
                let preds = split_and_predicates(&predicate);

                let has_b_eq_1 = preds.iter().any(|p| {
                    matches!(p, Expr::BinaryOp { left, op: BinaryOp::Eq, right }
                        if exprs_equal(left, &col_b) && matches!(right.as_ref(), Expr::Literal(Literal::Int64(1))))
                });
                let has_d_eq_2 = preds.iter().any(|p| {
                    matches!(p, Expr::BinaryOp { left, op: BinaryOp::Eq, right }
                        if exprs_equal(left, &col_d) && matches!(right.as_ref(), Expr::Literal(Literal::Int64(2))))
                });
                assert!(has_b_eq_1);
                assert!(has_d_eq_2);
            }
            _ => panic!("Expected Filter plan"),
        }
    }

    #[test]
    fn handles_transitive_equality() {
        let scan = make_scan("t", 3);
        let col_a = make_column("a", 0);
        let col_b = make_column("b", 1);
        let col_c = make_column("c", 2);

        let a_eq_b = make_eq(col_a.clone(), col_b.clone());
        let b_eq_c = make_eq(col_b.clone(), col_c.clone());
        let a_eq_5 = make_eq(col_a.clone(), make_literal(5));

        let pred1 = make_and(a_eq_b, b_eq_c);
        let predicate = make_and(pred1, a_eq_5);

        let plan = PhysicalPlan::Filter {
            input: Box::new(scan),
            predicate,
        };

        let result = apply_predicate_inference(plan);

        match result {
            PhysicalPlan::Filter { predicate, .. } => {
                let preds = split_and_predicates(&predicate);

                let has_b_eq_5 = preds.iter().any(|p| {
                    matches!(p, Expr::BinaryOp { left, op: BinaryOp::Eq, right }
                        if exprs_equal(left, &col_b) && matches!(right.as_ref(), Expr::Literal(Literal::Int64(5))))
                });
                let has_c_eq_5 = preds.iter().any(|p| {
                    matches!(p, Expr::BinaryOp { left, op: BinaryOp::Eq, right }
                        if exprs_equal(left, &col_c) && matches!(right.as_ref(), Expr::Literal(Literal::Int64(5))))
                });
                assert!(has_b_eq_5);
                assert!(has_c_eq_5);
            }
            _ => panic!("Expected Filter plan"),
        }
    }

    #[test]
    fn avoids_duplicate_predicates() {
        let scan = make_scan("t", 2);
        let col_a = make_column("a", 0);
        let col_b = make_column("b", 1);

        let a_eq_b = make_eq(col_a.clone(), col_b.clone());
        let a_eq_5 = make_eq(col_a.clone(), make_literal(5));
        let b_eq_5 = make_eq(col_b.clone(), make_literal(5));

        let pred1 = make_and(a_eq_b, a_eq_5);
        let predicate = make_and(pred1, b_eq_5);

        let plan = PhysicalPlan::Filter {
            input: Box::new(scan),
            predicate,
        };

        let result = apply_predicate_inference(plan);

        match result {
            PhysicalPlan::Filter { predicate, .. } => {
                let preds = split_and_predicates(&predicate);
                assert_eq!(preds.len(), 3);
            }
            _ => panic!("Expected Filter plan"),
        }
    }

    #[test]
    fn propagates_lt_predicate() {
        let scan = make_scan("t", 2);
        let col_a = make_column("a", 0);
        let col_b = make_column("b", 1);

        let a_eq_b = make_eq(col_a.clone(), col_b.clone());
        let a_lt_100 = make_lt(col_a.clone(), make_literal(100));
        let predicate = make_and(a_eq_b, a_lt_100);

        let plan = PhysicalPlan::Filter {
            input: Box::new(scan),
            predicate,
        };

        let result = apply_predicate_inference(plan);

        match result {
            PhysicalPlan::Filter { predicate, .. } => {
                let preds = split_and_predicates(&predicate);
                assert!(preds.len() >= 3);

                let has_b_lt_100 = preds.iter().any(|p| {
                    matches!(p, Expr::BinaryOp { left, op: BinaryOp::Lt, right }
                        if exprs_equal(left, &col_b) && matches!(right.as_ref(), Expr::Literal(Literal::Int64(100))))
                });
                assert!(has_b_lt_100);
            }
            _ => panic!("Expected Filter plan"),
        }
    }

    #[test]
    fn propagates_between_predicate() {
        let scan = make_scan("t", 2);
        let col_a = make_column("a", 0);
        let col_b = make_column("b", 1);

        let a_eq_b = make_eq(col_a.clone(), col_b.clone());
        let a_between = Expr::Between {
            expr: Box::new(col_a.clone()),
            low: Box::new(make_literal(1)),
            high: Box::new(make_literal(10)),
            negated: false,
        };
        let predicate = make_and(a_eq_b, a_between);

        let plan = PhysicalPlan::Filter {
            input: Box::new(scan),
            predicate,
        };

        let result = apply_predicate_inference(plan);

        match result {
            PhysicalPlan::Filter { predicate, .. } => {
                let preds = split_and_predicates(&predicate);
                assert!(preds.len() >= 3);

                let has_b_between = preds.iter().any(|p| {
                    matches!(p, Expr::Between { expr, low, high, negated: false }
                        if exprs_equal(expr, &col_b)
                        && matches!(low.as_ref(), Expr::Literal(Literal::Int64(1)))
                        && matches!(high.as_ref(), Expr::Literal(Literal::Int64(10))))
                });
                assert!(has_b_between);
            }
            _ => panic!("Expected Filter plan"),
        }
    }

    #[test]
    fn propagates_in_list_predicate() {
        let scan = make_scan("t", 2);
        let col_a = make_column("a", 0);
        let col_b = make_column("b", 1);

        let a_eq_b = make_eq(col_a.clone(), col_b.clone());
        let a_in_list = Expr::InList {
            expr: Box::new(col_a.clone()),
            list: vec![make_literal(1), make_literal(2), make_literal(3)],
            negated: false,
        };
        let predicate = make_and(a_eq_b, a_in_list);

        let plan = PhysicalPlan::Filter {
            input: Box::new(scan),
            predicate,
        };

        let result = apply_predicate_inference(plan);

        match result {
            PhysicalPlan::Filter { predicate, .. } => {
                let preds = split_and_predicates(&predicate);
                assert!(preds.len() >= 3);

                let has_b_in_list = preds.iter().any(|p| {
                    matches!(p, Expr::InList { expr, list, negated: false }
                        if exprs_equal(expr, &col_b) && list.len() == 3)
                });
                assert!(has_b_in_list);
            }
            _ => panic!("Expected Filter plan"),
        }
    }

    #[test]
    fn recurses_through_project() {
        let scan = make_scan("t", 2);
        let col_a = make_column("a", 0);
        let col_b = make_column("b", 1);

        let a_eq_b = make_eq(col_a.clone(), col_b.clone());
        let b_eq_5 = make_eq(col_b.clone(), make_literal(5));
        let predicate = make_and(a_eq_b, b_eq_5);

        let filter = PhysicalPlan::Filter {
            input: Box::new(scan),
            predicate,
        };

        let project = PhysicalPlan::Project {
            input: Box::new(filter),
            expressions: vec![col_a.clone()],
            schema: make_schema(1),
        };

        let result = apply_predicate_inference(project);

        match result {
            PhysicalPlan::Project { input, .. } => match *input {
                PhysicalPlan::Filter { predicate, .. } => {
                    let preds = split_and_predicates(&predicate);
                    assert!(preds.len() >= 3);
                }
                _ => panic!("Expected Filter inside Project"),
            },
            _ => panic!("Expected Project plan"),
        }
    }

    #[test]
    fn handles_empty_predicate_gracefully() {
        let scan = make_scan("t", 2);

        let plan = apply_predicate_inference(scan.clone());

        match plan {
            PhysicalPlan::TableScan { table_name, .. } => {
                assert_eq!(table_name, "t");
            }
            _ => panic!("Expected TableScan plan"),
        }
    }

    #[test]
    fn test_exprs_equal() {
        let col1 = make_column("a", 0);
        let col2 = make_column("a", 0);
        let col3 = make_column("b", 1);

        assert!(exprs_equal(&col1, &col2));
        assert!(!exprs_equal(&col1, &col3));

        let lit1 = make_literal(5);
        let lit2 = make_literal(5);
        let lit3 = make_literal(10);

        assert!(exprs_equal(&lit1, &lit2));
        assert!(!exprs_equal(&lit1, &lit3));

        let eq1 = make_eq(col1.clone(), lit1.clone());
        let eq2 = make_eq(col2.clone(), lit2.clone());
        let eq3 = make_eq(col3.clone(), lit1.clone());

        assert!(exprs_equal(&eq1, &eq2));
        assert!(!exprs_equal(&eq1, &eq3));
    }

    #[test]
    fn test_build_equivalence_classes_single_equality() {
        let col_a = make_column("a", 0);
        let col_b = make_column("b", 1);
        let a_eq_b = make_eq(col_a.clone(), col_b.clone());

        let predicates = vec![a_eq_b];
        let classes = build_equivalence_classes(&predicates);

        assert_eq!(classes.len(), 1);
        assert!(classes[0].contains(&col_a));
        assert!(classes[0].contains(&col_b));
        assert!(classes[0].constant.is_none());
    }

    #[test]
    fn test_build_equivalence_classes_with_constant() {
        let col_a = make_column("a", 0);
        let lit = make_literal(5);
        let a_eq_5 = make_eq(col_a.clone(), lit.clone());

        let predicates = vec![a_eq_5];
        let classes = build_equivalence_classes(&predicates);

        assert_eq!(classes.len(), 1);
        assert!(classes[0].contains(&col_a));
        assert!(classes[0].constant.is_some());
    }

    #[test]
    fn test_equivalence_class_merge() {
        let col_a = make_column("a", 0);
        let col_b = make_column("b", 1);
        let col_c = make_column("c", 2);

        let a_eq_b = make_eq(col_a.clone(), col_b.clone());
        let b_eq_c = make_eq(col_b.clone(), col_c.clone());

        let predicates = vec![a_eq_b, b_eq_c];
        let classes = build_equivalence_classes(&predicates);

        assert_eq!(classes.len(), 1);
        assert!(classes[0].contains(&col_a));
        assert!(classes[0].contains(&col_b));
        assert!(classes[0].contains(&col_c));
    }

    #[test]
    fn handles_literal_eq_literal() {
        let lit1 = make_literal(5);
        let lit2 = make_literal(5);
        let lit_eq_lit = make_eq(lit1, lit2);

        let predicates = vec![lit_eq_lit];
        let classes = build_equivalence_classes(&predicates);

        assert!(classes.is_empty());
    }

    #[test]
    fn handles_constant_on_left_side() {
        let col_a = make_column("a", 0);
        let lit = make_literal(5);
        let five_eq_a = make_eq(lit.clone(), col_a.clone());

        let predicates = vec![five_eq_a];
        let classes = build_equivalence_classes(&predicates);

        assert_eq!(classes.len(), 1);
        assert!(classes[0].contains(&col_a));
        assert!(classes[0].constant.is_some());
    }

    #[test]
    fn inference_with_qualify() {
        let scan = make_scan("t", 2);
        let col_a = make_column("a", 0);
        let col_b = make_column("b", 1);

        let a_eq_b = make_eq(col_a.clone(), col_b.clone());
        let b_eq_5 = make_eq(col_b.clone(), make_literal(5));
        let predicate = make_and(a_eq_b, b_eq_5);

        let plan = PhysicalPlan::Qualify {
            input: Box::new(scan),
            predicate,
        };

        let result = apply_predicate_inference(plan);

        match result {
            PhysicalPlan::Qualify { predicate, .. } => {
                let preds = split_and_predicates(&predicate);
                assert!(preds.len() >= 3);
            }
            _ => panic!("Expected Qualify plan"),
        }
    }
}
