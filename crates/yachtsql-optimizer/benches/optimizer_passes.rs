use criterion::{Criterion, black_box, criterion_group, criterion_main};
use yachtsql_common::types::DataType;
use yachtsql_ir::{
    AggregateFunction, BinaryOp, Expr, JoinType, Literal, PlanField, PlanSchema, SortExpr,
};
use yachtsql_optimizer::{
    ExecutionHints, PhysicalPlan, ProjectionPushdown, apply_cross_to_hash_join,
    apply_distinct_elimination, apply_empty_propagation, apply_filter_merging,
    apply_filter_pushdown_aggregate, apply_limit_pushdown, apply_outer_to_inner_join,
    apply_predicate_inference, apply_predicate_simplification, apply_project_merging,
    apply_short_circuit_ordering, apply_sort_elimination, apply_topn_pushdown,
    apply_trivial_predicate_removal, fold_constants,
};

fn make_schema(table_name: &str, num_columns: usize) -> PlanSchema {
    let fields = (0..num_columns)
        .map(|i| PlanField::new(format!("col{}", i), DataType::Int64).with_table(table_name))
        .collect();
    PlanSchema::from_fields(fields)
}

fn make_scan(table_name: &str, num_columns: usize) -> PhysicalPlan {
    PhysicalPlan::TableScan {
        table_name: table_name.to_string(),
        schema: make_schema(table_name, num_columns),
        projection: None,
        row_count: None,
    }
}

fn col(table: &str, name: &str, index: usize) -> Expr {
    Expr::Column {
        table: Some(table.to_string()),
        name: name.to_string(),
        index: Some(index),
    }
}

fn col_no_table(name: &str, index: usize) -> Expr {
    Expr::Column {
        table: None,
        name: name.to_string(),
        index: Some(index),
    }
}

fn lit_i64(v: i64) -> Expr {
    Expr::Literal(Literal::Int64(v))
}

fn lit_str(s: &str) -> Expr {
    Expr::Literal(Literal::String(s.to_string()))
}

fn lit_bool(b: bool) -> Expr {
    Expr::Literal(Literal::Bool(b))
}

fn binary(left: Expr, op: BinaryOp, right: Expr) -> Expr {
    Expr::BinaryOp {
        left: Box::new(left),
        op,
        right: Box::new(right),
    }
}

fn eq(left: Expr, right: Expr) -> Expr {
    binary(left, BinaryOp::Eq, right)
}

fn gt(left: Expr, right: Expr) -> Expr {
    binary(left, BinaryOp::Gt, right)
}

fn lt(left: Expr, right: Expr) -> Expr {
    binary(left, BinaryOp::Lt, right)
}

fn and(left: Expr, right: Expr) -> Expr {
    binary(left, BinaryOp::And, right)
}

fn or(left: Expr, right: Expr) -> Expr {
    binary(left, BinaryOp::Or, right)
}

fn make_empty(num_columns: usize) -> PhysicalPlan {
    let fields = (0..num_columns)
        .map(|i| PlanField::new(format!("col{}", i), DataType::Int64))
        .collect();
    PhysicalPlan::Empty {
        schema: PlanSchema::from_fields(fields),
    }
}

fn bench_trivial_predicate_removal(c: &mut Criterion) {
    let mut group = c.benchmark_group("pass/trivial_predicate");

    let filter_true = PhysicalPlan::Filter {
        input: Box::new(make_scan("t", 10)),
        predicate: lit_bool(true),
    };

    let filter_false = PhysicalPlan::Filter {
        input: Box::new(make_scan("t", 10)),
        predicate: lit_bool(false),
    };

    let filter_1_eq_1 = PhysicalPlan::Filter {
        input: Box::new(make_scan("t", 10)),
        predicate: and(
            eq(lit_i64(1), lit_i64(1)),
            gt(col("t", "col0", 0), lit_i64(100)),
        ),
    };

    let nested_trivial = {
        let inner = PhysicalPlan::Filter {
            input: Box::new(make_scan("t", 10)),
            predicate: lit_bool(true),
        };
        let middle = PhysicalPlan::Filter {
            input: Box::new(inner),
            predicate: and(lit_bool(true), gt(col("t", "col0", 0), lit_i64(50))),
        };
        PhysicalPlan::Filter {
            input: Box::new(middle),
            predicate: lit_bool(true),
        }
    };

    group.bench_function("filter_true", |b| {
        b.iter(|| apply_trivial_predicate_removal(black_box(filter_true.clone())))
    });

    group.bench_function("filter_false", |b| {
        b.iter(|| apply_trivial_predicate_removal(black_box(filter_false.clone())))
    });

    group.bench_function("filter_1_eq_1", |b| {
        b.iter(|| apply_trivial_predicate_removal(black_box(filter_1_eq_1.clone())))
    });

    group.bench_function("nested_trivial", |b| {
        b.iter(|| apply_trivial_predicate_removal(black_box(nested_trivial.clone())))
    });

    group.finish();
}

fn bench_empty_propagation(c: &mut Criterion) {
    let mut group = c.benchmark_group("pass/empty_propagation");

    let empty_through_filter = PhysicalPlan::Filter {
        input: Box::new(make_empty(5)),
        predicate: gt(col_no_table("col0", 0), lit_i64(100)),
    };

    let schema = make_schema("", 10);
    let empty_through_join = PhysicalPlan::HashJoin {
        left: Box::new(make_empty(5)),
        right: Box::new(make_scan("t", 5)),
        join_type: JoinType::Inner,
        left_keys: vec![col_no_table("col0", 0)],
        right_keys: vec![col("t", "col0", 5)],
        schema,
        parallel: false,
        hints: ExecutionHints::default(),
    };

    let union_with_empty = PhysicalPlan::Union {
        inputs: vec![make_scan("t1", 5), make_empty(5), make_scan("t2", 5)],
        all: true,
        schema: make_schema("", 5),
        parallel: false,
        hints: ExecutionHints::default(),
    };

    let limit_zero = PhysicalPlan::Limit {
        input: Box::new(make_scan("t", 10)),
        limit: Some(0),
        offset: None,
    };

    group.bench_function("empty_filter", |b| {
        b.iter(|| apply_empty_propagation(black_box(empty_through_filter.clone())))
    });

    group.bench_function("empty_join", |b| {
        b.iter(|| apply_empty_propagation(black_box(empty_through_join.clone())))
    });

    group.bench_function("union_with_empty", |b| {
        b.iter(|| apply_empty_propagation(black_box(union_with_empty.clone())))
    });

    group.bench_function("limit_zero", |b| {
        b.iter(|| apply_empty_propagation(black_box(limit_zero.clone())))
    });

    group.finish();
}

fn bench_filter_merging(c: &mut Criterion) {
    let mut group = c.benchmark_group("pass/filter_merging");

    let two_filters = PhysicalPlan::Filter {
        input: Box::new(PhysicalPlan::Filter {
            input: Box::new(make_scan("t", 10)),
            predicate: gt(col("t", "col0", 0), lit_i64(10)),
        }),
        predicate: lt(col("t", "col1", 1), lit_i64(100)),
    };

    let five_filters = {
        let mut plan = make_scan("t", 10);
        for i in 0..5 {
            plan = PhysicalPlan::Filter {
                input: Box::new(plan),
                predicate: gt(col("t", &format!("col{}", i), i), lit_i64((i * 10) as i64)),
            };
        }
        plan
    };

    group.bench_function("two_filters", |b| {
        b.iter(|| apply_filter_merging(black_box(two_filters.clone())))
    });

    group.bench_function("five_filters", |b| {
        b.iter(|| apply_filter_merging(black_box(five_filters.clone())))
    });

    group.finish();
}

fn bench_predicate_simplification(c: &mut Criterion) {
    let mut group = c.benchmark_group("pass/predicate_simplification");

    let redundant_and = PhysicalPlan::Filter {
        input: Box::new(make_scan("t", 10)),
        predicate: and(
            gt(col("t", "col0", 0), lit_i64(10)),
            gt(col("t", "col0", 0), lit_i64(5)),
        ),
    };

    let redundant_or = PhysicalPlan::Filter {
        input: Box::new(make_scan("t", 10)),
        predicate: or(
            lt(col("t", "col0", 0), lit_i64(10)),
            lt(col("t", "col0", 0), lit_i64(20)),
        ),
    };

    let complex_predicate = PhysicalPlan::Filter {
        input: Box::new(make_scan("t", 10)),
        predicate: and(
            and(
                gt(col("t", "col0", 0), lit_i64(10)),
                lt(col("t", "col0", 0), lit_i64(100)),
            ),
            and(
                gt(col("t", "col0", 0), lit_i64(5)),
                lt(col("t", "col0", 0), lit_i64(200)),
            ),
        ),
    };

    group.bench_function("redundant_and", |b| {
        b.iter(|| apply_predicate_simplification(black_box(redundant_and.clone())))
    });

    group.bench_function("redundant_or", |b| {
        b.iter(|| apply_predicate_simplification(black_box(redundant_or.clone())))
    });

    group.bench_function("complex", |b| {
        b.iter(|| apply_predicate_simplification(black_box(complex_predicate.clone())))
    });

    group.finish();
}

fn bench_project_merging(c: &mut Criterion) {
    let mut group = c.benchmark_group("pass/project_merging");

    let scan = make_scan("t", 10);
    let proj1 = PhysicalPlan::Project {
        input: Box::new(scan),
        expressions: (0..5).map(|i| col("t", &format!("col{}", i), i)).collect(),
        schema: make_schema("t", 5),
    };
    let two_projects = PhysicalPlan::Project {
        input: Box::new(proj1),
        expressions: (0..3).map(|i| col("t", &format!("col{}", i), i)).collect(),
        schema: make_schema("t", 3),
    };

    let scan2 = make_scan("t", 10);
    let mut plan = scan2;
    for i in (1..=5).rev() {
        plan = PhysicalPlan::Project {
            input: Box::new(plan),
            expressions: (0..i).map(|j| col("t", &format!("col{}", j), j)).collect(),
            schema: make_schema("t", i),
        };
    }
    let five_projects = plan;

    group.bench_function("two_projects", |b| {
        b.iter(|| apply_project_merging(black_box(two_projects.clone())))
    });

    group.bench_function("five_projects", |b| {
        b.iter(|| apply_project_merging(black_box(five_projects.clone())))
    });

    group.finish();
}

fn bench_distinct_elimination(c: &mut Criterion) {
    let mut group = c.benchmark_group("pass/distinct_elimination");

    let distinct_on_limit_1 = PhysicalPlan::Distinct {
        input: Box::new(PhysicalPlan::Limit {
            input: Box::new(make_scan("t", 5)),
            limit: Some(1),
            offset: None,
        }),
    };

    let distinct_on_empty = PhysicalPlan::Distinct {
        input: Box::new(make_empty(5)),
    };

    let distinct_on_distinct = PhysicalPlan::Distinct {
        input: Box::new(PhysicalPlan::Distinct {
            input: Box::new(make_scan("t", 5)),
        }),
    };

    group.bench_function("distinct_on_limit_1", |b| {
        b.iter(|| apply_distinct_elimination(black_box(distinct_on_limit_1.clone())))
    });

    group.bench_function("distinct_on_empty", |b| {
        b.iter(|| apply_distinct_elimination(black_box(distinct_on_empty.clone())))
    });

    group.bench_function("distinct_on_distinct", |b| {
        b.iter(|| apply_distinct_elimination(black_box(distinct_on_distinct.clone())))
    });

    group.finish();
}

fn bench_cross_to_hash_join(c: &mut Criterion) {
    let mut group = c.benchmark_group("pass/cross_to_hash_join");

    let schema = make_schema("", 10);
    let cross_with_equi_filter = PhysicalPlan::Filter {
        input: Box::new(PhysicalPlan::CrossJoin {
            left: Box::new(make_scan("t1", 5)),
            right: Box::new(make_scan("t2", 5)),
            schema: schema.clone(),
            parallel: false,
            hints: ExecutionHints::default(),
        }),
        predicate: eq(col("t1", "col0", 0), col("t2", "col0", 5)),
    };

    let cross_with_complex_filter = PhysicalPlan::Filter {
        input: Box::new(PhysicalPlan::CrossJoin {
            left: Box::new(make_scan("t1", 5)),
            right: Box::new(make_scan("t2", 5)),
            schema: schema.clone(),
            parallel: false,
            hints: ExecutionHints::default(),
        }),
        predicate: and(
            eq(col("t1", "col0", 0), col("t2", "col0", 5)),
            and(
                gt(col("t1", "col1", 1), lit_i64(100)),
                lt(col("t2", "col2", 7), lit_i64(50)),
            ),
        ),
    };

    group.bench_function("simple_equi", |b| {
        b.iter(|| apply_cross_to_hash_join(black_box(cross_with_equi_filter.clone())))
    });

    group.bench_function("complex_filter", |b| {
        b.iter(|| apply_cross_to_hash_join(black_box(cross_with_complex_filter.clone())))
    });

    group.finish();
}

fn bench_outer_to_inner_join(c: &mut Criterion) {
    let mut group = c.benchmark_group("pass/outer_to_inner_join");

    let schema = make_schema("", 10);

    let left_join_with_is_not_null = PhysicalPlan::Filter {
        input: Box::new(PhysicalPlan::HashJoin {
            left: Box::new(make_scan("t1", 5)),
            right: Box::new(make_scan("t2", 5)),
            join_type: JoinType::Left,
            left_keys: vec![col("t1", "col0", 0)],
            right_keys: vec![col("t2", "col0", 5)],
            schema: schema.clone(),
            parallel: false,
            hints: ExecutionHints::default(),
        }),
        predicate: Expr::IsNull {
            expr: Box::new(col("t2", "col1", 6)),
            negated: true,
        },
    };

    let right_join_with_filter = PhysicalPlan::Filter {
        input: Box::new(PhysicalPlan::HashJoin {
            left: Box::new(make_scan("t1", 5)),
            right: Box::new(make_scan("t2", 5)),
            join_type: JoinType::Right,
            left_keys: vec![col("t1", "col0", 0)],
            right_keys: vec![col("t2", "col0", 5)],
            schema: schema.clone(),
            parallel: false,
            hints: ExecutionHints::default(),
        }),
        predicate: gt(col("t1", "col1", 1), lit_i64(100)),
    };

    group.bench_function("left_to_inner", |b| {
        b.iter(|| apply_outer_to_inner_join(black_box(left_join_with_is_not_null.clone())))
    });

    group.bench_function("right_to_inner", |b| {
        b.iter(|| apply_outer_to_inner_join(black_box(right_join_with_filter.clone())))
    });

    group.finish();
}

fn bench_filter_pushdown_aggregate(c: &mut Criterion) {
    let mut group = c.benchmark_group("pass/filter_pushdown_aggregate");

    let agg_schema = PlanSchema::from_fields(vec![
        PlanField::new("col0", DataType::Int64).with_table("t"),
        PlanField::new("sum_col1", DataType::Int64),
    ]);

    let filter_on_group_by = PhysicalPlan::Filter {
        input: Box::new(PhysicalPlan::HashAggregate {
            input: Box::new(make_scan("t", 10)),
            group_by: vec![col("t", "col0", 0)],
            aggregates: vec![Expr::Aggregate {
                func: AggregateFunction::Sum,
                args: vec![col("t", "col1", 1)],
                distinct: false,
                filter: None,
                order_by: vec![],
                limit: None,
                ignore_nulls: false,
            }],
            schema: agg_schema.clone(),
            grouping_sets: None,
            hints: ExecutionHints::default(),
        }),
        predicate: eq(col("t", "col0", 0), lit_i64(42)),
    };

    let filter_on_aggregate_result = PhysicalPlan::Filter {
        input: Box::new(PhysicalPlan::HashAggregate {
            input: Box::new(make_scan("t", 10)),
            group_by: vec![col("t", "col0", 0)],
            aggregates: vec![Expr::Aggregate {
                func: AggregateFunction::Sum,
                args: vec![col("t", "col1", 1)],
                distinct: false,
                filter: None,
                order_by: vec![],
                limit: None,
                ignore_nulls: false,
            }],
            schema: agg_schema.clone(),
            grouping_sets: None,
            hints: ExecutionHints::default(),
        }),
        predicate: gt(col_no_table("sum_col1", 1), lit_i64(1000)),
    };

    group.bench_function("filter_on_group_by", |b| {
        b.iter(|| apply_filter_pushdown_aggregate(black_box(filter_on_group_by.clone())))
    });

    group.bench_function("filter_on_aggregate", |b| {
        b.iter(|| apply_filter_pushdown_aggregate(black_box(filter_on_aggregate_result.clone())))
    });

    group.finish();
}

fn bench_sort_elimination(c: &mut Criterion) {
    let mut group = c.benchmark_group("pass/sort_elimination");

    let sort_expr = SortExpr {
        expr: col("t", "col0", 0),
        asc: true,
        nulls_first: false,
    };

    let redundant_sorts = PhysicalPlan::Sort {
        input: Box::new(PhysicalPlan::Sort {
            input: Box::new(make_scan("t", 10)),
            sort_exprs: vec![sort_expr.clone()],
            hints: ExecutionHints::default(),
        }),
        sort_exprs: vec![sort_expr.clone()],
        hints: ExecutionHints::default(),
    };

    let different_sorts = PhysicalPlan::Sort {
        input: Box::new(PhysicalPlan::Sort {
            input: Box::new(make_scan("t", 10)),
            sort_exprs: vec![sort_expr.clone()],
            hints: ExecutionHints::default(),
        }),
        sort_exprs: vec![SortExpr {
            expr: col("t", "col1", 1),
            asc: false,
            nulls_first: true,
        }],
        hints: ExecutionHints::default(),
    };

    group.bench_function("redundant_sorts", |b| {
        b.iter(|| apply_sort_elimination(black_box(redundant_sorts.clone())))
    });

    group.bench_function("different_sorts", |b| {
        b.iter(|| apply_sort_elimination(black_box(different_sorts.clone())))
    });

    group.finish();
}

fn bench_limit_pushdown(c: &mut Criterion) {
    let mut group = c.benchmark_group("pass/limit_pushdown");

    let limit_over_union = PhysicalPlan::Limit {
        input: Box::new(PhysicalPlan::Union {
            inputs: vec![make_scan("t1", 5), make_scan("t2", 5), make_scan("t3", 5)],
            all: true,
            schema: make_schema("", 5),
            parallel: false,
            hints: ExecutionHints::default(),
        }),
        limit: Some(100),
        offset: None,
    };

    let limit_over_project = PhysicalPlan::Limit {
        input: Box::new(PhysicalPlan::Project {
            input: Box::new(make_scan("t", 10)),
            expressions: (0..5).map(|i| col("t", &format!("col{}", i), i)).collect(),
            schema: make_schema("t", 5),
        }),
        limit: Some(50),
        offset: None,
    };

    group.bench_function("limit_over_union", |b| {
        b.iter(|| apply_limit_pushdown(black_box(limit_over_union.clone())))
    });

    group.bench_function("limit_over_project", |b| {
        b.iter(|| apply_limit_pushdown(black_box(limit_over_project.clone())))
    });

    group.finish();
}

fn bench_topn_pushdown(c: &mut Criterion) {
    let mut group = c.benchmark_group("pass/topn_pushdown");

    let sort_expr = SortExpr {
        expr: col_no_table("col0", 0),
        asc: true,
        nulls_first: false,
    };

    let topn_over_union = PhysicalPlan::TopN {
        input: Box::new(PhysicalPlan::Union {
            inputs: vec![make_scan("t1", 5), make_scan("t2", 5)],
            all: true,
            schema: make_schema("", 5),
            parallel: false,
            hints: ExecutionHints::default(),
        }),
        sort_exprs: vec![sort_expr.clone()],
        limit: 10,
    };

    let topn_over_project = PhysicalPlan::TopN {
        input: Box::new(PhysicalPlan::Project {
            input: Box::new(make_scan("t", 10)),
            expressions: (0..5).map(|i| col("t", &format!("col{}", i), i)).collect(),
            schema: make_schema("t", 5),
        }),
        sort_exprs: vec![sort_expr.clone()],
        limit: 20,
    };

    group.bench_function("topn_over_union", |b| {
        b.iter(|| apply_topn_pushdown(black_box(topn_over_union.clone())))
    });

    group.bench_function("topn_over_project", |b| {
        b.iter(|| apply_topn_pushdown(black_box(topn_over_project.clone())))
    });

    group.finish();
}

fn bench_predicate_inference(c: &mut Criterion) {
    let mut group = c.benchmark_group("pass/predicate_inference");

    let schema = make_schema("", 10);

    let join_with_filter = PhysicalPlan::Filter {
        input: Box::new(PhysicalPlan::HashJoin {
            left: Box::new(make_scan("t1", 5)),
            right: Box::new(make_scan("t2", 5)),
            join_type: JoinType::Inner,
            left_keys: vec![col("t1", "col0", 0)],
            right_keys: vec![col("t2", "col0", 5)],
            schema: schema.clone(),
            parallel: false,
            hints: ExecutionHints::default(),
        }),
        predicate: eq(col("t1", "col0", 0), lit_i64(42)),
    };

    let transitive_equality = PhysicalPlan::Filter {
        input: Box::new(PhysicalPlan::HashJoin {
            left: Box::new(make_scan("t1", 5)),
            right: Box::new(make_scan("t2", 5)),
            join_type: JoinType::Inner,
            left_keys: vec![col("t1", "col0", 0)],
            right_keys: vec![col("t2", "col0", 5)],
            schema,
            parallel: false,
            hints: ExecutionHints::default(),
        }),
        predicate: and(
            eq(col("t1", "col0", 0), col("t2", "col1", 6)),
            gt(col("t1", "col0", 0), lit_i64(100)),
        ),
    };

    group.bench_function("simple_inference", |b| {
        b.iter(|| apply_predicate_inference(black_box(join_with_filter.clone())))
    });

    group.bench_function("transitive", |b| {
        b.iter(|| apply_predicate_inference(black_box(transitive_equality.clone())))
    });

    group.finish();
}

fn bench_short_circuit_ordering(c: &mut Criterion) {
    let mut group = c.benchmark_group("pass/short_circuit");

    let and_chain = PhysicalPlan::Filter {
        input: Box::new(make_scan("t", 10)),
        predicate: and(
            and(
                eq(col("t", "col0", 0), lit_i64(42)),
                gt(col("t", "col1", 1), lit_i64(100)),
            ),
            and(
                lt(col("t", "col2", 2), lit_i64(50)),
                eq(col("t", "col3", 3), lit_str("active")),
            ),
        ),
    };

    let or_chain = PhysicalPlan::Filter {
        input: Box::new(make_scan("t", 10)),
        predicate: or(
            or(
                eq(col("t", "col0", 0), lit_i64(1)),
                eq(col("t", "col0", 0), lit_i64(2)),
            ),
            or(
                eq(col("t", "col0", 0), lit_i64(3)),
                eq(col("t", "col0", 0), lit_i64(4)),
            ),
        ),
    };

    group.bench_function("and_chain", |b| {
        b.iter(|| apply_short_circuit_ordering(black_box(and_chain.clone())))
    });

    group.bench_function("or_chain", |b| {
        b.iter(|| apply_short_circuit_ordering(black_box(or_chain.clone())))
    });

    group.finish();
}

fn bench_projection_pushdown(c: &mut Criterion) {
    let mut group = c.benchmark_group("pass/projection_pushdown");

    let simple_projection = {
        let scan = make_scan("orders", 20);
        let expressions: Vec<Expr> = (0..3)
            .map(|i| col("orders", &format!("col{}", i), i))
            .collect();
        let proj_fields: Vec<PlanField> = (0..3)
            .map(|i| PlanField::new(format!("col{}", i), DataType::Int64).with_table("orders"))
            .collect();
        PhysicalPlan::Project {
            input: Box::new(scan),
            expressions,
            schema: PlanSchema::from_fields(proj_fields),
        }
    };

    let projection_on_join = {
        let schema = make_schema("", 35);
        let join = PhysicalPlan::HashJoin {
            left: Box::new(make_scan("customers", 15)),
            right: Box::new(make_scan("orders", 20)),
            join_type: JoinType::Inner,
            left_keys: vec![col("customers", "col0", 0)],
            right_keys: vec![col("orders", "col0", 15)],
            schema,
            parallel: false,
            hints: ExecutionHints::default(),
        };
        let expressions = vec![col("customers", "col0", 0), col("orders", "col1", 16)];
        let proj_fields = vec![
            PlanField::new("col0", DataType::Int64).with_table("customers"),
            PlanField::new("col1", DataType::Int64).with_table("orders"),
        ];
        PhysicalPlan::Project {
            input: Box::new(join),
            expressions,
            schema: PlanSchema::from_fields(proj_fields),
        }
    };

    group.bench_function("simple", |b| {
        b.iter(|| ProjectionPushdown::optimize(black_box(simple_projection.clone())))
    });

    group.bench_function("over_join", |b| {
        b.iter(|| ProjectionPushdown::optimize(black_box(projection_on_join.clone())))
    });

    group.finish();
}

fn bench_constant_folding(c: &mut Criterion) {
    let mut group = c.benchmark_group("pass/constant_folding");

    let simple_add = binary(lit_i64(1), BinaryOp::Add, lit_i64(2));

    let nested_arithmetic = binary(
        binary(
            binary(lit_i64(1), BinaryOp::Add, lit_i64(2)),
            BinaryOp::Mul,
            binary(lit_i64(3), BinaryOp::Add, lit_i64(4)),
        ),
        BinaryOp::Sub,
        lit_i64(5),
    );

    let logical = and(
        and(lit_bool(true), col("t", "a", 0)),
        or(lit_bool(false), col("t", "b", 1)),
    );

    group.bench_function("simple_add", |b| {
        b.iter(|| fold_constants(black_box(&simple_add)))
    });

    group.bench_function("nested_arithmetic", |b| {
        b.iter(|| fold_constants(black_box(&nested_arithmetic)))
    });

    group.bench_function("logical", |b| {
        b.iter(|| fold_constants(black_box(&logical)))
    });

    group.finish();
}

fn bench_all_passes_pipeline(c: &mut Criterion) {
    let mut group = c.benchmark_group("pass/pipeline");

    let complex_plan = {
        let scan = make_scan("fact", 20);

        let filter1 = PhysicalPlan::Filter {
            input: Box::new(scan),
            predicate: lit_bool(true),
        };

        let filter2 = PhysicalPlan::Filter {
            input: Box::new(filter1),
            predicate: and(
                gt(col("fact", "col0", 0), lit_i64(100)),
                eq(lit_i64(1), lit_i64(1)),
            ),
        };

        let filter3 = PhysicalPlan::Filter {
            input: Box::new(filter2),
            predicate: lt(col("fact", "col1", 1), lit_i64(50)),
        };

        let schema = make_schema("", 25);
        let join = PhysicalPlan::HashJoin {
            left: Box::new(filter3),
            right: Box::new(make_scan("dim", 5)),
            join_type: JoinType::Inner,
            left_keys: vec![col("fact", "col2", 2)],
            right_keys: vec![col("dim", "col0", 20)],
            schema,
            parallel: false,
            hints: ExecutionHints::default(),
        };

        let expressions: Vec<Expr> = (0..5)
            .map(|i| col("fact", &format!("col{}", i), i))
            .collect();
        let proj_fields: Vec<PlanField> = (0..5)
            .map(|i| PlanField::new(format!("col{}", i), DataType::Int64).with_table("fact"))
            .collect();

        PhysicalPlan::Project {
            input: Box::new(join),
            expressions,
            schema: PlanSchema::from_fields(proj_fields),
        }
    };

    group.bench_function("no_optimization", |b| {
        b.iter(|| black_box(complex_plan.clone()))
    });

    group.bench_function("basic_passes", |b| {
        b.iter(|| {
            let plan = complex_plan.clone();
            let plan = apply_trivial_predicate_removal(plan);
            let plan = apply_empty_propagation(plan);
            let plan = apply_filter_merging(plan);
            let plan = apply_predicate_simplification(plan);
            apply_project_merging(black_box(plan))
        })
    });

    group.bench_function("standard_passes", |b| {
        b.iter(|| {
            let plan = complex_plan.clone();
            let plan = apply_trivial_predicate_removal(plan);
            let plan = apply_empty_propagation(plan);
            let plan = apply_filter_merging(plan);
            let plan = apply_predicate_simplification(plan);
            let plan = apply_project_merging(plan);
            let plan = apply_distinct_elimination(plan);
            let plan = apply_cross_to_hash_join(plan);
            let plan = apply_outer_to_inner_join(plan);
            let plan = apply_filter_pushdown_aggregate(plan);
            let plan = apply_sort_elimination(plan);
            let plan = apply_limit_pushdown(plan);
            apply_topn_pushdown(black_box(plan))
        })
    });

    group.bench_function("all_passes", |b| {
        b.iter(|| {
            let plan = complex_plan.clone();
            let plan = apply_trivial_predicate_removal(plan);
            let plan = apply_empty_propagation(plan);
            let plan = apply_filter_merging(plan);
            let plan = apply_predicate_simplification(plan);
            let plan = apply_project_merging(plan);
            let plan = apply_distinct_elimination(plan);
            let plan = apply_cross_to_hash_join(plan);
            let plan = apply_outer_to_inner_join(plan);
            let plan = apply_filter_pushdown_aggregate(plan);
            let plan = apply_sort_elimination(plan);
            let plan = apply_limit_pushdown(plan);
            let plan = apply_topn_pushdown(plan);
            let plan = apply_predicate_inference(plan);
            let plan = apply_short_circuit_ordering(plan);
            ProjectionPushdown::optimize(black_box(plan))
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_trivial_predicate_removal,
    bench_empty_propagation,
    bench_filter_merging,
    bench_predicate_simplification,
    bench_project_merging,
    bench_distinct_elimination,
    bench_cross_to_hash_join,
    bench_outer_to_inner_join,
    bench_filter_pushdown_aggregate,
    bench_sort_elimination,
    bench_limit_pushdown,
    bench_topn_pushdown,
    bench_predicate_inference,
    bench_short_circuit_ordering,
    bench_projection_pushdown,
    bench_constant_folding,
    bench_all_passes_pipeline,
);
criterion_main!(benches);
