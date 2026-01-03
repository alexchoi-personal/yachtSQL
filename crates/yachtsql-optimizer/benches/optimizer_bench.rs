use criterion::{Criterion, black_box, criterion_group, criterion_main};
use yachtsql_common::types::DataType;
use yachtsql_ir::{BinaryOp, Expr, JoinType, LogicalPlan, PlanField, PlanSchema};
use yachtsql_optimizer::{
    CostModel, GreedyJoinReorderer, PhysicalPlanner, PredicateCollector, ProjectionPushdown,
};

fn make_table_schema(table_name: &str, num_columns: usize) -> PlanSchema {
    let fields = (0..num_columns)
        .map(|i| PlanField::new(format!("col{}", i), DataType::Int64).with_table(table_name))
        .collect();
    PlanSchema::from_fields(fields)
}

fn make_scan(table_name: &str, num_columns: usize) -> LogicalPlan {
    LogicalPlan::Scan {
        table_name: table_name.to_string(),
        schema: make_table_schema(table_name, num_columns),
        projection: None,
    }
}

fn make_join_condition(left_table: &str, right_table: &str) -> Expr {
    Expr::BinaryOp {
        left: Box::new(Expr::Column {
            table: Some(left_table.to_string()),
            name: "col0".to_string(),
            index: None,
        }),
        op: BinaryOp::Eq,
        right: Box::new(Expr::Column {
            table: Some(right_table.to_string()),
            name: "col0".to_string(),
            index: None,
        }),
    }
}

fn make_2_table_join() -> LogicalPlan {
    let t1 = make_scan("t1", 5);
    let t2 = make_scan("t2", 5);

    let merged_schema = t1.schema().clone().merge(t2.schema().clone());

    LogicalPlan::Join {
        left: Box::new(t1),
        right: Box::new(t2),
        join_type: JoinType::Inner,
        condition: Some(make_join_condition("t1", "t2")),
        schema: merged_schema,
    }
}

fn make_5_table_star_join() -> LogicalPlan {
    let fact = make_scan("fact", 10);
    let dim1 = make_scan("dim1", 5);
    let dim2 = make_scan("dim2", 5);
    let dim3 = make_scan("dim3", 5);
    let dim4 = make_scan("dim4", 5);

    let join1_schema = fact.schema().clone().merge(dim1.schema().clone());
    let join1 = LogicalPlan::Join {
        left: Box::new(fact),
        right: Box::new(dim1),
        join_type: JoinType::Inner,
        condition: Some(make_join_condition("fact", "dim1")),
        schema: join1_schema.clone(),
    };

    let join2_schema = join1_schema.merge(dim2.schema().clone());
    let join2 = LogicalPlan::Join {
        left: Box::new(join1),
        right: Box::new(dim2),
        join_type: JoinType::Inner,
        condition: Some(make_join_condition("fact", "dim2")),
        schema: join2_schema.clone(),
    };

    let join3_schema = join2_schema.merge(dim3.schema().clone());
    let join3 = LogicalPlan::Join {
        left: Box::new(join2),
        right: Box::new(dim3),
        join_type: JoinType::Inner,
        condition: Some(make_join_condition("fact", "dim3")),
        schema: join3_schema.clone(),
    };

    let join4_schema = join3_schema.merge(dim4.schema().clone());
    LogicalPlan::Join {
        left: Box::new(join3),
        right: Box::new(dim4),
        join_type: JoinType::Inner,
        condition: Some(make_join_condition("fact", "dim4")),
        schema: join4_schema,
    }
}

fn make_simple_projection() -> LogicalPlan {
    let scan = make_scan("orders", 20);
    let schema = scan.schema().clone();

    let expressions: Vec<Expr> = (0..3)
        .map(|i| Expr::Column {
            table: Some("orders".to_string()),
            name: format!("col{}", i),
            index: Some(i),
        })
        .collect();

    let proj_fields: Vec<PlanField> = expressions
        .iter()
        .enumerate()
        .map(|(i, _)| PlanField::new(format!("col{}", i), DataType::Int64).with_table("orders"))
        .collect();

    LogicalPlan::Project {
        input: Box::new(LogicalPlan::Scan {
            table_name: "orders".to_string(),
            schema,
            projection: None,
        }),
        expressions,
        schema: PlanSchema::from_fields(proj_fields),
    }
}

fn make_projection_on_join() -> LogicalPlan {
    let t1 = make_scan("customers", 15);
    let t2 = make_scan("orders", 20);

    let merged_schema = t1.schema().clone().merge(t2.schema().clone());

    let join = LogicalPlan::Join {
        left: Box::new(t1),
        right: Box::new(t2),
        join_type: JoinType::Inner,
        condition: Some(make_join_condition("customers", "orders")),
        schema: merged_schema.clone(),
    };

    let expressions: Vec<Expr> = vec![
        Expr::Column {
            table: Some("customers".to_string()),
            name: "col0".to_string(),
            index: Some(0),
        },
        Expr::Column {
            table: Some("customers".to_string()),
            name: "col1".to_string(),
            index: Some(1),
        },
        Expr::Column {
            table: Some("orders".to_string()),
            name: "col0".to_string(),
            index: Some(15),
        },
    ];

    let proj_fields = vec![
        PlanField::new("col0", DataType::Int64).with_table("customers"),
        PlanField::new("col1", DataType::Int64).with_table("customers"),
        PlanField::new("col0", DataType::Int64).with_table("orders"),
    ];

    LogicalPlan::Project {
        input: Box::new(join),
        expressions,
        schema: PlanSchema::from_fields(proj_fields),
    }
}

fn make_filter_pushdown_plan() -> LogicalPlan {
    let t1 = make_scan("t1", 10);
    let t2 = make_scan("t2", 10);

    let merged_schema = t1.schema().clone().merge(t2.schema().clone());

    let join = LogicalPlan::Join {
        left: Box::new(t1),
        right: Box::new(t2),
        join_type: JoinType::Inner,
        condition: Some(make_join_condition("t1", "t2")),
        schema: merged_schema,
    };

    let filter_predicate = Expr::BinaryOp {
        left: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column {
                table: Some("t1".to_string()),
                name: "col1".to_string(),
                index: Some(1),
            }),
            op: BinaryOp::Gt,
            right: Box::new(Expr::Literal(yachtsql_ir::Literal::Int64(100))),
        }),
        op: BinaryOp::And,
        right: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column {
                table: Some("t2".to_string()),
                name: "col2".to_string(),
                index: Some(12),
            }),
            op: BinaryOp::Lt,
            right: Box::new(Expr::Literal(yachtsql_ir::Literal::Int64(50))),
        }),
    };

    LogicalPlan::Filter {
        input: Box::new(join),
        predicate: filter_predicate,
    }
}

fn bench_join_reorder_2_tables(c: &mut Criterion) {
    let plan = make_2_table_join();
    let cost_model = CostModel::new();

    c.bench_function("join_reorder_2_tables", |b| {
        b.iter(|| {
            let graph = PredicateCollector::build_join_graph(black_box(&plan), &cost_model);
            if let Some(g) = graph {
                let reorderer = GreedyJoinReorderer::new(CostModel::new());
                let _ = black_box(reorderer.reorder(&g, plan.schema()));
            }
        })
    });
}

fn bench_join_reorder_5_tables(c: &mut Criterion) {
    let plan = make_5_table_star_join();
    let cost_model = CostModel::new();

    c.bench_function("join_reorder_5_tables", |b| {
        b.iter(|| {
            let graph = PredicateCollector::build_join_graph(black_box(&plan), &cost_model);
            if let Some(g) = graph {
                let reorderer = GreedyJoinReorderer::new(CostModel::new());
                let _ = black_box(reorderer.reorder(&g, plan.schema()));
            }
        })
    });
}

fn bench_projection_pushdown_simple(c: &mut Criterion) {
    let plan = make_simple_projection();
    let planner = PhysicalPlanner::new();

    c.bench_function("projection_pushdown_simple", |b| {
        b.iter(|| {
            let physical = planner.plan(black_box(&plan)).unwrap();
            let _ = black_box(ProjectionPushdown::optimize(physical));
        })
    });
}

fn bench_projection_pushdown_join(c: &mut Criterion) {
    let plan = make_projection_on_join();
    let planner = PhysicalPlanner::new();

    c.bench_function("projection_pushdown_join", |b| {
        b.iter(|| {
            let physical = planner.plan(black_box(&plan)).unwrap();
            let _ = black_box(ProjectionPushdown::optimize(physical));
        })
    });
}

fn bench_filter_pushdown(c: &mut Criterion) {
    let plan = make_filter_pushdown_plan();
    let planner = PhysicalPlanner::new();

    c.bench_function("filter_pushdown", |b| {
        b.iter(|| {
            let physical = planner.plan(black_box(&plan)).unwrap();
            let _ = black_box(ProjectionPushdown::optimize(physical));
        })
    });
}

fn bench_physical_planner(c: &mut Criterion) {
    let plan = make_5_table_star_join();

    c.bench_function("physical_planner_5_table_join", |b| {
        b.iter(|| {
            let _ = black_box(yachtsql_optimizer::optimize(black_box(&plan)));
        })
    });
}

fn make_10_table_chain_join() -> LogicalPlan {
    let mut current = make_scan("t0", 5);
    let mut current_schema = current.schema().clone();

    for i in 1..10 {
        let next_table = format!("t{}", i);
        let prev_table = format!("t{}", i - 1);
        let next = make_scan(&next_table, 5);
        let next_schema = next.schema().clone();
        let merged = current_schema.clone().merge(next_schema);

        current = LogicalPlan::Join {
            left: Box::new(current),
            right: Box::new(next),
            join_type: JoinType::Inner,
            condition: Some(make_join_condition(&prev_table, &next_table)),
            schema: merged.clone(),
        };
        current_schema = merged;
    }
    current
}

fn bench_join_reorder_10_tables(c: &mut Criterion) {
    let plan = make_10_table_chain_join();
    let cost_model = CostModel::new();

    c.bench_function("join_reorder_10_tables", |b| {
        b.iter(|| {
            let graph = PredicateCollector::build_join_graph(black_box(&plan), &cost_model);
            if let Some(g) = graph {
                let reorderer = GreedyJoinReorderer::new(CostModel::new());
                let _ = black_box(reorderer.reorder(&g, plan.schema()));
            }
        })
    });
}

fn make_filter_on_aggregate() -> LogicalPlan {
    let scan = make_scan("sales", 10);

    let group_by = vec![
        Expr::Column {
            table: Some("sales".to_string()),
            name: "col0".to_string(),
            index: Some(0),
        },
        Expr::Column {
            table: Some("sales".to_string()),
            name: "col1".to_string(),
            index: Some(1),
        },
    ];

    let aggregates = vec![Expr::Aggregate {
        func: yachtsql_ir::AggregateFunction::Sum,
        args: vec![Expr::Column {
            table: Some("sales".to_string()),
            name: "col2".to_string(),
            index: Some(2),
        }],
        distinct: false,
        filter: None,
        order_by: vec![],
        limit: None,
        ignore_nulls: false,
    }];

    let agg_schema = PlanSchema::from_fields(vec![
        PlanField::new("col0", DataType::Int64).with_table("sales"),
        PlanField::new("col1", DataType::Int64).with_table("sales"),
        PlanField::new("sum_col2", DataType::Int64),
    ]);

    let aggregate = LogicalPlan::Aggregate {
        input: Box::new(scan),
        group_by,
        aggregates,
        schema: agg_schema,
        grouping_sets: None,
    };

    let filter_predicate = Expr::BinaryOp {
        left: Box::new(Expr::Column {
            table: None,
            name: "col0".to_string(),
            index: Some(0),
        }),
        op: BinaryOp::Eq,
        right: Box::new(Expr::Literal(yachtsql_ir::Literal::Int64(42))),
    };

    LogicalPlan::Filter {
        input: Box::new(aggregate),
        predicate: filter_predicate,
    }
}

fn bench_filter_pushdown_aggregate(c: &mut Criterion) {
    let plan = make_filter_on_aggregate();
    let planner = PhysicalPlanner::new();

    c.bench_function("filter_pushdown_aggregate", |b| {
        b.iter(|| {
            let physical = planner.plan(black_box(&plan)).unwrap();
            let _ = black_box(ProjectionPushdown::optimize(physical));
        })
    });
}

fn make_filter_on_window() -> LogicalPlan {
    let scan = make_scan("events", 10);
    let scan_schema = scan.schema().clone();

    let partition_by = vec![Expr::Column {
        table: Some("events".to_string()),
        name: "col0".to_string(),
        index: Some(0),
    }];

    let window_expr = Expr::Window {
        func: yachtsql_ir::WindowFunction::RowNumber,
        args: vec![],
        partition_by: partition_by.clone(),
        order_by: vec![yachtsql_ir::SortExpr {
            expr: Expr::Column {
                table: Some("events".to_string()),
                name: "col1".to_string(),
                index: Some(1),
            },
            asc: true,
            nulls_first: false,
        }],
        frame: None,
    };

    let mut window_schema_fields: Vec<PlanField> = scan_schema.fields.to_vec();
    window_schema_fields.push(PlanField::new("row_num", DataType::Int64));
    let window_schema = PlanSchema::from_fields(window_schema_fields);

    let window = LogicalPlan::Window {
        input: Box::new(scan),
        window_exprs: vec![window_expr],
        schema: window_schema,
    };

    let filter_predicate = Expr::BinaryOp {
        left: Box::new(Expr::Column {
            table: Some("events".to_string()),
            name: "col2".to_string(),
            index: Some(2),
        }),
        op: BinaryOp::Gt,
        right: Box::new(Expr::Literal(yachtsql_ir::Literal::Int64(100))),
    };

    LogicalPlan::Filter {
        input: Box::new(window),
        predicate: filter_predicate,
    }
}

fn bench_filter_pushdown_window(c: &mut Criterion) {
    let plan = make_filter_on_window();
    let planner = PhysicalPlanner::new();

    c.bench_function("filter_pushdown_window", |b| {
        b.iter(|| {
            let physical = planner.plan(black_box(&plan)).unwrap();
            let _ = black_box(ProjectionPushdown::optimize(physical));
        })
    });
}

fn make_complex_query() -> LogicalPlan {
    let fact = make_scan("fact", 20);
    let dim1 = make_scan("dim1", 10);
    let dim2 = make_scan("dim2", 10);

    let join1_schema = fact.schema().clone().merge(dim1.schema().clone());
    let join1 = LogicalPlan::Join {
        left: Box::new(fact),
        right: Box::new(dim1),
        join_type: JoinType::Inner,
        condition: Some(make_join_condition("fact", "dim1")),
        schema: join1_schema.clone(),
    };

    let join2_schema = join1_schema.merge(dim2.schema().clone());
    let join2 = LogicalPlan::Join {
        left: Box::new(join1),
        right: Box::new(dim2),
        join_type: JoinType::Inner,
        condition: Some(make_join_condition("fact", "dim2")),
        schema: join2_schema.clone(),
    };

    let filter_predicate = Expr::BinaryOp {
        left: Box::new(Expr::Column {
            table: Some("dim1".to_string()),
            name: "col1".to_string(),
            index: Some(21),
        }),
        op: BinaryOp::Eq,
        right: Box::new(Expr::Literal(yachtsql_ir::Literal::Int64(1))),
    };

    let filtered = LogicalPlan::Filter {
        input: Box::new(join2),
        predicate: filter_predicate,
    };

    let proj_exprs: Vec<Expr> = (0..5)
        .map(|i| Expr::Column {
            table: Some("fact".to_string()),
            name: format!("col{}", i),
            index: Some(i),
        })
        .collect();

    let proj_fields: Vec<PlanField> = (0..5)
        .map(|i| PlanField::new(format!("col{}", i), DataType::Int64).with_table("fact"))
        .collect();

    LogicalPlan::Project {
        input: Box::new(filtered),
        expressions: proj_exprs,
        schema: PlanSchema::from_fields(proj_fields),
    }
}

fn bench_complex_query_optimization(c: &mut Criterion) {
    let plan = make_complex_query();

    c.bench_function("complex_query_3_table_join_filter_project", |b| {
        b.iter(|| {
            let _ = black_box(yachtsql_optimizer::optimize(black_box(&plan)));
        })
    });
}

criterion_group!(
    benches,
    bench_join_reorder_2_tables,
    bench_join_reorder_5_tables,
    bench_join_reorder_10_tables,
    bench_projection_pushdown_simple,
    bench_projection_pushdown_join,
    bench_filter_pushdown,
    bench_filter_pushdown_aggregate,
    bench_filter_pushdown_window,
    bench_physical_planner,
    bench_complex_query_optimization,
);
criterion_main!(benches);
