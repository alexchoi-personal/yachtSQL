use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use yachtsql_common::types::DataType;
use yachtsql_ir::{BinaryOp, Expr, JoinType, Literal, LogicalPlan, PlanField, PlanSchema};
use yachtsql_optimizer::{OptimizationLevel, OptimizerSettings, optimize_with_settings};

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

fn make_join_condition(left_table: &str, right_table: &str, col: &str) -> Expr {
    Expr::BinaryOp {
        left: Box::new(Expr::Column {
            table: Some(left_table.to_string()),
            name: col.to_string(),
            index: None,
        }),
        op: BinaryOp::Eq,
        right: Box::new(Expr::Column {
            table: Some(right_table.to_string()),
            name: col.to_string(),
            index: None,
        }),
    }
}

fn make_simple_scan() -> LogicalPlan {
    make_scan("orders", 10)
}

fn make_filter_scan() -> LogicalPlan {
    let scan = make_scan("orders", 10);
    LogicalPlan::Filter {
        input: Box::new(scan),
        predicate: Expr::BinaryOp {
            left: Box::new(Expr::Column {
                table: Some("orders".to_string()),
                name: "col0".to_string(),
                index: None,
            }),
            op: BinaryOp::Gt,
            right: Box::new(Expr::Literal(Literal::Int64(100))),
        },
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
        condition: Some(make_join_condition("t1", "t2", "col0")),
        schema: merged_schema,
    }
}

fn make_3_table_join() -> LogicalPlan {
    let join = make_2_table_join();
    let t3 = make_scan("t3", 5);
    let merged_schema = join.schema().clone().merge(t3.schema().clone());

    LogicalPlan::Join {
        left: Box::new(join),
        right: Box::new(t3),
        join_type: JoinType::Inner,
        condition: Some(make_join_condition("t2", "t3", "col0")),
        schema: merged_schema,
    }
}

fn make_project_filter() -> LogicalPlan {
    let scan = make_filter_scan();
    let output_schema = PlanSchema::from_fields(vec![
        PlanField::new("col0".to_string(), DataType::Int64).with_table("orders"),
        PlanField::new("col1".to_string(), DataType::Int64).with_table("orders"),
    ]);

    LogicalPlan::Project {
        input: Box::new(scan),
        expressions: vec![
            Expr::Column {
                table: Some("orders".to_string()),
                name: "col0".to_string(),
                index: Some(0),
            },
            Expr::Column {
                table: Some("orders".to_string()),
                name: "col1".to_string(),
                index: Some(1),
            },
        ],
        schema: output_schema,
    }
}

fn bench_optimization_levels(c: &mut Criterion) {
    let mut group = c.benchmark_group("optimization_levels");

    let plans = [
        ("simple_scan", make_simple_scan()),
        ("filter_scan", make_filter_scan()),
        ("2_table_join", make_2_table_join()),
        ("3_table_join", make_3_table_join()),
        ("project_filter", make_project_filter()),
    ];

    let levels = [
        ("None", OptimizationLevel::None),
        ("Basic", OptimizationLevel::Basic),
        ("Standard", OptimizationLevel::Standard),
        ("Aggressive", OptimizationLevel::Aggressive),
        ("Full", OptimizationLevel::Full),
    ];

    for (plan_name, plan) in &plans {
        for (level_name, level) in &levels {
            group.bench_with_input(
                BenchmarkId::new(*level_name, *plan_name),
                &(plan.clone(), *level),
                |b, (plan, level)| {
                    let settings = OptimizerSettings::with_level(*level);
                    b.iter(|| optimize_with_settings(black_box(plan), black_box(&settings)))
                },
            );
        }
    }

    group.finish();
}

fn bench_cumulative_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("cumulative_overhead");

    let plans = [
        ("simple", make_simple_scan()),
        ("2_join", make_2_table_join()),
        ("3_join", make_3_table_join()),
    ];

    for (name, plan) in &plans {
        group.bench_with_input(
            BenchmarkId::new("none_vs_standard", *name),
            plan,
            |b, plan| {
                b.iter(|| {
                    let settings = OptimizerSettings::with_level(OptimizationLevel::Standard);
                    optimize_with_settings(black_box(plan), black_box(&settings))
                })
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_optimization_levels,
    bench_cumulative_overhead
);
criterion_main!(benches);
