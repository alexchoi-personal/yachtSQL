#![coverage(off)]

#[cfg(test)]
mod benchmark_query_analysis {
    use yachtsql_common::types::DataType;
    use yachtsql_storage::{Field, Schema};

    use crate::test_utils::MockCatalog;
    use crate::{OptimizerSettings, PhysicalPlan, RuleFlags, optimize_with_settings};

    fn users_schema() -> Schema {
        Schema::from_fields(vec![
            Field::nullable("id", DataType::Int64),
            Field::nullable("name", DataType::String),
            Field::nullable("status", DataType::String),
            Field::nullable("country", DataType::String),
            Field::nullable("score", DataType::Int64),
        ])
    }

    fn orders_schema() -> Schema {
        Schema::from_fields(vec![
            Field::nullable("id", DataType::Int64),
            Field::nullable("user_id", DataType::Int64),
            Field::nullable("amount", DataType::Int64),
            Field::nullable("status", DataType::String),
            Field::nullable("region", DataType::String),
        ])
    }

    fn test_catalog() -> MockCatalog {
        MockCatalog::new()
            .with_table("users", users_schema())
            .with_table("orders", orders_schema())
    }

    fn plan_variant_name(plan: &PhysicalPlan) -> &'static str {
        match plan {
            PhysicalPlan::TableScan { .. } => "TableScan",
            PhysicalPlan::Filter { .. } => "Filter",
            PhysicalPlan::Project { .. } => "Project",
            PhysicalPlan::HashAggregate { .. } => "HashAggregate",
            PhysicalPlan::HashJoin { .. } => "HashJoin",
            PhysicalPlan::Sort { .. } => "Sort",
            PhysicalPlan::TopN { .. } => "TopN",
            PhysicalPlan::Limit { .. } => "Limit",
            PhysicalPlan::Distinct { .. } => "Distinct",
            PhysicalPlan::Union { .. } => "Union",
            PhysicalPlan::WithCte { .. } => "WithCte",
            PhysicalPlan::Empty { .. } => "Empty",
            _ => "Other",
        }
    }

    fn print_plan_tree(plan: &PhysicalPlan, indent: usize) {
        let prefix = "  ".repeat(indent);
        print!("{}{}", prefix, plan_variant_name(plan));

        match plan {
            PhysicalPlan::TableScan { table_name, .. } => println!("({})", table_name),
            PhysicalPlan::Filter { input, .. } => {
                println!();
                print_plan_tree(input, indent + 1);
            }
            PhysicalPlan::Project { input, .. } => {
                println!();
                print_plan_tree(input, indent + 1);
            }
            PhysicalPlan::HashAggregate { input, .. } => {
                println!();
                print_plan_tree(input, indent + 1);
            }
            PhysicalPlan::Sort { input, .. } => {
                println!();
                print_plan_tree(input, indent + 1);
            }
            PhysicalPlan::TopN { input, limit, .. } => {
                println!("(limit={})", limit);
                print_plan_tree(input, indent + 1);
            }
            PhysicalPlan::Limit { input, limit, .. } => {
                println!("(limit={:?})", limit);
                print_plan_tree(input, indent + 1);
            }
            PhysicalPlan::Distinct { input } => {
                println!();
                print_plan_tree(input, indent + 1);
            }
            PhysicalPlan::Union { inputs, all, .. } => {
                println!("(all={})", all);
                for inp in inputs {
                    print_plan_tree(inp, indent + 1);
                }
            }
            PhysicalPlan::WithCte { body, .. } => {
                println!();
                print_plan_tree(body, indent + 1);
            }
            PhysicalPlan::HashJoin { left, right, .. } => {
                println!();
                print_plan_tree(left, indent + 1);
                print_plan_tree(right, indent + 1);
            }
            PhysicalPlan::NestedLoopJoin { left, right, .. } => {
                println!();
                print_plan_tree(left, indent + 1);
                print_plan_tree(right, indent + 1);
            }
            PhysicalPlan::CrossJoin { left, right, .. } => {
                println!();
                print_plan_tree(left, indent + 1);
                print_plan_tree(right, indent + 1);
            }
            _ => println!(),
        }
    }

    fn optimize_with_rules(sql: &str, rule_flag: fn(&mut RuleFlags)) -> PhysicalPlan {
        let catalog = test_catalog();
        let logical = yachtsql_parser::parse_and_plan(sql, &catalog).expect("failed to parse SQL");

        let mut flags = RuleFlags::all_disabled();
        rule_flag(&mut flags);

        let settings = OptimizerSettings {
            rules: flags,
            ..Default::default()
        };

        optimize_with_settings(&logical, &settings).expect("failed to optimize")
    }

    fn optimize_with_rule(sql: &str, rule_flag: fn(&mut RuleFlags)) -> PhysicalPlan {
        let catalog = test_catalog();
        let logical = yachtsql_parser::parse_and_plan(sql, &catalog).expect("failed to parse SQL");

        let mut flags = RuleFlags::all_disabled();
        rule_flag(&mut flags);

        let settings = OptimizerSettings {
            rules: flags,
            ..Default::default()
        };

        optimize_with_settings(&logical, &settings).expect("failed to optimize")
    }

    fn optimize_without_rules(sql: &str) -> PhysicalPlan {
        let catalog = test_catalog();
        let logical = yachtsql_parser::parse_and_plan(sql, &catalog).expect("failed to parse SQL");

        let settings = OptimizerSettings {
            rules: RuleFlags::all_disabled(),
            ..Default::default()
        };

        optimize_with_settings(&logical, &settings).expect("failed to optimize")
    }

    #[test]
    fn analyze_filter_merging() {
        let sql = "WITH base AS (SELECT * FROM users WHERE status = 'active') SELECT * FROM base WHERE country = 'US' AND score > 50";

        println!("\n=== FILTER_MERGING ===");
        println!("Query: {}", sql);

        println!("\nWithout optimization:");
        let plan_off = optimize_without_rules(sql);
        print_plan_tree(&plan_off, 0);

        println!("\nWith filter_merging:");
        let plan_on = optimize_with_rule(sql, |f| f.filter_merging = Some(true));
        print_plan_tree(&plan_on, 0);
    }

    #[test]
    fn analyze_distinct_elimination() {
        let sql = "SELECT DISTINCT * FROM (SELECT * FROM users LIMIT 1) sub";

        println!("\n=== DISTINCT_ELIMINATION ===");
        println!("Query: {}", sql);

        println!("\nWithout optimization:");
        let plan_off = optimize_without_rules(sql);
        print_plan_tree(&plan_off, 0);

        println!("\nWith distinct_elimination:");
        let plan_on = optimize_with_rule(sql, |f| f.distinct_elimination = Some(true));
        print_plan_tree(&plan_on, 0);
    }

    #[test]
    fn analyze_sort_elimination() {
        let sql = "WITH ordered AS (SELECT * FROM users ORDER BY score) SELECT * FROM ordered ORDER BY score";

        println!("\n=== SORT_ELIMINATION ===");
        println!("Query: {}", sql);

        println!("\nWithout optimization:");
        let plan_off = optimize_without_rules(sql);
        print_plan_tree(&plan_off, 0);

        println!("\nWith sort_elimination:");
        let plan_on = optimize_with_rule(sql, |f| f.sort_elimination = Some(true));
        print_plan_tree(&plan_on, 0);
    }

    #[test]
    fn analyze_topn_pushdown() {
        let sql = "SELECT * FROM (SELECT * FROM users UNION ALL SELECT * FROM users) ORDER BY score DESC LIMIT 10";

        println!("\n=== TOPN_PUSHDOWN ===");
        println!("Query: {}", sql);

        println!("\nWithout optimization:");
        let plan_off = optimize_without_rules(sql);
        print_plan_tree(&plan_off, 0);

        println!("\nWith topn_pushdown:");
        let plan_on = optimize_with_rule(sql, |f| f.topn_pushdown = Some(true));
        print_plan_tree(&plan_on, 0);
    }

    #[test]
    fn analyze_filter_pushdown_agg() {
        let sql = "SELECT country, cnt FROM (SELECT country, COUNT(*) as cnt FROM users GROUP BY country) WHERE country = 'US'";

        println!("\n=== FILTER_PUSHDOWN_AGG ===");
        println!("Query: {}", sql);

        println!("\nWithout optimization:");
        let plan_off = optimize_without_rules(sql);
        print_plan_tree(&plan_off, 0);

        println!("\nWith filter_pushdown_aggregate:");
        let plan_on = optimize_with_rule(sql, |f| f.filter_pushdown_aggregate = Some(true));
        print_plan_tree(&plan_on, 0);
    }

    #[test]
    fn analyze_filter_pushdown_join() {
        let sql = "SELECT * FROM (SELECT u.name as user_name, u.country, u.score, o.amount, o.status as order_status FROM users u JOIN orders o ON u.id = o.user_id) sub WHERE sub.country = 'US' AND sub.order_status = 'active' AND sub.score < 10";

        println!("\n=== FILTER_PUSHDOWN_JOIN ===");
        println!("Query: {}", sql);

        println!("\nWithout optimization:");
        let plan_off = optimize_without_rules(sql);
        print_plan_tree(&plan_off, 0);

        println!("\nWith filter_pushdown_project only:");
        let plan_proj = optimize_with_rules(sql, |f| f.filter_pushdown_project = Some(true));
        print_plan_tree(&plan_proj, 0);

        println!("\nWith filter_pushdown_project + filter_pushdown_join:");
        let plan_both = optimize_with_rules(sql, |f| {
            f.filter_pushdown_project = Some(true);
            f.filter_pushdown_join = Some(true);
        });
        print_plan_tree(&plan_both, 0);
    }
}

#[cfg(test)]
mod join_order_tests {
    use rustc_hash::FxHashMap;
    use yachtsql_common::types::DataType;
    use yachtsql_ir::{BinaryOp, Expr, JoinType, LogicalPlan, PlanField, PlanSchema};

    use crate::stats::TableStats;
    use crate::{
        CostModel, GreedyJoinReorderer, JoinEdge, JoinGraph, JoinRelation, PredicateCollector,
    };

    fn make_schema(fields: &[(&str, DataType)]) -> PlanSchema {
        PlanSchema::from_fields(
            fields
                .iter()
                .map(|(name, dtype)| PlanField::new(*name, dtype.clone()))
                .collect(),
        )
    }

    fn make_table_schema(table: &str, fields: &[(&str, DataType)]) -> PlanSchema {
        PlanSchema::from_fields(
            fields
                .iter()
                .map(|(name, dtype)| PlanField::new(*name, dtype.clone()).with_table(table))
                .collect(),
        )
    }

    fn make_scan(table_name: &str, schema: PlanSchema) -> LogicalPlan {
        LogicalPlan::Scan {
            table_name: table_name.to_string(),
            schema,
            projection: None,
        }
    }

    fn make_relation(table_name: &str, schema: PlanSchema, row_count: usize) -> JoinRelation {
        let plan = make_scan(table_name, schema.clone());
        JoinRelation {
            id: 0,
            table_name: Some(table_name.to_string()),
            original_position: 0,
            plan,
            schema,
            row_count_estimate: row_count,
        }
    }

    fn col_table(table: &str, name: &str) -> Expr {
        Expr::Column {
            table: Some(table.to_string()),
            name: name.to_string(),
            index: None,
        }
    }

    fn eq(left: Expr, right: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOp::Eq,
            right: Box::new(right),
        }
    }

    fn and(left: Expr, right: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOp::And,
            right: Box::new(right),
        }
    }

    mod join_graph_tests {
        use super::*;

        #[test]
        fn add_relation_assigns_id_and_position() {
            let mut graph = JoinGraph::new();
            let schema = make_schema(&[("id", DataType::Int64)]);

            let rel1 = make_relation("t1", schema.clone(), 100);
            let id1 = graph.add_relation(rel1);
            assert_eq!(id1, 0);

            let rel2 = make_relation("t2", schema.clone(), 200);
            let id2 = graph.add_relation(rel2);
            assert_eq!(id2, 1);

            let rel3 = make_relation("t3", schema, 300);
            let id3 = graph.add_relation(rel3);
            assert_eq!(id3, 2);
        }

        #[test]
        fn get_relation_returns_relation_by_id() {
            let mut graph = JoinGraph::new();
            let schema = make_schema(&[("id", DataType::Int64)]);

            let rel = make_relation("users", schema, 500);
            let id = graph.add_relation(rel);

            let retrieved = graph.get_relation(id);
            assert!(retrieved.is_some());
            let retrieved = retrieved.unwrap();
            assert_eq!(retrieved.table_name, Some("users".to_string()));
            assert_eq!(retrieved.row_count_estimate, 500);
        }

        #[test]
        fn get_relation_returns_none_for_invalid_id() {
            let graph = JoinGraph::new();
            assert!(graph.get_relation(0).is_none());
            assert!(graph.get_relation(100).is_none());
        }

        #[test]
        fn add_edge_connects_relations() {
            let mut graph = JoinGraph::new();
            let schema = make_schema(&[("id", DataType::Int64)]);

            let rel1 = make_relation("t1", schema.clone(), 100);
            let rel2 = make_relation("t2", schema, 200);

            let id1 = graph.add_relation(rel1);
            let id2 = graph.add_relation(rel2);

            let edge = JoinEdge {
                left_relation: id1,
                right_relation: id2,
                predicate: eq(col_table("t1", "id"), col_table("t2", "id")),
                selectivity_estimate: 0.1,
            };

            graph.add_edge(edge);

            let edges = graph.get_edges_between(id1, id2);
            assert_eq!(edges.len(), 1);
            assert_eq!(edges[0].left_relation, 0);
            assert_eq!(edges[0].right_relation, 1);
        }

        #[test]
        fn get_edges_between_is_bidirectional() {
            let mut graph = JoinGraph::new();
            let schema = make_schema(&[("id", DataType::Int64)]);

            let id1 = graph.add_relation(make_relation("t1", schema.clone(), 100));
            let id2 = graph.add_relation(make_relation("t2", schema, 200));

            let edge = JoinEdge {
                left_relation: id1,
                right_relation: id2,
                predicate: eq(col_table("t1", "id"), col_table("t2", "id")),
                selectivity_estimate: 0.1,
            };
            graph.add_edge(edge);

            let edges_forward = graph.get_edges_between(id1, id2);
            let edges_backward = graph.get_edges_between(id2, id1);

            assert_eq!(edges_forward.len(), 1);
            assert_eq!(edges_backward.len(), 1);
        }

        #[test]
        fn get_edges_between_returns_empty_for_unconnected_relations() {
            let mut graph = JoinGraph::new();
            let schema = make_schema(&[("id", DataType::Int64)]);

            let id1 = graph.add_relation(make_relation("t1", schema.clone(), 100));
            let id2 = graph.add_relation(make_relation("t2", schema.clone(), 200));
            let id3 = graph.add_relation(make_relation("t3", schema, 300));

            let edge = JoinEdge {
                left_relation: id1,
                right_relation: id2,
                predicate: eq(col_table("t1", "id"), col_table("t2", "id")),
                selectivity_estimate: 0.1,
            };
            graph.add_edge(edge);

            let edges = graph.get_edges_between(id1, id3);
            assert!(edges.is_empty());
        }

        #[test]
        fn multiple_edges_between_same_relations() {
            let mut graph = JoinGraph::new();
            let schema = make_schema(&[("id", DataType::Int64), ("code", DataType::String)]);

            let id1 = graph.add_relation(make_relation("t1", schema.clone(), 100));
            let id2 = graph.add_relation(make_relation("t2", schema, 200));

            let edge1 = JoinEdge {
                left_relation: id1,
                right_relation: id2,
                predicate: eq(col_table("t1", "id"), col_table("t2", "id")),
                selectivity_estimate: 0.1,
            };
            let edge2 = JoinEdge {
                left_relation: id1,
                right_relation: id2,
                predicate: eq(col_table("t1", "code"), col_table("t2", "code")),
                selectivity_estimate: 0.05,
            };

            graph.add_edge(edge1);
            graph.add_edge(edge2);

            let edges = graph.get_edges_between(id1, id2);
            assert_eq!(edges.len(), 2);
        }

        #[test]
        fn relations_returns_all_relations() {
            let mut graph = JoinGraph::new();
            let schema = make_schema(&[("id", DataType::Int64)]);

            graph.add_relation(make_relation("t1", schema.clone(), 100));
            graph.add_relation(make_relation("t2", schema.clone(), 200));
            graph.add_relation(make_relation("t3", schema, 300));

            assert_eq!(graph.relations().len(), 3);
        }

        #[test]
        fn default_creates_empty_graph() {
            let graph = JoinGraph::default();
            assert!(graph.relations().is_empty());
        }
    }

    mod cost_model_tests {
        use super::*;

        #[test]
        fn estimate_base_cardinality_with_stats() {
            let mut stats = FxHashMap::default();
            stats.insert("USERS".to_string(), TableStats::new(5000));
            stats.insert("ORDERS".to_string(), TableStats::new(10000));

            let cost_model = CostModel::with_stats(stats);

            assert_eq!(cost_model.estimate_base_cardinality("users"), 5000);
            assert_eq!(cost_model.estimate_base_cardinality("orders"), 10000);
        }

        #[test]
        fn estimate_base_cardinality_returns_default_for_unknown_table() {
            let cost_model = CostModel::new();
            assert_eq!(cost_model.estimate_base_cardinality("unknown"), 1000);
        }

        #[test]
        fn estimate_base_cardinality_with_empty_stats() {
            let cost_model = CostModel::with_stats(FxHashMap::default());
            assert_eq!(cost_model.estimate_base_cardinality("any_table"), 1000);
        }

        #[test]
        fn estimate_join_cost_with_edges() {
            let cost_model = CostModel::new();
            let edge = JoinEdge {
                left_relation: 0,
                right_relation: 1,
                predicate: eq(col_table("t1", "id"), col_table("t2", "id")),
                selectivity_estimate: 0.1,
            };

            let cost = cost_model.estimate_join_cost(1000, 500, &[&edge]);

            let expected_output_rows = (1000.0_f64 * 500.0 * 0.1).ceil() as usize;
            assert_eq!(cost.output_rows, expected_output_rows);

            let expected_cost = (500.0 + 1000.0 + expected_output_rows as f64) * 1.0;
            assert!((cost.total_cost - expected_cost).abs() < 0.001);
        }

        #[test]
        fn estimate_join_cost_without_edges_has_cross_join_penalty() {
            let cost_model = CostModel::new();

            let cost = cost_model.estimate_join_cost(100, 50, &[]);

            let expected_output_rows = (100.0_f64 * 50.0 * 1.0).ceil() as usize;
            assert_eq!(cost.output_rows, expected_output_rows);

            let expected_cost = (50.0 + 100.0 + expected_output_rows as f64) * 1000.0;
            assert!((cost.total_cost - expected_cost).abs() < 0.001);
        }

        #[test]
        fn estimate_join_cost_multiple_edges_combines_selectivity() {
            let cost_model = CostModel::new();
            let edge1 = JoinEdge {
                left_relation: 0,
                right_relation: 1,
                predicate: eq(col_table("t1", "id"), col_table("t2", "id")),
                selectivity_estimate: 0.1,
            };
            let edge2 = JoinEdge {
                left_relation: 0,
                right_relation: 1,
                predicate: eq(col_table("t1", "code"), col_table("t2", "code")),
                selectivity_estimate: 0.5,
            };

            let cost = cost_model.estimate_join_cost(1000, 500, &[&edge1, &edge2]);

            let combined_selectivity = 0.1 * 0.5;
            let expected_output_rows = (1000.0_f64 * 500.0 * combined_selectivity).ceil() as usize;
            assert_eq!(cost.output_rows, expected_output_rows);
        }

        #[test]
        fn estimate_join_cost_output_rows_at_least_one() {
            let cost_model = CostModel::new();
            let edge = JoinEdge {
                left_relation: 0,
                right_relation: 1,
                predicate: eq(col_table("t1", "id"), col_table("t2", "id")),
                selectivity_estimate: 0.00001,
            };

            let cost = cost_model.estimate_join_cost(1, 1, &[&edge]);
            assert_eq!(cost.output_rows, 1);
        }

        #[test]
        fn default_cost_model() {
            let cost_model = CostModel::default();
            assert_eq!(cost_model.estimate_base_cardinality("any"), 1000);
        }
    }

    mod greedy_reorderer_tests {
        use super::*;

        fn make_graph_two_tables() -> (JoinGraph, PlanSchema) {
            let mut graph = JoinGraph::new();

            let schema1 = make_table_schema("t1", &[("id", DataType::Int64)]);
            let schema2 = make_table_schema("t2", &[("id", DataType::Int64)]);

            graph.add_relation(make_relation("t1", schema1.clone(), 1000));
            graph.add_relation(make_relation("t2", schema2.clone(), 100));

            let edge = JoinEdge {
                left_relation: 0,
                right_relation: 1,
                predicate: eq(col_table("t1", "id"), col_table("t2", "id")),
                selectivity_estimate: 0.1,
            };
            graph.add_edge(edge);

            let combined_schema = PlanSchema::from_fields(vec![
                PlanField::new("id", DataType::Int64).with_table("t1"),
                PlanField::new("id", DataType::Int64).with_table("t2"),
            ]);

            (graph, combined_schema)
        }

        #[test]
        fn reorder_two_tables_starts_with_smallest() {
            let (graph, schema) = make_graph_two_tables();
            let cost_model = CostModel::new();
            let reorderer = GreedyJoinReorderer::new(cost_model);

            let plan = reorderer.reorder(&graph, &schema).unwrap();

            match plan {
                LogicalPlan::Project { input, .. } => match *input {
                    LogicalPlan::Join {
                        left,
                        right,
                        join_type,
                        ..
                    } => {
                        assert_eq!(join_type, JoinType::Inner);

                        match *left {
                            LogicalPlan::Scan { table_name, .. } => {
                                assert_eq!(table_name, "t2");
                            }
                            _ => panic!("Expected Scan"),
                        }

                        match *right {
                            LogicalPlan::Scan { table_name, .. } => {
                                assert_eq!(table_name, "t1");
                            }
                            _ => panic!("Expected Scan"),
                        }
                    }
                    _ => panic!("Expected Join"),
                },
                LogicalPlan::Join {
                    left,
                    right,
                    join_type,
                    ..
                } => {
                    assert_eq!(join_type, JoinType::Inner);

                    match *left {
                        LogicalPlan::Scan { table_name, .. } => {
                            assert_eq!(table_name, "t2");
                        }
                        _ => panic!("Expected Scan"),
                    }

                    match *right {
                        LogicalPlan::Scan { table_name, .. } => {
                            assert_eq!(table_name, "t1");
                        }
                        _ => panic!("Expected Scan"),
                    }
                }
                _ => panic!("Expected Join or Project"),
            }
        }

        #[test]
        fn reorder_three_tables_with_chain_edges() {
            let mut graph = JoinGraph::new();

            let schema1 = make_table_schema("t1", &[("a", DataType::Int64)]);
            let schema2 = make_table_schema("t2", &[("b", DataType::Int64)]);
            let schema3 = make_table_schema("t3", &[("c", DataType::Int64)]);

            graph.add_relation(make_relation("t1", schema1.clone(), 10000));
            graph.add_relation(make_relation("t2", schema2.clone(), 100));
            graph.add_relation(make_relation("t3", schema3.clone(), 10));

            let edge1 = JoinEdge {
                left_relation: 0,
                right_relation: 1,
                predicate: eq(col_table("t1", "a"), col_table("t2", "b")),
                selectivity_estimate: 0.1,
            };
            let edge2 = JoinEdge {
                left_relation: 1,
                right_relation: 2,
                predicate: eq(col_table("t2", "b"), col_table("t3", "c")),
                selectivity_estimate: 0.1,
            };
            graph.add_edge(edge1);
            graph.add_edge(edge2);

            let combined_schema = PlanSchema::from_fields(vec![
                PlanField::new("a", DataType::Int64).with_table("t1"),
                PlanField::new("b", DataType::Int64).with_table("t2"),
                PlanField::new("c", DataType::Int64).with_table("t3"),
            ]);

            let cost_model = CostModel::new();
            let reorderer = GreedyJoinReorderer::new(cost_model);

            let plan = reorderer.reorder(&graph, &combined_schema).unwrap();

            fn extract_scan_names(plan: &LogicalPlan) -> Vec<String> {
                match plan {
                    LogicalPlan::Join { left, right, .. } => {
                        let mut names = extract_scan_names(left);
                        names.extend(extract_scan_names(right));
                        names
                    }
                    LogicalPlan::Scan { table_name, .. } => vec![table_name.clone()],
                    LogicalPlan::Project { input, .. } => extract_scan_names(input),
                    _ => vec![],
                }
            }

            let scan_order = extract_scan_names(&plan);
            assert_eq!(scan_order.len(), 3);
            assert_eq!(scan_order[0], "t3");
        }

        #[test]
        fn reorder_original_order_no_projection() {
            let mut graph = JoinGraph::new();

            let schema1 = make_table_schema("t1", &[("a", DataType::Int64)]);
            let schema2 = make_table_schema("t2", &[("b", DataType::Int64)]);

            graph.add_relation(make_relation("t1", schema1.clone(), 10));
            graph.add_relation(make_relation("t2", schema2.clone(), 100));

            let edge = JoinEdge {
                left_relation: 0,
                right_relation: 1,
                predicate: eq(col_table("t1", "a"), col_table("t2", "b")),
                selectivity_estimate: 0.1,
            };
            graph.add_edge(edge);

            let combined_schema = PlanSchema::from_fields(vec![
                PlanField::new("a", DataType::Int64).with_table("t1"),
                PlanField::new("b", DataType::Int64).with_table("t2"),
            ]);

            let cost_model = CostModel::new();
            let reorderer = GreedyJoinReorderer::new(cost_model);

            let plan = reorderer.reorder(&graph, &combined_schema).unwrap();

            match &plan {
                LogicalPlan::Join { .. } => {}
                LogicalPlan::Project { .. } => panic!("Expected no projection for original order"),
                _ => panic!("Expected Join"),
            }
        }

        #[test]
        fn reorder_adds_schema_restoration_projection() {
            let (graph, schema) = make_graph_two_tables();
            let cost_model = CostModel::new();
            let reorderer = GreedyJoinReorderer::new(cost_model);

            let plan = reorderer.reorder(&graph, &schema).unwrap();

            match &plan {
                LogicalPlan::Project {
                    expressions,
                    schema: proj_schema,
                    ..
                } => {
                    assert_eq!(expressions.len(), 2);
                    assert_eq!(proj_schema.fields.len(), 2);
                }
                LogicalPlan::Join { .. } => {}
                _ => panic!("Expected Project or Join"),
            }
        }
    }

    mod predicate_collector_tests {
        use super::*;

        fn make_inner_join(
            left: LogicalPlan,
            right: LogicalPlan,
            condition: Option<Expr>,
        ) -> LogicalPlan {
            let mut fields = left.schema().fields.clone();
            fields.extend(right.schema().fields.clone());
            LogicalPlan::Join {
                left: Box::new(left),
                right: Box::new(right),
                join_type: JoinType::Inner,
                condition,
                schema: PlanSchema { fields },
            }
        }

        fn make_left_join(
            left: LogicalPlan,
            right: LogicalPlan,
            condition: Option<Expr>,
        ) -> LogicalPlan {
            let mut fields = left.schema().fields.clone();
            fields.extend(right.schema().fields.clone());
            LogicalPlan::Join {
                left: Box::new(left),
                right: Box::new(right),
                join_type: JoinType::Left,
                condition,
                schema: PlanSchema { fields },
            }
        }

        #[test]
        fn build_join_graph_two_table_inner_join() {
            let schema1 = make_table_schema("t1", &[("id", DataType::Int64)]);
            let schema2 = make_table_schema("t2", &[("id", DataType::Int64)]);

            let scan1 = make_scan("t1", schema1);
            let scan2 = make_scan("t2", schema2);

            let plan = make_inner_join(
                scan1,
                scan2,
                Some(eq(col_table("t1", "id"), col_table("t2", "id"))),
            );

            let cost_model = CostModel::new();
            let graph = PredicateCollector::build_join_graph(&plan, &cost_model);

            assert!(graph.is_some());
            let graph = graph.unwrap();

            assert_eq!(graph.relations().len(), 2);

            let edges = graph.get_edges_between(0, 1);
            assert_eq!(edges.len(), 1);
        }

        #[test]
        fn build_join_graph_three_table_inner_join() {
            let schema1 = make_table_schema("t1", &[("a", DataType::Int64)]);
            let schema2 = make_table_schema("t2", &[("b", DataType::Int64)]);
            let schema3 = make_table_schema("t3", &[("c", DataType::Int64)]);

            let scan1 = make_scan("t1", schema1);
            let scan2 = make_scan("t2", schema2);
            let scan3 = make_scan("t3", schema3);

            let join1 = make_inner_join(
                scan1,
                scan2,
                Some(eq(col_table("t1", "a"), col_table("t2", "b"))),
            );
            let plan = make_inner_join(
                join1,
                scan3,
                Some(eq(col_table("t2", "b"), col_table("t3", "c"))),
            );

            let cost_model = CostModel::new();
            let graph = PredicateCollector::build_join_graph(&plan, &cost_model);

            assert!(graph.is_some());
            let graph = graph.unwrap();

            assert_eq!(graph.relations().len(), 3);
        }

        #[test]
        fn build_join_graph_returns_none_for_left_join() {
            let schema1 = make_table_schema("t1", &[("id", DataType::Int64)]);
            let schema2 = make_table_schema("t2", &[("id", DataType::Int64)]);

            let scan1 = make_scan("t1", schema1);
            let scan2 = make_scan("t2", schema2);

            let plan = make_left_join(
                scan1,
                scan2,
                Some(eq(col_table("t1", "id"), col_table("t2", "id"))),
            );

            let cost_model = CostModel::new();
            let graph = PredicateCollector::build_join_graph(&plan, &cost_model);

            assert!(graph.is_none());
        }

        #[test]
        fn build_join_graph_returns_none_for_right_join() {
            let schema1 = make_table_schema("t1", &[("id", DataType::Int64)]);
            let schema2 = make_table_schema("t2", &[("id", DataType::Int64)]);

            let scan1 = make_scan("t1", schema1);
            let scan2 = make_scan("t2", schema2);

            let mut fields = scan1.schema().fields.clone();
            fields.extend(scan2.schema().fields.clone());
            let plan = LogicalPlan::Join {
                left: Box::new(scan1),
                right: Box::new(scan2),
                join_type: JoinType::Right,
                condition: Some(eq(col_table("t1", "id"), col_table("t2", "id"))),
                schema: PlanSchema { fields },
            };

            let cost_model = CostModel::new();
            let graph = PredicateCollector::build_join_graph(&plan, &cost_model);

            assert!(graph.is_none());
        }

        #[test]
        fn build_join_graph_returns_none_for_full_join() {
            let schema1 = make_table_schema("t1", &[("id", DataType::Int64)]);
            let schema2 = make_table_schema("t2", &[("id", DataType::Int64)]);

            let scan1 = make_scan("t1", schema1);
            let scan2 = make_scan("t2", schema2);

            let mut fields = scan1.schema().fields.clone();
            fields.extend(scan2.schema().fields.clone());
            let plan = LogicalPlan::Join {
                left: Box::new(scan1),
                right: Box::new(scan2),
                join_type: JoinType::Full,
                condition: Some(eq(col_table("t1", "id"), col_table("t2", "id"))),
                schema: PlanSchema { fields },
            };

            let cost_model = CostModel::new();
            let graph = PredicateCollector::build_join_graph(&plan, &cost_model);

            assert!(graph.is_none());
        }

        #[test]
        fn build_join_graph_returns_none_for_single_table() {
            let schema = make_table_schema("t1", &[("id", DataType::Int64)]);
            let scan = make_scan("t1", schema);

            let cost_model = CostModel::new();
            let graph = PredicateCollector::build_join_graph(&scan, &cost_model);

            assert!(graph.is_none());
        }

        #[test]
        fn build_join_graph_with_and_predicates() {
            let schema1 =
                make_table_schema("t1", &[("id", DataType::Int64), ("code", DataType::String)]);
            let schema2 =
                make_table_schema("t2", &[("id", DataType::Int64), ("code", DataType::String)]);

            let scan1 = make_scan("t1", schema1);
            let scan2 = make_scan("t2", schema2);

            let condition = and(
                eq(col_table("t1", "id"), col_table("t2", "id")),
                eq(col_table("t1", "code"), col_table("t2", "code")),
            );

            let plan = make_inner_join(scan1, scan2, Some(condition));

            let cost_model = CostModel::new();
            let graph = PredicateCollector::build_join_graph(&plan, &cost_model);

            assert!(graph.is_some());
            let graph = graph.unwrap();

            let edges = graph.get_edges_between(0, 1);
            assert_eq!(edges.len(), 2);
        }

        #[test]
        fn build_join_graph_with_filter_predicates() {
            let schema1 = make_table_schema("t1", &[("id", DataType::Int64)]);
            let schema2 = make_table_schema("t2", &[("id", DataType::Int64)]);

            let scan1 = make_scan("t1", schema1);
            let scan2 = make_scan("t2", schema2);

            let join = make_inner_join(scan1, scan2, None);
            let filter = LogicalPlan::Filter {
                input: Box::new(join),
                predicate: eq(col_table("t1", "id"), col_table("t2", "id")),
            };

            let cost_model = CostModel::new();
            let graph = PredicateCollector::build_join_graph(&filter, &cost_model);

            assert!(graph.is_some());
            let graph = graph.unwrap();

            let edges = graph.get_edges_between(0, 1);
            assert_eq!(edges.len(), 1);
        }

        #[test]
        fn build_join_graph_with_table_stats() {
            let schema1 = make_table_schema("t1", &[("id", DataType::Int64)]);
            let schema2 = make_table_schema("t2", &[("id", DataType::Int64)]);

            let scan1 = make_scan("t1", schema1);
            let scan2 = make_scan("t2", schema2);

            let plan = make_inner_join(
                scan1,
                scan2,
                Some(eq(col_table("t1", "id"), col_table("t2", "id"))),
            );

            let mut stats = FxHashMap::default();
            stats.insert("T1".to_string(), TableStats::new(5000));
            stats.insert("T2".to_string(), TableStats::new(500));

            let cost_model = CostModel::with_stats(stats);
            let graph = PredicateCollector::build_join_graph(&plan, &cost_model);

            assert!(graph.is_some());
            let graph = graph.unwrap();

            assert_eq!(graph.get_relation(0).unwrap().row_count_estimate, 5000);
            assert_eq!(graph.get_relation(1).unwrap().row_count_estimate, 500);
        }

        #[test]
        fn build_join_graph_returns_none_for_project() {
            let schema = make_table_schema("t1", &[("id", DataType::Int64)]);
            let scan = make_scan("t1", schema.clone());

            let plan = LogicalPlan::Project {
                input: Box::new(scan),
                expressions: vec![Expr::column("id")],
                schema,
            };

            let cost_model = CostModel::new();
            let graph = PredicateCollector::build_join_graph(&plan, &cost_model);

            assert!(graph.is_none());
        }

        #[test]
        fn build_join_graph_mixed_inner_and_outer_returns_none() {
            let schema1 = make_table_schema("t1", &[("a", DataType::Int64)]);
            let schema2 = make_table_schema("t2", &[("b", DataType::Int64)]);
            let schema3 = make_table_schema("t3", &[("c", DataType::Int64)]);

            let scan1 = make_scan("t1", schema1);
            let scan2 = make_scan("t2", schema2);
            let scan3 = make_scan("t3", schema3);

            let join1 = make_inner_join(
                scan1,
                scan2,
                Some(eq(col_table("t1", "a"), col_table("t2", "b"))),
            );
            let plan = make_left_join(
                join1,
                scan3,
                Some(eq(col_table("t2", "b"), col_table("t3", "c"))),
            );

            let cost_model = CostModel::new();
            let graph = PredicateCollector::build_join_graph(&plan, &cost_model);

            assert!(graph.is_none());
        }

        #[test]
        fn build_join_graph_cross_join() {
            let schema1 = make_table_schema("t1", &[("id", DataType::Int64)]);
            let schema2 = make_table_schema("t2", &[("id", DataType::Int64)]);

            let scan1 = make_scan("t1", schema1);
            let scan2 = make_scan("t2", schema2);

            let plan = make_inner_join(scan1, scan2, None);

            let cost_model = CostModel::new();
            let graph = PredicateCollector::build_join_graph(&plan, &cost_model);

            assert!(graph.is_some());
            let graph = graph.unwrap();

            assert_eq!(graph.relations().len(), 2);
            let edges = graph.get_edges_between(0, 1);
            assert!(edges.is_empty());
        }
    }
}

#[cfg(test)]
mod optimizer_tests {
    use yachtsql_common::types::DataType;
    use yachtsql_ir::{BinaryOp, Expr, JoinType, LogicalPlan, PlanField, PlanSchema, SortExpr};

    use crate::{PhysicalPlan, PhysicalPlanner};

    fn test_schema() -> PlanSchema {
        PlanSchema::from_fields(vec![
            PlanField::new("id", DataType::Int64),
            PlanField::new("name", DataType::String),
            PlanField::new("value", DataType::Float64),
        ])
    }

    fn users_schema() -> PlanSchema {
        PlanSchema::from_fields(vec![
            PlanField::new("id", DataType::Int64),
            PlanField::new("name", DataType::String),
        ])
    }

    fn orders_schema() -> PlanSchema {
        PlanSchema::from_fields(vec![
            PlanField::new("order_id", DataType::Int64),
            PlanField::new("user_id", DataType::Int64),
            PlanField::new("amount", DataType::Float64),
        ])
    }

    fn scan(name: &str) -> LogicalPlan {
        LogicalPlan::Scan {
            table_name: name.to_string(),
            schema: test_schema(),
            projection: None,
        }
    }

    fn scan_users() -> LogicalPlan {
        LogicalPlan::Scan {
            table_name: "users".to_string(),
            schema: users_schema(),
            projection: None,
        }
    }

    fn scan_orders() -> LogicalPlan {
        LogicalPlan::Scan {
            table_name: "orders".to_string(),
            schema: orders_schema(),
            projection: None,
        }
    }

    fn col(name: &str) -> Expr {
        Expr::column(name)
    }

    fn col_idx(name: &str, index: usize) -> Expr {
        Expr::Column {
            table: None,
            name: name.to_string(),
            index: Some(index),
        }
    }

    fn col_table_idx(table: &str, name: &str, index: usize) -> Expr {
        Expr::Column {
            table: Some(table.to_string()),
            name: name.to_string(),
            index: Some(index),
        }
    }

    fn lit_i64(v: i64) -> Expr {
        Expr::literal_i64(v)
    }

    fn eq(left: Expr, right: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOp::Eq,
            right: Box::new(right),
        }
    }

    fn and(left: Expr, right: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOp::And,
            right: Box::new(right),
        }
    }

    fn gt(left: Expr, right: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOp::Gt,
            right: Box::new(right),
        }
    }

    fn optimize(plan: &LogicalPlan) -> PhysicalPlan {
        PhysicalPlanner::new().plan(plan).unwrap()
    }

    mod topn_optimization {
        use super::*;

        #[test]
        fn sort_with_limit_becomes_topn() {
            let plan = LogicalPlan::Limit {
                input: Box::new(LogicalPlan::Sort {
                    input: Box::new(scan("users")),
                    sort_exprs: vec![SortExpr {
                        expr: col("id"),
                        asc: true,
                        nulls_first: false,
                    }],
                }),
                limit: Some(10),
                offset: None,
            };

            let optimized = optimize(&plan);

            match optimized {
                PhysicalPlan::TopN {
                    sort_exprs, limit, ..
                } => {
                    assert_eq!(limit, 10);
                    assert_eq!(sort_exprs.len(), 1);
                    assert!(sort_exprs[0].asc);
                }
                other => panic!("Expected TopN, got {:?}", other),
            }
        }

        #[test]
        fn sort_with_limit_and_offset_stays_separate() {
            let plan = LogicalPlan::Limit {
                input: Box::new(LogicalPlan::Sort {
                    input: Box::new(scan("users")),
                    sort_exprs: vec![SortExpr {
                        expr: col("id"),
                        asc: true,
                        nulls_first: false,
                    }],
                }),
                limit: Some(10),
                offset: Some(5),
            };

            let optimized = optimize(&plan);

            match optimized {
                PhysicalPlan::Limit { offset, .. } => {
                    assert_eq!(offset, Some(5));
                }
                other => panic!("Expected Limit (not TopN due to offset), got {:?}", other),
            }
        }

        #[test]
        fn sort_without_limit_stays_sort() {
            let plan = LogicalPlan::Sort {
                input: Box::new(scan("users")),
                sort_exprs: vec![SortExpr {
                    expr: col("id"),
                    asc: false,
                    nulls_first: true,
                }],
            };

            let optimized = optimize(&plan);

            match optimized {
                PhysicalPlan::Sort { sort_exprs, .. } => {
                    assert_eq!(sort_exprs.len(), 1);
                    assert!(!sort_exprs[0].asc);
                }
                other => panic!("Expected Sort, got {:?}", other),
            }
        }

        #[test]
        fn limit_without_sort_stays_limit() {
            let plan = LogicalPlan::Limit {
                input: Box::new(scan("users")),
                limit: Some(10),
                offset: None,
            };

            let optimized = optimize(&plan);

            match optimized {
                PhysicalPlan::Limit { limit, offset, .. } => {
                    assert_eq!(limit, Some(10));
                    assert_eq!(offset, None);
                }
                other => panic!("Expected Limit, got {:?}", other),
            }
        }

        #[test]
        fn limit_none_with_sort_stays_separate() {
            let plan = LogicalPlan::Limit {
                input: Box::new(LogicalPlan::Sort {
                    input: Box::new(scan("users")),
                    sort_exprs: vec![SortExpr {
                        expr: col("id"),
                        asc: true,
                        nulls_first: false,
                    }],
                }),
                limit: None,
                offset: Some(5),
            };

            let optimized = optimize(&plan);

            match optimized {
                PhysicalPlan::Limit { limit, offset, .. } => {
                    assert_eq!(limit, None);
                    assert_eq!(offset, Some(5));
                }
                other => panic!("Expected Limit, got {:?}", other),
            }
        }

        #[test]
        fn topn_with_filter() {
            let plan = LogicalPlan::Limit {
                input: Box::new(LogicalPlan::Sort {
                    input: Box::new(LogicalPlan::Filter {
                        input: Box::new(scan("users")),
                        predicate: eq(col("id"), lit_i64(1)),
                    }),
                    sort_exprs: vec![SortExpr {
                        expr: col("value"),
                        asc: false,
                        nulls_first: false,
                    }],
                }),
                limit: Some(5),
                offset: None,
            };

            let optimized = optimize(&plan);

            match optimized {
                PhysicalPlan::TopN { input, limit, .. } => {
                    assert_eq!(limit, 5);
                    match input.as_ref() {
                        PhysicalPlan::Filter { .. } => {}
                        _ => panic!("Expected Filter under TopN"),
                    }
                }
                other => panic!("Expected TopN at top, got {:?}", other),
            }
        }
    }

    mod hash_join_optimization {
        use super::*;

        fn joined_schema() -> PlanSchema {
            PlanSchema::from_fields(vec![
                PlanField::new("id", DataType::Int64),
                PlanField::new("name", DataType::String),
                PlanField::new("order_id", DataType::Int64),
                PlanField::new("user_id", DataType::Int64),
                PlanField::new("amount", DataType::Float64),
            ])
        }

        #[test]
        fn inner_join_with_equi_condition_becomes_hash_join() {
            let plan = LogicalPlan::Join {
                left: Box::new(scan_users()),
                right: Box::new(scan_orders()),
                join_type: JoinType::Inner,
                condition: Some(eq(col_idx("id", 0), col_idx("user_id", 3))),
                schema: joined_schema(),
            };

            let optimized = optimize(&plan);

            match optimized {
                PhysicalPlan::HashJoin {
                    join_type,
                    left_keys,
                    right_keys,
                    ..
                } => {
                    assert_eq!(join_type, JoinType::Inner);
                    assert_eq!(left_keys.len(), 1);
                    assert_eq!(right_keys.len(), 1);
                    match &left_keys[0] {
                        Expr::Column { name, index, .. } => {
                            assert_eq!(name, "id");
                            assert_eq!(*index, Some(0));
                        }
                        _ => panic!("Expected column expression in left_keys"),
                    }
                    match &right_keys[0] {
                        Expr::Column { name, index, .. } => {
                            assert_eq!(name, "user_id");
                            assert_eq!(*index, Some(1));
                        }
                        _ => panic!("Expected column expression in right_keys"),
                    }
                }
                other => panic!("Expected HashJoin, got {:?}", other),
            }
        }

        #[test]
        fn inner_join_with_multiple_equi_keys_becomes_hash_join() {
            let multi_key_schema = PlanSchema::from_fields(vec![
                PlanField::new("a", DataType::Int64),
                PlanField::new("b", DataType::Int64),
                PlanField::new("c", DataType::Int64),
                PlanField::new("d", DataType::Int64),
            ]);

            let left_scan = LogicalPlan::Scan {
                table_name: "t1".to_string(),
                schema: PlanSchema::from_fields(vec![
                    PlanField::new("a", DataType::Int64),
                    PlanField::new("b", DataType::Int64),
                ]),
                projection: None,
            };

            let right_scan = LogicalPlan::Scan {
                table_name: "t2".to_string(),
                schema: PlanSchema::from_fields(vec![
                    PlanField::new("c", DataType::Int64),
                    PlanField::new("d", DataType::Int64),
                ]),
                projection: None,
            };

            let plan = LogicalPlan::Join {
                left: Box::new(left_scan),
                right: Box::new(right_scan),
                join_type: JoinType::Inner,
                condition: Some(and(
                    eq(col_idx("a", 0), col_idx("c", 2)),
                    eq(col_idx("b", 1), col_idx("d", 3)),
                )),
                schema: multi_key_schema,
            };

            let optimized = optimize(&plan);

            match optimized {
                PhysicalPlan::HashJoin {
                    left_keys,
                    right_keys,
                    ..
                } => {
                    assert_eq!(left_keys.len(), 2);
                    assert_eq!(right_keys.len(), 2);
                }
                other => panic!("Expected HashJoin with 2 keys, got {:?}", other),
            }
        }

        #[test]
        fn inner_join_without_condition_uses_nested_loop() {
            let plan = LogicalPlan::Join {
                left: Box::new(scan_users()),
                right: Box::new(scan_orders()),
                join_type: JoinType::Inner,
                condition: None,
                schema: joined_schema(),
            };

            let optimized = optimize(&plan);

            match optimized {
                PhysicalPlan::NestedLoopJoin { join_type, .. } => {
                    assert_eq!(join_type, JoinType::Inner);
                }
                other => panic!("Expected NestedLoopJoin (no condition), got {:?}", other),
            }
        }

        #[test]
        fn inner_join_with_non_equi_condition_uses_nested_loop() {
            let plan = LogicalPlan::Join {
                left: Box::new(scan_users()),
                right: Box::new(scan_orders()),
                join_type: JoinType::Inner,
                condition: Some(gt(col_idx("id", 0), col_idx("user_id", 3))),
                schema: joined_schema(),
            };

            let optimized = optimize(&plan);

            match optimized {
                PhysicalPlan::NestedLoopJoin { .. } => {}
                other => panic!("Expected NestedLoopJoin (non-equi), got {:?}", other),
            }
        }

        #[test]
        fn left_join_with_equi_condition_uses_hash_join() {
            let plan = LogicalPlan::Join {
                left: Box::new(scan_users()),
                right: Box::new(scan_orders()),
                join_type: JoinType::Left,
                condition: Some(eq(col_idx("id", 0), col_idx("user_id", 3))),
                schema: joined_schema(),
            };

            let optimized = optimize(&plan);

            match optimized {
                PhysicalPlan::HashJoin { join_type, .. } => {
                    assert_eq!(join_type, JoinType::Left);
                }
                other => panic!("Expected HashJoin (left join), got {:?}", other),
            }
        }

        #[test]
        fn right_join_with_equi_condition_uses_hash_join() {
            let plan = LogicalPlan::Join {
                left: Box::new(scan_users()),
                right: Box::new(scan_orders()),
                join_type: JoinType::Right,
                condition: Some(eq(col_idx("id", 0), col_idx("user_id", 3))),
                schema: joined_schema(),
            };

            let optimized = optimize(&plan);

            match optimized {
                PhysicalPlan::HashJoin { join_type, .. } => {
                    assert_eq!(join_type, JoinType::Right);
                }
                other => panic!("Expected HashJoin (right join), got {:?}", other),
            }
        }

        #[test]
        fn full_join_with_equi_condition_uses_hash_join() {
            let plan = LogicalPlan::Join {
                left: Box::new(scan_users()),
                right: Box::new(scan_orders()),
                join_type: JoinType::Full,
                condition: Some(eq(col_idx("id", 0), col_idx("user_id", 3))),
                schema: joined_schema(),
            };

            let optimized = optimize(&plan);

            match optimized {
                PhysicalPlan::HashJoin { join_type, .. } => {
                    assert_eq!(join_type, JoinType::Full);
                }
                other => panic!("Expected HashJoin (full join), got {:?}", other),
            }
        }

        #[test]
        fn left_join_with_non_equi_condition_uses_nested_loop() {
            let plan = LogicalPlan::Join {
                left: Box::new(scan_users()),
                right: Box::new(scan_orders()),
                join_type: JoinType::Left,
                condition: Some(gt(col_idx("id", 0), col_idx("user_id", 3))),
                schema: joined_schema(),
            };

            let optimized = optimize(&plan);

            match optimized {
                PhysicalPlan::NestedLoopJoin { join_type, .. } => {
                    assert_eq!(join_type, JoinType::Left);
                }
                other => panic!(
                    "Expected NestedLoopJoin (left join non-equi), got {:?}",
                    other
                ),
            }
        }

        #[test]
        fn right_join_with_non_equi_condition_uses_nested_loop() {
            let plan = LogicalPlan::Join {
                left: Box::new(scan_users()),
                right: Box::new(scan_orders()),
                join_type: JoinType::Right,
                condition: Some(gt(col_idx("id", 0), col_idx("user_id", 3))),
                schema: joined_schema(),
            };

            let optimized = optimize(&plan);

            match optimized {
                PhysicalPlan::NestedLoopJoin { join_type, .. } => {
                    assert_eq!(join_type, JoinType::Right);
                }
                other => panic!(
                    "Expected NestedLoopJoin (right join non-equi), got {:?}",
                    other
                ),
            }
        }

        #[test]
        fn full_join_with_non_equi_condition_uses_nested_loop() {
            let plan = LogicalPlan::Join {
                left: Box::new(scan_users()),
                right: Box::new(scan_orders()),
                join_type: JoinType::Full,
                condition: Some(gt(col_idx("id", 0), col_idx("user_id", 3))),
                schema: joined_schema(),
            };

            let optimized = optimize(&plan);

            match optimized {
                PhysicalPlan::NestedLoopJoin { join_type, .. } => {
                    assert_eq!(join_type, JoinType::Full);
                }
                other => panic!(
                    "Expected NestedLoopJoin (full join non-equi), got {:?}",
                    other
                ),
            }
        }

        #[test]
        fn cross_join_uses_cross_join() {
            let plan = LogicalPlan::Join {
                left: Box::new(scan_users()),
                right: Box::new(scan_orders()),
                join_type: JoinType::Cross,
                condition: None,
                schema: joined_schema(),
            };

            let optimized = optimize(&plan);

            match optimized {
                PhysicalPlan::CrossJoin { .. } => {}
                other => panic!("Expected CrossJoin, got {:?}", other),
            }
        }

        #[test]
        fn hash_join_into_logical_restores_right_indices() {
            let plan = LogicalPlan::Join {
                left: Box::new(scan_users()),
                right: Box::new(scan_orders()),
                join_type: JoinType::Inner,
                condition: Some(eq(col_idx("id", 0), col_idx("user_id", 3))),
                schema: joined_schema(),
            };

            let optimized = optimize(&plan);

            match &optimized {
                PhysicalPlan::HashJoin { .. } => {}
                other => panic!("Expected HashJoin, got {:?}", other),
            }

            let back_to_logical = optimized.into_logical();

            match back_to_logical {
                LogicalPlan::Join { condition, .. } => {
                    let cond = condition.expect("should have condition");
                    match cond {
                        Expr::BinaryOp { left, right, .. } => {
                            match left.as_ref() {
                                Expr::Column { index, .. } => {
                                    assert_eq!(*index, Some(0), "left key should be index 0");
                                }
                                _ => panic!("Expected column"),
                            }
                            match right.as_ref() {
                                Expr::Column { index, .. } => {
                                    assert_eq!(
                                        *index,
                                        Some(3),
                                        "right key should be restored to index 3"
                                    );
                                }
                                _ => panic!("Expected column"),
                            }
                        }
                        _ => panic!("Expected BinaryOp"),
                    }
                }
                _ => panic!("Expected Join"),
            }
        }

        #[test]
        fn hash_join_with_reversed_condition_order() {
            let plan = LogicalPlan::Join {
                left: Box::new(scan_users()),
                right: Box::new(scan_orders()),
                join_type: JoinType::Inner,
                condition: Some(eq(col_idx("user_id", 3), col_idx("id", 0))),
                schema: joined_schema(),
            };

            let optimized = optimize(&plan);

            match optimized {
                PhysicalPlan::HashJoin {
                    left_keys,
                    right_keys,
                    ..
                } => {
                    assert_eq!(left_keys.len(), 1);
                    assert_eq!(right_keys.len(), 1);
                    match &left_keys[0] {
                        Expr::Column { name, index, .. } => {
                            assert_eq!(name, "id");
                            assert_eq!(*index, Some(0));
                        }
                        _ => panic!("Expected left key to be 'id'"),
                    }
                    match &right_keys[0] {
                        Expr::Column { name, index, .. } => {
                            assert_eq!(name, "user_id");
                            assert_eq!(*index, Some(1));
                        }
                        _ => panic!("Expected right key to be 'user_id'"),
                    }
                }
                other => panic!(
                    "Expected HashJoin even with reversed condition, got {:?}",
                    other
                ),
            }
        }

        #[test]
        fn hash_join_with_table_qualified_columns() {
            let plan = LogicalPlan::Join {
                left: Box::new(scan_users()),
                right: Box::new(scan_orders()),
                join_type: JoinType::Inner,
                condition: Some(eq(
                    col_table_idx("users", "id", 0),
                    col_table_idx("orders", "user_id", 3),
                )),
                schema: joined_schema(),
            };

            let optimized = optimize(&plan);

            match optimized {
                PhysicalPlan::HashJoin { .. } => {}
                other => panic!("Expected HashJoin with qualified columns, got {:?}", other),
            }
        }

        #[test]
        fn nested_hash_joins() {
            let products_schema = PlanSchema::from_fields(vec![
                PlanField::new("product_id", DataType::Int64),
                PlanField::new("product_name", DataType::String),
            ]);

            let products_scan = LogicalPlan::Scan {
                table_name: "products".to_string(),
                schema: products_schema,
                projection: None,
            };

            let order_items_schema = PlanSchema::from_fields(vec![
                PlanField::new("item_id", DataType::Int64),
                PlanField::new("order_id", DataType::Int64),
                PlanField::new("product_id", DataType::Int64),
            ]);

            let order_items_scan = LogicalPlan::Scan {
                table_name: "order_items".to_string(),
                schema: order_items_schema,
                projection: None,
            };

            let first_join_schema = PlanSchema::from_fields(vec![
                PlanField::new("order_id", DataType::Int64),
                PlanField::new("user_id", DataType::Int64),
                PlanField::new("amount", DataType::Float64),
                PlanField::new("item_id", DataType::Int64),
                PlanField::new("order_id", DataType::Int64),
                PlanField::new("product_id", DataType::Int64),
            ]);

            let first_join = LogicalPlan::Join {
                left: Box::new(scan_orders()),
                right: Box::new(order_items_scan),
                join_type: JoinType::Inner,
                condition: Some(eq(col_idx("order_id", 0), col_idx("order_id", 4))),
                schema: first_join_schema.clone(),
            };

            let final_schema = PlanSchema::from_fields(vec![
                PlanField::new("order_id", DataType::Int64),
                PlanField::new("user_id", DataType::Int64),
                PlanField::new("amount", DataType::Float64),
                PlanField::new("item_id", DataType::Int64),
                PlanField::new("order_id", DataType::Int64),
                PlanField::new("product_id", DataType::Int64),
                PlanField::new("product_id", DataType::Int64),
                PlanField::new("product_name", DataType::String),
            ]);

            let second_join = LogicalPlan::Join {
                left: Box::new(first_join),
                right: Box::new(products_scan),
                join_type: JoinType::Inner,
                condition: Some(eq(col_idx("product_id", 5), col_idx("product_id", 6))),
                schema: final_schema,
            };

            let optimized = optimize(&second_join);

            match &optimized {
                PhysicalPlan::HashJoin { left, .. } => match left.as_ref() {
                    PhysicalPlan::HashJoin { .. } => {}
                    other => panic!("Expected nested HashJoin, got {:?}", other),
                },
                other => panic!("Expected outer HashJoin, got {:?}", other),
            }
        }
    }
}

#[cfg(test)]
mod projection_pushdown_tests {
    use yachtsql_common::types::DataType;
    use yachtsql_ir::{
        AggregateFunction, BinaryOp, Expr, JoinType, PlanField, PlanSchema, SortExpr,
        WindowFunction,
    };

    use crate::planner::ProjectionPushdown;
    use crate::{ExecutionHints, PhysicalPlan};

    fn make_schema(fields: &[(&str, DataType)]) -> PlanSchema {
        PlanSchema::from_fields(
            fields
                .iter()
                .map(|(name, dt)| PlanField::new(*name, dt.clone()))
                .collect(),
        )
    }

    fn col_idx(name: &str, index: usize) -> Expr {
        Expr::Column {
            table: None,
            name: name.to_string(),
            index: Some(index),
        }
    }

    fn lit_i64(v: i64) -> Expr {
        Expr::literal_i64(v)
    }

    fn eq(left: Expr, right: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOp::Eq,
            right: Box::new(right),
        }
    }

    fn gt(left: Expr, right: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOp::Gt,
            right: Box::new(right),
        }
    }

    fn add(left: Expr, right: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOp::Add,
            right: Box::new(right),
        }
    }

    mod table_scan_tests {
        use super::*;

        #[test]
        fn pushdown_all_columns_keeps_none_projection() {
            let schema = make_schema(&[
                ("id", DataType::Int64),
                ("name", DataType::String),
                ("value", DataType::Float64),
            ]);

            let plan = PhysicalPlan::TableScan {
                table_name: "test".to_string(),
                schema: schema.clone(),
                projection: None,
                row_count: None,
            };

            let optimized = ProjectionPushdown::optimize(plan);

            match optimized {
                PhysicalPlan::TableScan { projection, .. } => {
                    assert_eq!(projection, None);
                }
                other => panic!("Expected TableScan, got {:?}", other),
            }
        }

        #[test]
        fn pushdown_with_existing_projection_unchanged_when_all_columns_required() {
            let schema = make_schema(&[
                ("id", DataType::Int64),
                ("name", DataType::String),
                ("value", DataType::Float64),
            ]);

            let plan = PhysicalPlan::TableScan {
                table_name: "test".to_string(),
                schema: schema.clone(),
                projection: Some(vec![0, 1]),
                row_count: None,
            };

            let optimized = ProjectionPushdown::optimize(plan);

            match optimized {
                PhysicalPlan::TableScan { projection, .. } => {
                    assert_eq!(projection, Some(vec![0, 1]));
                }
                other => panic!("Expected TableScan, got {:?}", other),
            }
        }
    }

    mod filter_tests {
        use super::*;

        #[test]
        fn filter_adds_predicate_columns_to_required() {
            let schema = make_schema(&[
                ("id", DataType::Int64),
                ("name", DataType::String),
                ("value", DataType::Float64),
            ]);

            let scan = PhysicalPlan::TableScan {
                table_name: "test".to_string(),
                schema: schema.clone(),
                projection: None,
                row_count: None,
            };

            let filter = PhysicalPlan::Filter {
                input: Box::new(scan),
                predicate: eq(col_idx("id", 0), lit_i64(1)),
            };

            let project_schema = make_schema(&[("name", DataType::String)]);
            let project = PhysicalPlan::Project {
                input: Box::new(filter),
                expressions: vec![col_idx("name", 1)],
                schema: project_schema,
            };

            let optimized = ProjectionPushdown::optimize(project);

            match optimized {
                PhysicalPlan::Project { input, .. } => match input.as_ref() {
                    PhysicalPlan::Filter { input, .. } => match input.as_ref() {
                        PhysicalPlan::TableScan { projection, .. } => {
                            let proj = projection.as_ref().expect("Expected projection");
                            assert!(proj.contains(&0));
                            assert!(proj.contains(&1));
                        }
                        other => panic!("Expected TableScan, got {:?}", other),
                    },
                    other => panic!("Expected Filter, got {:?}", other),
                },
                other => panic!("Expected Project, got {:?}", other),
            }
        }

        #[test]
        fn filter_with_multiple_columns_in_predicate() {
            let schema = make_schema(&[
                ("a", DataType::Int64),
                ("b", DataType::Int64),
                ("c", DataType::Int64),
                ("d", DataType::Int64),
                ("e", DataType::Int64),
            ]);

            let scan = PhysicalPlan::TableScan {
                table_name: "test".to_string(),
                schema: schema.clone(),
                projection: None,
                row_count: None,
            };

            let predicate = Expr::BinaryOp {
                left: Box::new(eq(col_idx("a", 0), col_idx("b", 1))),
                op: BinaryOp::And,
                right: Box::new(gt(col_idx("c", 2), lit_i64(10))),
            };

            let filter = PhysicalPlan::Filter {
                input: Box::new(scan),
                predicate,
            };

            let project_schema = make_schema(&[("d", DataType::Int64)]);
            let project = PhysicalPlan::Project {
                input: Box::new(filter),
                expressions: vec![col_idx("d", 3)],
                schema: project_schema,
            };

            let optimized = ProjectionPushdown::optimize(project);

            match optimized {
                PhysicalPlan::Project { input, .. } => match input.as_ref() {
                    PhysicalPlan::Filter { input, .. } => match input.as_ref() {
                        PhysicalPlan::TableScan { projection, .. } => {
                            let proj = projection.as_ref().expect("Expected projection");
                            assert!(proj.contains(&0));
                            assert!(proj.contains(&1));
                            assert!(proj.contains(&2));
                            assert!(proj.contains(&3));
                            assert!(!proj.contains(&4));
                        }
                        other => panic!("Expected TableScan, got {:?}", other),
                    },
                    other => panic!("Expected Filter, got {:?}", other),
                },
                other => panic!("Expected Project, got {:?}", other),
            }
        }
    }

    mod project_tests {
        use super::*;

        #[test]
        fn project_only_requires_columns_from_needed_expressions() {
            let schema = make_schema(&[
                ("a", DataType::Int64),
                ("b", DataType::Int64),
                ("c", DataType::Int64),
            ]);

            let scan = PhysicalPlan::TableScan {
                table_name: "test".to_string(),
                schema: schema.clone(),
                projection: None,
                row_count: None,
            };

            let inner_project_schema = make_schema(&[
                ("x", DataType::Int64),
                ("y", DataType::Int64),
                ("z", DataType::Int64),
            ]);
            let inner_project = PhysicalPlan::Project {
                input: Box::new(scan),
                expressions: vec![col_idx("a", 0), col_idx("b", 1), col_idx("c", 2)],
                schema: inner_project_schema,
            };

            let outer_project_schema = make_schema(&[("x", DataType::Int64)]);
            let outer_project = PhysicalPlan::Project {
                input: Box::new(inner_project),
                expressions: vec![col_idx("x", 0)],
                schema: outer_project_schema,
            };

            let optimized = ProjectionPushdown::optimize(outer_project);

            match optimized {
                PhysicalPlan::Project { input, .. } => match input.as_ref() {
                    PhysicalPlan::Project { input, .. } => match input.as_ref() {
                        PhysicalPlan::TableScan { projection, .. } => {
                            let proj = projection.as_ref().expect("Expected projection");
                            assert_eq!(proj, &vec![0]);
                        }
                        other => panic!("Expected TableScan, got {:?}", other),
                    },
                    other => panic!("Expected Project, got {:?}", other),
                },
                other => panic!("Expected Project, got {:?}", other),
            }
        }

        #[test]
        fn project_with_expression_using_multiple_columns() {
            let schema = make_schema(&[
                ("a", DataType::Int64),
                ("b", DataType::Int64),
                ("c", DataType::Int64),
            ]);

            let scan = PhysicalPlan::TableScan {
                table_name: "test".to_string(),
                schema: schema.clone(),
                projection: None,
                row_count: None,
            };

            let project_schema = make_schema(&[("sum", DataType::Int64)]);
            let project = PhysicalPlan::Project {
                input: Box::new(scan),
                expressions: vec![add(col_idx("a", 0), col_idx("b", 1))],
                schema: project_schema,
            };

            let optimized = ProjectionPushdown::optimize(project);

            match optimized {
                PhysicalPlan::Project { input, .. } => match input.as_ref() {
                    PhysicalPlan::TableScan { projection, .. } => {
                        let proj = projection.as_ref().expect("Expected projection");
                        assert!(proj.contains(&0));
                        assert!(proj.contains(&1));
                        assert!(!proj.contains(&2));
                    }
                    other => panic!("Expected TableScan, got {:?}", other),
                },
                other => panic!("Expected Project, got {:?}", other),
            }
        }

        #[test]
        fn project_skips_unrequired_output_columns() {
            let schema = make_schema(&[
                ("a", DataType::Int64),
                ("b", DataType::Int64),
                ("c", DataType::Int64),
            ]);

            let scan = PhysicalPlan::TableScan {
                table_name: "test".to_string(),
                schema: schema.clone(),
                projection: None,
                row_count: None,
            };

            let inner_project_schema =
                make_schema(&[("x", DataType::Int64), ("y", DataType::Int64)]);
            let inner_project = PhysicalPlan::Project {
                input: Box::new(scan),
                expressions: vec![col_idx("a", 0), col_idx("c", 2)],
                schema: inner_project_schema,
            };

            let outer_project_schema = make_schema(&[("y", DataType::Int64)]);
            let outer_project = PhysicalPlan::Project {
                input: Box::new(inner_project),
                expressions: vec![col_idx("y", 1)],
                schema: outer_project_schema,
            };

            let optimized = ProjectionPushdown::optimize(outer_project);

            match optimized {
                PhysicalPlan::Project { input, .. } => match input.as_ref() {
                    PhysicalPlan::Project { input, .. } => match input.as_ref() {
                        PhysicalPlan::TableScan { projection, .. } => {
                            let proj = projection.as_ref().expect("Expected projection");
                            assert!(proj.contains(&2));
                        }
                        other => panic!("Expected TableScan, got {:?}", other),
                    },
                    other => panic!("Expected Project, got {:?}", other),
                },
                other => panic!("Expected Project, got {:?}", other),
            }
        }
    }

    mod hash_join_tests {
        use super::*;

        fn users_schema() -> PlanSchema {
            make_schema(&[
                ("id", DataType::Int64),
                ("name", DataType::String),
                ("email", DataType::String),
            ])
        }

        fn orders_schema() -> PlanSchema {
            make_schema(&[
                ("order_id", DataType::Int64),
                ("user_id", DataType::Int64),
                ("amount", DataType::Float64),
                ("status", DataType::String),
            ])
        }

        fn joined_schema() -> PlanSchema {
            make_schema(&[
                ("id", DataType::Int64),
                ("name", DataType::String),
                ("email", DataType::String),
                ("order_id", DataType::Int64),
                ("user_id", DataType::Int64),
                ("amount", DataType::Float64),
                ("status", DataType::String),
            ])
        }

        #[test]
        fn hash_join_splits_required_columns_between_left_and_right() {
            let left = PhysicalPlan::TableScan {
                table_name: "users".to_string(),
                schema: users_schema(),
                projection: None,
                row_count: None,
            };

            let right = PhysicalPlan::TableScan {
                table_name: "orders".to_string(),
                schema: orders_schema(),
                projection: None,
                row_count: None,
            };

            let join = PhysicalPlan::HashJoin {
                left: Box::new(left),
                right: Box::new(right),
                join_type: JoinType::Inner,
                left_keys: vec![col_idx("id", 0)],
                right_keys: vec![col_idx("user_id", 1)],
                schema: joined_schema(),
                parallel: false,
                hints: ExecutionHints::default(),
            };

            let project_schema =
                make_schema(&[("name", DataType::String), ("amount", DataType::Float64)]);
            let project = PhysicalPlan::Project {
                input: Box::new(join),
                expressions: vec![col_idx("name", 1), col_idx("amount", 5)],
                schema: project_schema,
            };

            let optimized = ProjectionPushdown::optimize(project);

            match optimized {
                PhysicalPlan::Project { input, .. } => match input.as_ref() {
                    PhysicalPlan::HashJoin { left, right, .. } => {
                        match left.as_ref() {
                            PhysicalPlan::TableScan { projection, .. } => {
                                let proj = projection.as_ref().expect("Expected projection");
                                assert!(proj.contains(&0));
                                assert!(proj.contains(&1));
                                assert!(!proj.contains(&2));
                            }
                            other => panic!("Expected left TableScan, got {:?}", other),
                        }
                        match right.as_ref() {
                            PhysicalPlan::TableScan { projection, .. } => {
                                let proj = projection.as_ref().expect("Expected projection");
                                assert!(proj.contains(&1));
                                assert!(proj.contains(&2));
                                assert!(!proj.contains(&0));
                                assert!(!proj.contains(&3));
                            }
                            other => panic!("Expected right TableScan, got {:?}", other),
                        }
                    }
                    other => panic!("Expected HashJoin, got {:?}", other),
                },
                other => panic!("Expected Project, got {:?}", other),
            }
        }

        #[test]
        fn hash_join_includes_key_columns() {
            let left = PhysicalPlan::TableScan {
                table_name: "users".to_string(),
                schema: users_schema(),
                projection: None,
                row_count: None,
            };

            let right = PhysicalPlan::TableScan {
                table_name: "orders".to_string(),
                schema: orders_schema(),
                projection: None,
                row_count: None,
            };

            let join = PhysicalPlan::HashJoin {
                left: Box::new(left),
                right: Box::new(right),
                join_type: JoinType::Inner,
                left_keys: vec![col_idx("id", 0)],
                right_keys: vec![col_idx("user_id", 1)],
                schema: joined_schema(),
                parallel: false,
                hints: ExecutionHints::default(),
            };

            let project_schema = make_schema(&[("name", DataType::String)]);
            let project = PhysicalPlan::Project {
                input: Box::new(join),
                expressions: vec![col_idx("name", 1)],
                schema: project_schema,
            };

            let optimized = ProjectionPushdown::optimize(project);

            match optimized {
                PhysicalPlan::Project { input, .. } => match input.as_ref() {
                    PhysicalPlan::HashJoin { left, right, .. } => {
                        match left.as_ref() {
                            PhysicalPlan::TableScan { projection, .. } => {
                                let proj = projection.as_ref().expect("Expected projection");
                                assert!(proj.contains(&0));
                                assert!(proj.contains(&1));
                                assert!(!proj.contains(&2));
                            }
                            other => panic!("Expected left TableScan, got {:?}", other),
                        }
                        match right.as_ref() {
                            PhysicalPlan::TableScan { projection, .. } => {
                                let proj = projection.as_ref().expect("Expected projection");
                                assert!(proj.contains(&1));
                                assert!(!proj.contains(&0));
                                assert!(!proj.contains(&2));
                                assert!(!proj.contains(&3));
                            }
                            other => panic!("Expected right TableScan, got {:?}", other),
                        }
                    }
                    other => panic!("Expected HashJoin, got {:?}", other),
                },
                other => panic!("Expected Project, got {:?}", other),
            }
        }
    }

    mod cross_join_tests {
        use super::*;

        #[test]
        fn cross_join_splits_required_columns() {
            let left_schema = make_schema(&[("a", DataType::Int64), ("b", DataType::Int64)]);
            let right_schema = make_schema(&[("c", DataType::Int64), ("d", DataType::Int64)]);
            let joined_schema = make_schema(&[
                ("a", DataType::Int64),
                ("b", DataType::Int64),
                ("c", DataType::Int64),
                ("d", DataType::Int64),
            ]);

            let left = PhysicalPlan::TableScan {
                table_name: "t1".to_string(),
                schema: left_schema.clone(),
                projection: None,
                row_count: None,
            };

            let right = PhysicalPlan::TableScan {
                table_name: "t2".to_string(),
                schema: right_schema.clone(),
                projection: None,
                row_count: None,
            };

            let cross_join = PhysicalPlan::CrossJoin {
                left: Box::new(left),
                right: Box::new(right),
                schema: joined_schema.clone(),
                parallel: false,
                hints: ExecutionHints::default(),
            };

            let project_schema = make_schema(&[("a", DataType::Int64), ("d", DataType::Int64)]);
            let project = PhysicalPlan::Project {
                input: Box::new(cross_join),
                expressions: vec![col_idx("a", 0), col_idx("d", 3)],
                schema: project_schema,
            };

            let optimized = ProjectionPushdown::optimize(project);

            match optimized {
                PhysicalPlan::Project { input, .. } => match input.as_ref() {
                    PhysicalPlan::CrossJoin { left, right, .. } => {
                        match left.as_ref() {
                            PhysicalPlan::TableScan { projection, .. } => {
                                let proj = projection.as_ref().expect("Expected projection");
                                assert_eq!(proj, &vec![0]);
                            }
                            other => panic!("Expected left TableScan, got {:?}", other),
                        }
                        match right.as_ref() {
                            PhysicalPlan::TableScan { projection, .. } => {
                                let proj = projection.as_ref().expect("Expected projection");
                                assert_eq!(proj, &vec![1]);
                            }
                            other => panic!("Expected right TableScan, got {:?}", other),
                        }
                    }
                    other => panic!("Expected CrossJoin, got {:?}", other),
                },
                other => panic!("Expected Project, got {:?}", other),
            }
        }

        #[test]
        fn cross_join_with_all_columns_from_left() {
            let left_schema = make_schema(&[("a", DataType::Int64), ("b", DataType::Int64)]);
            let right_schema = make_schema(&[("c", DataType::Int64), ("d", DataType::Int64)]);
            let joined_schema = make_schema(&[
                ("a", DataType::Int64),
                ("b", DataType::Int64),
                ("c", DataType::Int64),
                ("d", DataType::Int64),
            ]);

            let left = PhysicalPlan::TableScan {
                table_name: "t1".to_string(),
                schema: left_schema.clone(),
                projection: None,
                row_count: None,
            };

            let right = PhysicalPlan::TableScan {
                table_name: "t2".to_string(),
                schema: right_schema.clone(),
                projection: None,
                row_count: None,
            };

            let cross_join = PhysicalPlan::CrossJoin {
                left: Box::new(left),
                right: Box::new(right),
                schema: joined_schema.clone(),
                parallel: false,
                hints: ExecutionHints::default(),
            };

            let project_schema = make_schema(&[("a", DataType::Int64), ("b", DataType::Int64)]);
            let project = PhysicalPlan::Project {
                input: Box::new(cross_join),
                expressions: vec![col_idx("a", 0), col_idx("b", 1)],
                schema: project_schema,
            };

            let optimized = ProjectionPushdown::optimize(project);

            match optimized {
                PhysicalPlan::Project { input, .. } => match input.as_ref() {
                    PhysicalPlan::CrossJoin { left, right, .. } => {
                        match left.as_ref() {
                            PhysicalPlan::TableScan { projection, .. } => {
                                assert_eq!(projection, &None);
                            }
                            other => panic!("Expected left TableScan, got {:?}", other),
                        }
                        match right.as_ref() {
                            PhysicalPlan::TableScan { projection, .. } => {
                                let proj = projection.as_ref().expect("Expected projection");
                                assert!(proj.is_empty());
                            }
                            other => panic!("Expected right TableScan, got {:?}", other),
                        }
                    }
                    other => panic!("Expected CrossJoin, got {:?}", other),
                },
                other => panic!("Expected Project, got {:?}", other),
            }
        }
    }

    mod union_tests {
        use super::*;

        #[test]
        fn union_passes_requirements_to_all_branches() {
            let schema = make_schema(&[
                ("a", DataType::Int64),
                ("b", DataType::Int64),
                ("c", DataType::Int64),
            ]);

            let scan1 = PhysicalPlan::TableScan {
                table_name: "t1".to_string(),
                schema: schema.clone(),
                projection: None,
                row_count: None,
            };

            let scan2 = PhysicalPlan::TableScan {
                table_name: "t2".to_string(),
                schema: schema.clone(),
                projection: None,
                row_count: None,
            };

            let scan3 = PhysicalPlan::TableScan {
                table_name: "t3".to_string(),
                schema: schema.clone(),
                projection: None,
                row_count: None,
            };

            let union_plan = PhysicalPlan::Union {
                inputs: vec![scan1, scan2, scan3],
                all: true,
                schema: schema.clone(),
                parallel: false,
                hints: ExecutionHints::default(),
            };

            let project_schema = make_schema(&[("a", DataType::Int64)]);
            let project = PhysicalPlan::Project {
                input: Box::new(union_plan),
                expressions: vec![col_idx("a", 0)],
                schema: project_schema,
            };

            let optimized = ProjectionPushdown::optimize(project);

            match optimized {
                PhysicalPlan::Project { input, .. } => match input.as_ref() {
                    PhysicalPlan::Union { inputs, .. } => {
                        assert_eq!(inputs.len(), 3);
                        for (i, input) in inputs.iter().enumerate() {
                            match input {
                                PhysicalPlan::TableScan { projection, .. } => {
                                    let proj = projection.as_ref().unwrap_or_else(|| {
                                        panic!("Expected projection on branch {}", i)
                                    });
                                    assert_eq!(
                                        proj,
                                        &vec![0],
                                        "Branch {} should only need column 0",
                                        i
                                    );
                                }
                                other => {
                                    panic!("Expected TableScan in branch {}, got {:?}", i, other)
                                }
                            }
                        }
                    }
                    other => panic!("Expected Union, got {:?}", other),
                },
                other => panic!("Expected Project, got {:?}", other),
            }
        }

        #[test]
        fn union_all_false_passes_requirements() {
            let schema = make_schema(&[("x", DataType::Int64), ("y", DataType::Int64)]);

            let scan1 = PhysicalPlan::TableScan {
                table_name: "t1".to_string(),
                schema: schema.clone(),
                projection: None,
                row_count: None,
            };

            let scan2 = PhysicalPlan::TableScan {
                table_name: "t2".to_string(),
                schema: schema.clone(),
                projection: None,
                row_count: None,
            };

            let union_plan = PhysicalPlan::Union {
                inputs: vec![scan1, scan2],
                all: false,
                schema: schema.clone(),
                parallel: false,
                hints: ExecutionHints::default(),
            };

            let project_schema = make_schema(&[("y", DataType::Int64)]);
            let project = PhysicalPlan::Project {
                input: Box::new(union_plan),
                expressions: vec![col_idx("y", 1)],
                schema: project_schema,
            };

            let optimized = ProjectionPushdown::optimize(project);

            match optimized {
                PhysicalPlan::Project { input, .. } => match input.as_ref() {
                    PhysicalPlan::Union { inputs, all, .. } => {
                        assert!(!all);
                        for input in inputs.iter() {
                            match input {
                                PhysicalPlan::TableScan { projection, .. } => {
                                    let proj = projection.as_ref().expect("Expected projection");
                                    assert_eq!(proj, &vec![1]);
                                }
                                other => panic!("Expected TableScan, got {:?}", other),
                            }
                        }
                    }
                    other => panic!("Expected Union, got {:?}", other),
                },
                other => panic!("Expected Project, got {:?}", other),
            }
        }
    }

    mod window_tests {
        use super::*;

        #[test]
        fn window_passes_through_input_columns_and_extracts_window_columns() {
            let schema = make_schema(&[
                ("a", DataType::Int64),
                ("b", DataType::Int64),
                ("c", DataType::Int64),
                ("extra", DataType::Int64),
            ]);

            let scan = PhysicalPlan::TableScan {
                table_name: "test".to_string(),
                schema: schema.clone(),
                projection: None,
                row_count: None,
            };

            let window_schema = make_schema(&[
                ("a", DataType::Int64),
                ("b", DataType::Int64),
                ("c", DataType::Int64),
                ("extra", DataType::Int64),
                ("row_num", DataType::Int64),
            ]);

            let window_expr = Expr::Window {
                func: WindowFunction::RowNumber,
                args: vec![],
                partition_by: vec![col_idx("a", 0)],
                order_by: vec![SortExpr {
                    expr: col_idx("b", 1),
                    asc: true,
                    nulls_first: false,
                }],
                frame: None,
            };

            let window = PhysicalPlan::Window {
                input: Box::new(scan),
                window_exprs: vec![window_expr],
                schema: window_schema.clone(),
                hints: ExecutionHints::default(),
            };

            let project_schema =
                make_schema(&[("c", DataType::Int64), ("row_num", DataType::Int64)]);
            let project = PhysicalPlan::Project {
                input: Box::new(window),
                expressions: vec![col_idx("c", 2), col_idx("row_num", 4)],
                schema: project_schema,
            };

            let optimized = ProjectionPushdown::optimize(project);

            match optimized {
                PhysicalPlan::Project { input, .. } => match input.as_ref() {
                    PhysicalPlan::Window { input, .. } => match input.as_ref() {
                        PhysicalPlan::TableScan { projection, .. } => {
                            let proj = projection.as_ref().expect("Expected projection");
                            assert!(proj.contains(&0));
                            assert!(proj.contains(&1));
                            assert!(proj.contains(&2));
                            assert!(!proj.contains(&3));
                        }
                        other => panic!("Expected TableScan, got {:?}", other),
                    },
                    other => panic!("Expected Window, got {:?}", other),
                },
                other => panic!("Expected Project, got {:?}", other),
            }
        }

        #[test]
        fn window_with_aggregate_function() {
            let schema = make_schema(&[
                ("category", DataType::String),
                ("value", DataType::Float64),
                ("extra", DataType::Int64),
            ]);

            let scan = PhysicalPlan::TableScan {
                table_name: "test".to_string(),
                schema: schema.clone(),
                projection: None,
                row_count: None,
            };

            let window_schema = make_schema(&[
                ("category", DataType::String),
                ("value", DataType::Float64),
                ("extra", DataType::Int64),
                ("running_sum", DataType::Float64),
            ]);

            let window_expr = Expr::AggregateWindow {
                func: AggregateFunction::Sum,
                args: vec![col_idx("value", 1)],
                distinct: false,
                partition_by: vec![col_idx("category", 0)],
                order_by: vec![],
                frame: None,
            };

            let window = PhysicalPlan::Window {
                input: Box::new(scan),
                window_exprs: vec![window_expr],
                schema: window_schema.clone(),
                hints: ExecutionHints::default(),
            };

            let project_schema = make_schema(&[("running_sum", DataType::Float64)]);
            let project = PhysicalPlan::Project {
                input: Box::new(window),
                expressions: vec![col_idx("running_sum", 3)],
                schema: project_schema,
            };

            let optimized = ProjectionPushdown::optimize(project);

            match optimized {
                PhysicalPlan::Project { input, .. } => match input.as_ref() {
                    PhysicalPlan::Window { input, .. } => match input.as_ref() {
                        PhysicalPlan::TableScan { projection, .. } => {
                            let proj = projection.as_ref().expect("Expected projection");
                            assert!(proj.contains(&0));
                            assert!(proj.contains(&1));
                            assert!(!proj.contains(&2));
                        }
                        other => panic!("Expected TableScan, got {:?}", other),
                    },
                    other => panic!("Expected Window, got {:?}", other),
                },
                other => panic!("Expected Project, got {:?}", other),
            }
        }

        #[test]
        fn window_only_requires_window_expression_columns() {
            let schema = make_schema(&[
                ("a", DataType::Int64),
                ("b", DataType::Int64),
                ("c", DataType::Int64),
                ("d", DataType::Int64),
            ]);

            let scan = PhysicalPlan::TableScan {
                table_name: "test".to_string(),
                schema: schema.clone(),
                projection: None,
                row_count: None,
            };

            let window_schema = make_schema(&[
                ("a", DataType::Int64),
                ("b", DataType::Int64),
                ("c", DataType::Int64),
                ("d", DataType::Int64),
                ("rank", DataType::Int64),
            ]);

            let window_expr = Expr::Window {
                func: WindowFunction::Rank,
                args: vec![],
                partition_by: vec![col_idx("a", 0)],
                order_by: vec![SortExpr {
                    expr: col_idx("b", 1),
                    asc: true,
                    nulls_first: false,
                }],
                frame: None,
            };

            let window = PhysicalPlan::Window {
                input: Box::new(scan),
                window_exprs: vec![window_expr],
                schema: window_schema.clone(),
                hints: ExecutionHints::default(),
            };

            let project_schema = make_schema(&[("rank", DataType::Int64)]);
            let project = PhysicalPlan::Project {
                input: Box::new(window),
                expressions: vec![col_idx("rank", 4)],
                schema: project_schema,
            };

            let optimized = ProjectionPushdown::optimize(project);

            match optimized {
                PhysicalPlan::Project { input, .. } => match input.as_ref() {
                    PhysicalPlan::Window { input, .. } => match input.as_ref() {
                        PhysicalPlan::TableScan { projection, .. } => {
                            let proj = projection.as_ref().expect("Expected projection");
                            assert!(proj.contains(&0));
                            assert!(proj.contains(&1));
                            assert!(!proj.contains(&2));
                            assert!(!proj.contains(&3));
                        }
                        other => panic!("Expected TableScan, got {:?}", other),
                    },
                    other => panic!("Expected Window, got {:?}", other),
                },
                other => panic!("Expected Project, got {:?}", other),
            }
        }
    }

    mod sort_tests {
        use super::*;

        #[test]
        fn sort_adds_sort_expression_columns_to_required() {
            let schema = make_schema(&[
                ("a", DataType::Int64),
                ("b", DataType::Int64),
                ("c", DataType::Int64),
            ]);

            let scan = PhysicalPlan::TableScan {
                table_name: "test".to_string(),
                schema: schema.clone(),
                projection: None,
                row_count: None,
            };

            let sort = PhysicalPlan::Sort {
                input: Box::new(scan),
                sort_exprs: vec![SortExpr {
                    expr: col_idx("b", 1),
                    asc: true,
                    nulls_first: false,
                }],
                hints: ExecutionHints::default(),
            };

            let project_schema = make_schema(&[("a", DataType::Int64)]);
            let project = PhysicalPlan::Project {
                input: Box::new(sort),
                expressions: vec![col_idx("a", 0)],
                schema: project_schema,
            };

            let optimized = ProjectionPushdown::optimize(project);

            match optimized {
                PhysicalPlan::Project { input, .. } => match input.as_ref() {
                    PhysicalPlan::Sort { input, .. } => match input.as_ref() {
                        PhysicalPlan::TableScan { projection, .. } => {
                            let proj = projection.as_ref().expect("Expected projection");
                            assert!(proj.contains(&0));
                            assert!(proj.contains(&1));
                        }
                        other => panic!("Expected TableScan, got {:?}", other),
                    },
                    other => panic!("Expected Sort, got {:?}", other),
                },
                other => panic!("Expected Project, got {:?}", other),
            }
        }
    }

    mod limit_tests {
        use super::*;

        #[test]
        fn limit_passes_through_required_columns() {
            let schema = make_schema(&[("a", DataType::Int64), ("b", DataType::Int64)]);

            let scan = PhysicalPlan::TableScan {
                table_name: "test".to_string(),
                schema: schema.clone(),
                projection: None,
                row_count: None,
            };

            let limit = PhysicalPlan::Limit {
                input: Box::new(scan),
                limit: Some(10),
                offset: None,
            };

            let project_schema = make_schema(&[("a", DataType::Int64)]);
            let project = PhysicalPlan::Project {
                input: Box::new(limit),
                expressions: vec![col_idx("a", 0)],
                schema: project_schema,
            };

            let optimized = ProjectionPushdown::optimize(project);

            match optimized {
                PhysicalPlan::Project { input, .. } => match input.as_ref() {
                    PhysicalPlan::Limit { input, .. } => match input.as_ref() {
                        PhysicalPlan::TableScan { projection, .. } => {
                            let proj = projection.as_ref().expect("Expected projection");
                            assert_eq!(proj, &vec![0]);
                        }
                        other => panic!("Expected TableScan, got {:?}", other),
                    },
                    other => panic!("Expected Limit, got {:?}", other),
                },
                other => panic!("Expected Project, got {:?}", other),
            }
        }
    }

    mod distinct_tests {
        use super::*;

        #[test]
        fn distinct_passes_through_required_columns() {
            let schema = make_schema(&[("a", DataType::Int64), ("b", DataType::Int64)]);

            let scan = PhysicalPlan::TableScan {
                table_name: "test".to_string(),
                schema: schema.clone(),
                projection: None,
                row_count: None,
            };

            let distinct = PhysicalPlan::Distinct {
                input: Box::new(scan),
            };

            let project_schema = make_schema(&[("b", DataType::Int64)]);
            let project = PhysicalPlan::Project {
                input: Box::new(distinct),
                expressions: vec![col_idx("b", 1)],
                schema: project_schema,
            };

            let optimized = ProjectionPushdown::optimize(project);

            match optimized {
                PhysicalPlan::Project { input, .. } => match input.as_ref() {
                    PhysicalPlan::Distinct { input } => match input.as_ref() {
                        PhysicalPlan::TableScan { projection, .. } => {
                            let proj = projection.as_ref().expect("Expected projection");
                            assert_eq!(proj, &vec![1]);
                        }
                        other => panic!("Expected TableScan, got {:?}", other),
                    },
                    other => panic!("Expected Distinct, got {:?}", other),
                },
                other => panic!("Expected Project, got {:?}", other),
            }
        }
    }

    mod aggregate_tests {
        use super::*;

        #[test]
        fn aggregate_extracts_columns_from_group_by_and_aggregates() {
            let schema = make_schema(&[
                ("category", DataType::String),
                ("region", DataType::String),
                ("amount", DataType::Float64),
                ("quantity", DataType::Int64),
            ]);

            let scan = PhysicalPlan::TableScan {
                table_name: "sales".to_string(),
                schema: schema.clone(),
                projection: None,
                row_count: None,
            };

            let agg_schema =
                make_schema(&[("category", DataType::String), ("total", DataType::Float64)]);

            let aggregate = PhysicalPlan::HashAggregate {
                input: Box::new(scan),
                group_by: vec![col_idx("category", 0)],
                aggregates: vec![Expr::Aggregate {
                    func: AggregateFunction::Sum,
                    args: vec![col_idx("amount", 2)],
                    distinct: false,
                    filter: None,
                    order_by: vec![],
                    limit: None,
                    ignore_nulls: false,
                }],
                schema: agg_schema.clone(),
                grouping_sets: None,
                hints: ExecutionHints::default(),
            };

            let optimized = ProjectionPushdown::optimize(aggregate);

            match optimized {
                PhysicalPlan::HashAggregate { input, .. } => match input.as_ref() {
                    PhysicalPlan::TableScan { projection, .. } => {
                        let proj = projection.as_ref().expect("Expected projection");
                        assert!(proj.contains(&0));
                        assert!(proj.contains(&2));
                        assert!(!proj.contains(&1));
                        assert!(!proj.contains(&3));
                    }
                    other => panic!("Expected TableScan, got {:?}", other),
                },
                other => panic!("Expected HashAggregate, got {:?}", other),
            }
        }
    }

    mod topn_tests {
        use super::*;

        #[test]
        fn topn_adds_sort_columns_to_required() {
            let schema = make_schema(&[
                ("a", DataType::Int64),
                ("b", DataType::Int64),
                ("c", DataType::Int64),
            ]);

            let scan = PhysicalPlan::TableScan {
                table_name: "test".to_string(),
                schema: schema.clone(),
                projection: None,
                row_count: None,
            };

            let topn = PhysicalPlan::TopN {
                input: Box::new(scan),
                sort_exprs: vec![SortExpr {
                    expr: col_idx("c", 2),
                    asc: false,
                    nulls_first: true,
                }],
                limit: 5,
            };

            let project_schema = make_schema(&[("a", DataType::Int64)]);
            let project = PhysicalPlan::Project {
                input: Box::new(topn),
                expressions: vec![col_idx("a", 0)],
                schema: project_schema,
            };

            let optimized = ProjectionPushdown::optimize(project);

            match optimized {
                PhysicalPlan::Project { input, .. } => match input.as_ref() {
                    PhysicalPlan::TopN { input, .. } => match input.as_ref() {
                        PhysicalPlan::TableScan { projection, .. } => {
                            let proj = projection.as_ref().expect("Expected projection");
                            assert!(proj.contains(&0));
                            assert!(proj.contains(&2));
                        }
                        other => panic!("Expected TableScan, got {:?}", other),
                    },
                    other => panic!("Expected TopN, got {:?}", other),
                },
                other => panic!("Expected Project, got {:?}", other),
            }
        }
    }

    mod nested_loop_join_tests {
        use super::*;

        #[test]
        fn nested_loop_join_splits_required_and_extracts_condition_columns() {
            let left_schema = make_schema(&[
                ("a", DataType::Int64),
                ("b", DataType::Int64),
                ("extra_left", DataType::Int64),
            ]);
            let right_schema = make_schema(&[
                ("c", DataType::Int64),
                ("d", DataType::Int64),
                ("extra_right", DataType::Int64),
            ]);
            let joined_schema = make_schema(&[
                ("a", DataType::Int64),
                ("b", DataType::Int64),
                ("extra_left", DataType::Int64),
                ("c", DataType::Int64),
                ("d", DataType::Int64),
                ("extra_right", DataType::Int64),
            ]);

            let left = PhysicalPlan::TableScan {
                table_name: "t1".to_string(),
                schema: left_schema.clone(),
                projection: None,
                row_count: None,
            };

            let right = PhysicalPlan::TableScan {
                table_name: "t2".to_string(),
                schema: right_schema.clone(),
                projection: None,
                row_count: None,
            };

            let join = PhysicalPlan::NestedLoopJoin {
                left: Box::new(left),
                right: Box::new(right),
                join_type: JoinType::Inner,
                condition: Some(gt(col_idx("a", 0), col_idx("c", 3))),
                schema: joined_schema.clone(),
                parallel: false,
                hints: ExecutionHints::default(),
            };

            let project_schema = make_schema(&[("d", DataType::Int64)]);
            let project = PhysicalPlan::Project {
                input: Box::new(join),
                expressions: vec![col_idx("d", 4)],
                schema: project_schema,
            };

            let optimized = ProjectionPushdown::optimize(project);

            match optimized {
                PhysicalPlan::Project { input, .. } => match input.as_ref() {
                    PhysicalPlan::NestedLoopJoin { left, right, .. } => {
                        match left.as_ref() {
                            PhysicalPlan::TableScan { projection, .. } => {
                                let proj = projection.as_ref().expect("Expected projection");
                                assert!(proj.contains(&0));
                                assert!(!proj.contains(&1));
                                assert!(!proj.contains(&2));
                            }
                            other => panic!("Expected left TableScan, got {:?}", other),
                        }
                        match right.as_ref() {
                            PhysicalPlan::TableScan { projection, .. } => {
                                let proj = projection.as_ref().expect("Expected projection");
                                assert!(proj.contains(&0));
                                assert!(proj.contains(&1));
                                assert!(!proj.contains(&2));
                            }
                            other => panic!("Expected right TableScan, got {:?}", other),
                        }
                    }
                    other => panic!("Expected NestedLoopJoin, got {:?}", other),
                },
                other => panic!("Expected Project, got {:?}", other),
            }
        }

        #[test]
        fn nested_loop_join_without_condition() {
            let left_schema = make_schema(&[("a", DataType::Int64)]);
            let right_schema = make_schema(&[("b", DataType::Int64)]);
            let joined_schema = make_schema(&[("a", DataType::Int64), ("b", DataType::Int64)]);

            let left = PhysicalPlan::TableScan {
                table_name: "t1".to_string(),
                schema: left_schema.clone(),
                projection: None,
                row_count: None,
            };

            let right = PhysicalPlan::TableScan {
                table_name: "t2".to_string(),
                schema: right_schema.clone(),
                projection: None,
                row_count: None,
            };

            let join = PhysicalPlan::NestedLoopJoin {
                left: Box::new(left),
                right: Box::new(right),
                join_type: JoinType::Inner,
                condition: None,
                schema: joined_schema.clone(),
                parallel: false,
                hints: ExecutionHints::default(),
            };

            let project_schema = make_schema(&[("a", DataType::Int64)]);
            let project = PhysicalPlan::Project {
                input: Box::new(join),
                expressions: vec![col_idx("a", 0)],
                schema: project_schema,
            };

            let optimized = ProjectionPushdown::optimize(project);

            match optimized {
                PhysicalPlan::Project { input, .. } => match input.as_ref() {
                    PhysicalPlan::NestedLoopJoin { left, right, .. } => {
                        match left.as_ref() {
                            PhysicalPlan::TableScan { projection, .. } => {
                                assert_eq!(projection, &None);
                            }
                            other => panic!("Expected left TableScan, got {:?}", other),
                        }
                        match right.as_ref() {
                            PhysicalPlan::TableScan { projection, .. } => {
                                let proj = projection.as_ref().expect("Expected projection");
                                assert!(proj.is_empty());
                            }
                            other => panic!("Expected right TableScan, got {:?}", other),
                        }
                    }
                    other => panic!("Expected NestedLoopJoin, got {:?}", other),
                },
                other => panic!("Expected Project, got {:?}", other),
            }
        }
    }

    mod intersect_except_tests {
        use super::*;

        #[test]
        fn intersect_passes_requirements_to_both_branches() {
            let schema = make_schema(&[("a", DataType::Int64), ("b", DataType::Int64)]);

            let left = PhysicalPlan::TableScan {
                table_name: "t1".to_string(),
                schema: schema.clone(),
                projection: None,
                row_count: None,
            };

            let right = PhysicalPlan::TableScan {
                table_name: "t2".to_string(),
                schema: schema.clone(),
                projection: None,
                row_count: None,
            };

            let intersect = PhysicalPlan::Intersect {
                left: Box::new(left),
                right: Box::new(right),
                all: false,
                schema: schema.clone(),
                parallel: false,
                hints: ExecutionHints::default(),
            };

            let project_schema = make_schema(&[("a", DataType::Int64)]);
            let project = PhysicalPlan::Project {
                input: Box::new(intersect),
                expressions: vec![col_idx("a", 0)],
                schema: project_schema,
            };

            let optimized = ProjectionPushdown::optimize(project);

            match optimized {
                PhysicalPlan::Project { input, .. } => match input.as_ref() {
                    PhysicalPlan::Intersect { left, right, .. } => {
                        match left.as_ref() {
                            PhysicalPlan::TableScan { projection, .. } => {
                                let proj = projection.as_ref().expect("Expected projection");
                                assert_eq!(proj, &vec![0]);
                            }
                            other => panic!("Expected left TableScan, got {:?}", other),
                        }
                        match right.as_ref() {
                            PhysicalPlan::TableScan { projection, .. } => {
                                let proj = projection.as_ref().expect("Expected projection");
                                assert_eq!(proj, &vec![0]);
                            }
                            other => panic!("Expected right TableScan, got {:?}", other),
                        }
                    }
                    other => panic!("Expected Intersect, got {:?}", other),
                },
                other => panic!("Expected Project, got {:?}", other),
            }
        }

        #[test]
        fn except_passes_requirements_to_both_branches() {
            let schema = make_schema(&[("x", DataType::Int64), ("y", DataType::Int64)]);

            let left = PhysicalPlan::TableScan {
                table_name: "t1".to_string(),
                schema: schema.clone(),
                projection: None,
                row_count: None,
            };

            let right = PhysicalPlan::TableScan {
                table_name: "t2".to_string(),
                schema: schema.clone(),
                projection: None,
                row_count: None,
            };

            let except = PhysicalPlan::Except {
                left: Box::new(left),
                right: Box::new(right),
                all: true,
                schema: schema.clone(),
                parallel: false,
                hints: ExecutionHints::default(),
            };

            let project_schema = make_schema(&[("y", DataType::Int64)]);
            let project = PhysicalPlan::Project {
                input: Box::new(except),
                expressions: vec![col_idx("y", 1)],
                schema: project_schema,
            };

            let optimized = ProjectionPushdown::optimize(project);

            match optimized {
                PhysicalPlan::Project { input, .. } => match input.as_ref() {
                    PhysicalPlan::Except {
                        left, right, all, ..
                    } => {
                        assert!(*all);
                        match left.as_ref() {
                            PhysicalPlan::TableScan { projection, .. } => {
                                let proj = projection.as_ref().expect("Expected projection");
                                assert_eq!(proj, &vec![1]);
                            }
                            other => panic!("Expected left TableScan, got {:?}", other),
                        }
                        match right.as_ref() {
                            PhysicalPlan::TableScan { projection, .. } => {
                                let proj = projection.as_ref().expect("Expected projection");
                                assert_eq!(proj, &vec![1]);
                            }
                            other => panic!("Expected right TableScan, got {:?}", other),
                        }
                    }
                    other => panic!("Expected Except, got {:?}", other),
                },
                other => panic!("Expected Project, got {:?}", other),
            }
        }
    }

    mod qualify_tests {
        use super::*;

        #[test]
        fn qualify_adds_predicate_columns() {
            let schema = make_schema(&[
                ("a", DataType::Int64),
                ("b", DataType::Int64),
                ("c", DataType::Int64),
            ]);

            let scan = PhysicalPlan::TableScan {
                table_name: "test".to_string(),
                schema: schema.clone(),
                projection: None,
                row_count: None,
            };

            let qualify = PhysicalPlan::Qualify {
                input: Box::new(scan),
                predicate: gt(col_idx("b", 1), lit_i64(5)),
            };

            let project_schema = make_schema(&[("a", DataType::Int64)]);
            let project = PhysicalPlan::Project {
                input: Box::new(qualify),
                expressions: vec![col_idx("a", 0)],
                schema: project_schema,
            };

            let optimized = ProjectionPushdown::optimize(project);

            match optimized {
                PhysicalPlan::Project { input, .. } => match input.as_ref() {
                    PhysicalPlan::Qualify { input, .. } => match input.as_ref() {
                        PhysicalPlan::TableScan { projection, .. } => {
                            let proj = projection.as_ref().expect("Expected projection");
                            assert!(proj.contains(&0));
                            assert!(proj.contains(&1));
                            assert!(!proj.contains(&2));
                        }
                        other => panic!("Expected TableScan, got {:?}", other),
                    },
                    other => panic!("Expected Qualify, got {:?}", other),
                },
                other => panic!("Expected Project, got {:?}", other),
            }
        }
    }

    mod sample_tests {
        use super::*;
        use crate::SampleType;

        #[test]
        fn sample_passes_through_required_columns() {
            let schema = make_schema(&[("a", DataType::Int64), ("b", DataType::Int64)]);

            let scan = PhysicalPlan::TableScan {
                table_name: "test".to_string(),
                schema: schema.clone(),
                projection: None,
                row_count: None,
            };

            let sample = PhysicalPlan::Sample {
                input: Box::new(scan),
                sample_type: SampleType::Rows,
                sample_value: 100,
            };

            let project_schema = make_schema(&[("b", DataType::Int64)]);
            let project = PhysicalPlan::Project {
                input: Box::new(sample),
                expressions: vec![col_idx("b", 1)],
                schema: project_schema,
            };

            let optimized = ProjectionPushdown::optimize(project);

            match optimized {
                PhysicalPlan::Project { input, .. } => match input.as_ref() {
                    PhysicalPlan::Sample { input, .. } => match input.as_ref() {
                        PhysicalPlan::TableScan { projection, .. } => {
                            let proj = projection.as_ref().expect("Expected projection");
                            assert_eq!(proj, &vec![1]);
                        }
                        other => panic!("Expected TableScan, got {:?}", other),
                    },
                    other => panic!("Expected Sample, got {:?}", other),
                },
                other => panic!("Expected Project, got {:?}", other),
            }
        }
    }

    mod unnest_tests {
        use yachtsql_ir::UnnestColumn;

        use super::*;

        #[test]
        fn unnest_adds_unnest_expression_columns() {
            let schema = make_schema(&[
                ("id", DataType::Int64),
                ("arr", DataType::Array(Box::new(DataType::Int64))),
                ("extra", DataType::Int64),
            ]);

            let scan = PhysicalPlan::TableScan {
                table_name: "test".to_string(),
                schema: schema.clone(),
                projection: None,
                row_count: None,
            };

            let unnest_schema = make_schema(&[
                ("id", DataType::Int64),
                ("arr", DataType::Array(Box::new(DataType::Int64))),
                ("extra", DataType::Int64),
                ("elem", DataType::Int64),
            ]);

            let unnest = PhysicalPlan::Unnest {
                input: Box::new(scan),
                columns: vec![UnnestColumn {
                    expr: col_idx("arr", 1),
                    alias: Some("elem".to_string()),
                    with_offset: false,
                    offset_alias: None,
                }],
                schema: unnest_schema.clone(),
            };

            let project_schema = make_schema(&[("elem", DataType::Int64)]);
            let project = PhysicalPlan::Project {
                input: Box::new(unnest),
                expressions: vec![col_idx("elem", 3)],
                schema: project_schema,
            };

            let optimized = ProjectionPushdown::optimize(project);

            match optimized {
                PhysicalPlan::Project { input, .. } => match input.as_ref() {
                    PhysicalPlan::Unnest { input, .. } => match input.as_ref() {
                        PhysicalPlan::TableScan { projection, .. } => {
                            let proj = projection.as_ref().expect("Expected projection");
                            assert!(proj.contains(&1));
                            assert!(!proj.contains(&0));
                            assert!(!proj.contains(&2));
                        }
                        other => panic!("Expected TableScan, got {:?}", other),
                    },
                    other => panic!("Expected Unnest, got {:?}", other),
                },
                other => panic!("Expected Project, got {:?}", other),
            }
        }
    }

    mod with_cte_tests {
        use super::*;

        #[test]
        fn with_cte_pushes_to_body() {
            let schema = make_schema(&[("a", DataType::Int64), ("b", DataType::Int64)]);

            let scan = PhysicalPlan::TableScan {
                table_name: "test".to_string(),
                schema: schema.clone(),
                projection: None,
                row_count: None,
            };

            let project_schema = make_schema(&[("a", DataType::Int64)]);
            let body = PhysicalPlan::Project {
                input: Box::new(scan),
                expressions: vec![col_idx("a", 0)],
                schema: project_schema.clone(),
            };

            let with_cte = PhysicalPlan::WithCte {
                ctes: vec![],
                body: Box::new(body),
                parallel_ctes: Vec::new(),
                hints: ExecutionHints::default(),
            };

            let optimized = ProjectionPushdown::optimize(with_cte);

            match optimized {
                PhysicalPlan::WithCte { body, .. } => match body.as_ref() {
                    PhysicalPlan::Project { input, .. } => match input.as_ref() {
                        PhysicalPlan::TableScan { projection, .. } => {
                            let proj = projection.as_ref().expect("Expected projection");
                            assert_eq!(proj, &vec![0]);
                        }
                        other => panic!("Expected TableScan, got {:?}", other),
                    },
                    other => panic!("Expected Project, got {:?}", other),
                },
                other => panic!("Expected WithCte, got {:?}", other),
            }
        }
    }

    mod required_columns_tests {
        use crate::planner::projection_pushdown::RequiredColumns;

        #[test]
        fn required_columns_new_is_empty() {
            let required = RequiredColumns::new();
            assert!(!required.contains(0));
            assert!(!required.contains(1));
        }

        #[test]
        fn required_columns_all_contains_all_indices() {
            let required = RequiredColumns::all(3);
            assert!(required.contains(0));
            assert!(required.contains(1));
            assert!(required.contains(2));
            assert!(!required.contains(3));
        }

        #[test]
        fn required_columns_add_and_contains() {
            let mut required = RequiredColumns::new();
            required.add(5);
            required.add(10);
            assert!(required.contains(5));
            assert!(required.contains(10));
            assert!(!required.contains(0));
            assert!(!required.contains(7));
        }

        #[test]
        fn required_columns_iter() {
            let mut required = RequiredColumns::new();
            required.add(2);
            required.add(4);
            required.add(6);

            let mut collected: Vec<_> = required.iter().collect();
            collected.sort();
            assert_eq!(collected, vec![2, 4, 6]);
        }
    }
}

#[cfg(test)]
mod predicate_tests {
    use rustc_hash::{FxHashMap, FxHashSet};
    use yachtsql_common::types::DataType;
    use yachtsql_ir::{
        AggregateFunction, BinaryOp, DateTimeField, Expr, JoinType, LogicalPlan, PlanSchema,
        ScalarFunction, SortExpr, TrimWhere, UnaryOp, WhenClause, WindowFrame, WindowFrameBound,
        WindowFrameUnit, WindowFunction,
    };

    use crate::planner::predicate::{
        build_aggregate_output_to_input_map, can_push_through_aggregate, can_push_through_window,
        classify_predicates_for_join, collect_column_indices, collect_column_indices_into,
        remap_predicate_indices,
    };

    fn empty_plan() -> LogicalPlan {
        LogicalPlan::Empty {
            schema: PlanSchema::from_fields(vec![]),
        }
    }

    fn col_idx(name: &str, index: usize) -> Expr {
        Expr::Column {
            table: None,
            name: name.to_string(),
            index: Some(index),
        }
    }

    fn col_no_idx(name: &str) -> Expr {
        Expr::Column {
            table: None,
            name: name.to_string(),
            index: None,
        }
    }

    fn lit_i64(v: i64) -> Expr {
        Expr::literal_i64(v)
    }

    fn eq(left: Expr, right: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOp::Eq,
            right: Box::new(right),
        }
    }

    fn and(left: Expr, right: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOp::And,
            right: Box::new(right),
        }
    }

    fn gt(left: Expr, right: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOp::Gt,
            right: Box::new(right),
        }
    }

    fn add(left: Expr, right: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOp::Add,
            right: Box::new(right),
        }
    }

    mod collect_column_indices_tests {
        use super::*;

        #[test]
        fn column_with_index() {
            let expr = col_idx("a", 5);
            let indices = collect_column_indices(&expr);
            assert_eq!(indices, FxHashSet::from_iter([5]));
        }

        #[test]
        fn column_without_index() {
            let expr = col_no_idx("a");
            let indices = collect_column_indices(&expr);
            assert_eq!(indices, FxHashSet::default());
        }

        #[test]
        fn literal() {
            let expr = lit_i64(42);
            let indices = collect_column_indices(&expr);
            assert_eq!(indices, FxHashSet::default());
        }

        #[test]
        fn binary_op() {
            let expr = add(col_idx("a", 1), col_idx("b", 3));
            let indices = collect_column_indices(&expr);
            assert_eq!(indices, FxHashSet::from_iter([1, 3]));
        }

        #[test]
        fn unary_op() {
            let expr = Expr::UnaryOp {
                op: UnaryOp::Not,
                expr: Box::new(col_idx("a", 2)),
            };
            let indices = collect_column_indices(&expr);
            assert_eq!(indices, FxHashSet::from_iter([2]));
        }

        #[test]
        fn is_null() {
            let expr = Expr::IsNull {
                expr: Box::new(col_idx("a", 7)),
                negated: false,
            };
            let indices = collect_column_indices(&expr);
            assert_eq!(indices, FxHashSet::from_iter([7]));
        }

        #[test]
        fn is_distinct_from() {
            let expr = Expr::IsDistinctFrom {
                left: Box::new(col_idx("a", 1)),
                right: Box::new(col_idx("b", 4)),
                negated: false,
            };
            let indices = collect_column_indices(&expr);
            assert_eq!(indices, FxHashSet::from_iter([1, 4]));
        }

        #[test]
        fn scalar_function() {
            let expr = Expr::ScalarFunction {
                name: ScalarFunction::Upper,
                args: vec![col_idx("a", 0), col_idx("b", 2)],
            };
            let indices = collect_column_indices(&expr);
            assert_eq!(indices, FxHashSet::from_iter([0, 2]));
        }

        #[test]
        fn cast() {
            let expr = Expr::Cast {
                expr: Box::new(col_idx("a", 3)),
                data_type: DataType::String,
                safe: false,
            };
            let indices = collect_column_indices(&expr);
            assert_eq!(indices, FxHashSet::from_iter([3]));
        }

        #[test]
        fn alias() {
            let expr = Expr::Alias {
                expr: Box::new(col_idx("a", 6)),
                name: "aliased".to_string(),
            };
            let indices = collect_column_indices(&expr);
            assert_eq!(indices, FxHashSet::from_iter([6]));
        }

        #[test]
        fn like() {
            let expr = Expr::Like {
                expr: Box::new(col_idx("a", 1)),
                pattern: Box::new(col_idx("b", 5)),
                negated: false,
                case_insensitive: false,
            };
            let indices = collect_column_indices(&expr);
            assert_eq!(indices, FxHashSet::from_iter([1, 5]));
        }

        #[test]
        fn in_list() {
            let expr = Expr::InList {
                expr: Box::new(col_idx("a", 0)),
                list: vec![col_idx("b", 2), col_idx("c", 4)],
                negated: false,
            };
            let indices = collect_column_indices(&expr);
            assert_eq!(indices, FxHashSet::from_iter([0, 2, 4]));
        }

        #[test]
        fn between() {
            let expr = Expr::Between {
                expr: Box::new(col_idx("a", 1)),
                low: Box::new(col_idx("b", 3)),
                high: Box::new(col_idx("c", 5)),
                negated: false,
            };
            let indices = collect_column_indices(&expr);
            assert_eq!(indices, FxHashSet::from_iter([1, 3, 5]));
        }

        #[test]
        fn case_with_operand() {
            let expr = Expr::Case {
                operand: Some(Box::new(col_idx("op", 0))),
                when_clauses: vec![WhenClause {
                    condition: col_idx("cond", 1),
                    result: col_idx("res", 2),
                }],
                else_result: Some(Box::new(col_idx("else", 3))),
            };
            let indices = collect_column_indices(&expr);
            assert_eq!(indices, FxHashSet::from_iter([0, 1, 2, 3]));
        }

        #[test]
        fn case_without_operand_or_else() {
            let expr = Expr::Case {
                operand: None,
                when_clauses: vec![WhenClause {
                    condition: col_idx("cond", 5),
                    result: col_idx("res", 7),
                }],
                else_result: None,
            };
            let indices = collect_column_indices(&expr);
            assert_eq!(indices, FxHashSet::from_iter([5, 7]));
        }

        #[test]
        fn extract() {
            let expr = Expr::Extract {
                field: DateTimeField::Year,
                expr: Box::new(col_idx("date_col", 4)),
            };
            let indices = collect_column_indices(&expr);
            assert_eq!(indices, FxHashSet::from_iter([4]));
        }

        #[test]
        fn substring_all_present() {
            let expr = Expr::Substring {
                expr: Box::new(col_idx("s", 0)),
                start: Some(Box::new(col_idx("start", 1))),
                length: Some(Box::new(col_idx("len", 2))),
            };
            let indices = collect_column_indices(&expr);
            assert_eq!(indices, FxHashSet::from_iter([0, 1, 2]));
        }

        #[test]
        fn substring_none_optional() {
            let expr = Expr::Substring {
                expr: Box::new(col_idx("s", 3)),
                start: None,
                length: None,
            };
            let indices = collect_column_indices(&expr);
            assert_eq!(indices, FxHashSet::from_iter([3]));
        }

        #[test]
        fn trim_with_what() {
            let expr = Expr::Trim {
                expr: Box::new(col_idx("s", 2)),
                trim_what: Some(Box::new(col_idx("tw", 4))),
                trim_where: TrimWhere::Both,
            };
            let indices = collect_column_indices(&expr);
            assert_eq!(indices, FxHashSet::from_iter([2, 4]));
        }

        #[test]
        fn trim_without_what() {
            let expr = Expr::Trim {
                expr: Box::new(col_idx("s", 6)),
                trim_what: None,
                trim_where: TrimWhere::Leading,
            };
            let indices = collect_column_indices(&expr);
            assert_eq!(indices, FxHashSet::from_iter([6]));
        }

        #[test]
        fn position() {
            let expr = Expr::Position {
                substr: Box::new(col_idx("sub", 1)),
                string: Box::new(col_idx("str", 3)),
            };
            let indices = collect_column_indices(&expr);
            assert_eq!(indices, FxHashSet::from_iter([1, 3]));
        }

        #[test]
        fn overlay_with_for() {
            let expr = Expr::Overlay {
                expr: Box::new(col_idx("e", 0)),
                overlay_what: Box::new(col_idx("what", 1)),
                overlay_from: Box::new(col_idx("from", 2)),
                overlay_for: Some(Box::new(col_idx("for", 3))),
            };
            let indices = collect_column_indices(&expr);
            assert_eq!(indices, FxHashSet::from_iter([0, 1, 2, 3]));
        }

        #[test]
        fn overlay_without_for() {
            let expr = Expr::Overlay {
                expr: Box::new(col_idx("e", 5)),
                overlay_what: Box::new(col_idx("what", 6)),
                overlay_from: Box::new(col_idx("from", 7)),
                overlay_for: None,
            };
            let indices = collect_column_indices(&expr);
            assert_eq!(indices, FxHashSet::from_iter([5, 6, 7]));
        }

        #[test]
        fn array_access() {
            let expr = Expr::ArrayAccess {
                array: Box::new(col_idx("arr", 2)),
                index: Box::new(col_idx("idx", 4)),
            };
            let indices = collect_column_indices(&expr);
            assert_eq!(indices, FxHashSet::from_iter([2, 4]));
        }

        #[test]
        fn struct_access() {
            let expr = Expr::StructAccess {
                expr: Box::new(col_idx("s", 8)),
                field: "f".to_string(),
            };
            let indices = collect_column_indices(&expr);
            assert_eq!(indices, FxHashSet::from_iter([8]));
        }

        #[test]
        fn array_elements() {
            let expr = Expr::Array {
                elements: vec![col_idx("a", 0), col_idx("b", 1), col_idx("c", 2)],
                element_type: None,
            };
            let indices = collect_column_indices(&expr);
            assert_eq!(indices, FxHashSet::from_iter([0, 1, 2]));
        }

        #[test]
        fn struct_fields() {
            let expr = Expr::Struct {
                fields: vec![
                    (Some("x".to_string()), col_idx("a", 3)),
                    (None, col_idx("b", 5)),
                ],
            };
            let indices = collect_column_indices(&expr);
            assert_eq!(indices, FxHashSet::from_iter([3, 5]));
        }

        #[test]
        fn aggregate() {
            let expr = Expr::Aggregate {
                func: AggregateFunction::Sum,
                args: vec![col_idx("val", 1)],
                distinct: false,
                filter: Some(Box::new(col_idx("f", 2))),
                order_by: vec![SortExpr {
                    expr: col_idx("o", 3),
                    asc: true,
                    nulls_first: false,
                }],
                limit: None,
                ignore_nulls: false,
            };
            let indices = collect_column_indices(&expr);
            assert_eq!(indices, FxHashSet::from_iter([1, 2, 3]));
        }

        #[test]
        fn aggregate_no_filter_no_order() {
            let expr = Expr::Aggregate {
                func: AggregateFunction::Count,
                args: vec![col_idx("x", 5)],
                distinct: true,
                filter: None,
                order_by: vec![],
                limit: None,
                ignore_nulls: false,
            };
            let indices = collect_column_indices(&expr);
            assert_eq!(indices, FxHashSet::from_iter([5]));
        }

        #[test]
        fn user_defined_aggregate() {
            let expr = Expr::UserDefinedAggregate {
                name: "my_agg".to_string(),
                args: vec![col_idx("a", 0), col_idx("b", 1)],
                distinct: false,
                filter: Some(Box::new(col_idx("f", 2))),
            };
            let indices = collect_column_indices(&expr);
            assert_eq!(indices, FxHashSet::from_iter([0, 1, 2]));
        }

        #[test]
        fn user_defined_aggregate_no_filter() {
            let expr = Expr::UserDefinedAggregate {
                name: "uda".to_string(),
                args: vec![col_idx("x", 7)],
                distinct: false,
                filter: None,
            };
            let indices = collect_column_indices(&expr);
            assert_eq!(indices, FxHashSet::from_iter([7]));
        }

        #[test]
        fn window() {
            let expr = Expr::Window {
                func: WindowFunction::RowNumber,
                args: vec![],
                partition_by: vec![col_idx("p", 1)],
                order_by: vec![SortExpr {
                    expr: col_idx("o", 2),
                    asc: true,
                    nulls_first: false,
                }],
                frame: None,
            };
            let indices = collect_column_indices(&expr);
            assert_eq!(indices, FxHashSet::from_iter([1, 2]));
        }

        #[test]
        fn window_with_args() {
            let expr = Expr::Window {
                func: WindowFunction::Lead,
                args: vec![col_idx("a", 0)],
                partition_by: vec![col_idx("p", 3)],
                order_by: vec![],
                frame: Some(WindowFrame {
                    unit: WindowFrameUnit::Rows,
                    start: WindowFrameBound::CurrentRow,
                    end: None,
                }),
            };
            let indices = collect_column_indices(&expr);
            assert_eq!(indices, FxHashSet::from_iter([0, 3]));
        }

        #[test]
        fn aggregate_window() {
            let expr = Expr::AggregateWindow {
                func: AggregateFunction::Sum,
                args: vec![col_idx("val", 4)],
                distinct: false,
                partition_by: vec![col_idx("p", 5)],
                order_by: vec![SortExpr {
                    expr: col_idx("o", 6),
                    asc: true,
                    nulls_first: false,
                }],
                frame: None,
            };
            let indices = collect_column_indices(&expr);
            assert_eq!(indices, FxHashSet::from_iter([4, 5, 6]));
        }

        #[test]
        fn at_time_zone() {
            let expr = Expr::AtTimeZone {
                timestamp: Box::new(col_idx("ts", 0)),
                time_zone: Box::new(col_idx("tz", 1)),
            };
            let indices = collect_column_indices(&expr);
            assert_eq!(indices, FxHashSet::from_iter([0, 1]));
        }

        #[test]
        fn json_access() {
            let expr = Expr::JsonAccess {
                expr: Box::new(col_idx("j", 9)),
                path: vec![],
            };
            let indices = collect_column_indices(&expr);
            assert_eq!(indices, FxHashSet::from_iter([9]));
        }

        #[test]
        fn in_unnest() {
            let expr = Expr::InUnnest {
                expr: Box::new(col_idx("e", 2)),
                array_expr: Box::new(col_idx("arr", 4)),
                negated: false,
            };
            let indices = collect_column_indices(&expr);
            assert_eq!(indices, FxHashSet::from_iter([2, 4]));
        }

        #[test]
        fn in_subquery() {
            let expr = Expr::InSubquery {
                expr: Box::new(col_idx("e", 7)),
                subquery: Box::new(empty_plan()),
                negated: false,
            };
            let indices = collect_column_indices(&expr);
            assert_eq!(indices, FxHashSet::from_iter([7]));
        }

        #[test]
        fn lambda() {
            let expr = Expr::Lambda {
                params: vec!["x".to_string()],
                body: Box::new(col_idx("col", 3)),
            };
            let indices = collect_column_indices(&expr);
            assert_eq!(indices, FxHashSet::from_iter([3]));
        }

        #[test]
        fn interval() {
            let expr = Expr::Interval {
                value: Box::new(col_idx("v", 2)),
                leading_field: Some(DateTimeField::Day),
            };
            let indices = collect_column_indices(&expr);
            assert_eq!(indices, FxHashSet::from_iter([2]));
        }

        #[test]
        fn no_column_expressions() {
            let exprs = [
                Expr::Parameter {
                    name: "p".to_string(),
                },
                Expr::Variable {
                    name: "v".to_string(),
                },
                Expr::Placeholder {
                    id: "?1".to_string(),
                },
                Expr::TypedString {
                    data_type: DataType::Date,
                    value: "2023-01-01".to_string(),
                },
                Expr::Wildcard { table: None },
                Expr::Default,
                Expr::Subquery(Box::new(empty_plan())),
                Expr::ScalarSubquery(Box::new(empty_plan())),
                Expr::ArraySubquery(Box::new(empty_plan())),
                Expr::Exists {
                    subquery: Box::new(empty_plan()),
                    negated: false,
                },
            ];
            for expr in exprs {
                let indices = collect_column_indices(&expr);
                assert_eq!(indices, FxHashSet::default());
            }
        }

        #[test]
        fn collect_column_indices_into_appends() {
            let mut indices = FxHashSet::from_iter([100, 200]);
            let expr = add(col_idx("a", 1), col_idx("b", 2));
            collect_column_indices_into(&expr, &mut indices);
            assert_eq!(indices, FxHashSet::from_iter([100, 200, 1, 2]));
        }

        #[test]
        fn complex_nested_expression() {
            let expr = Expr::Case {
                operand: Some(Box::new(col_idx("op", 0))),
                when_clauses: vec![WhenClause {
                    condition: Expr::BinaryOp {
                        left: Box::new(col_idx("a", 1)),
                        op: BinaryOp::Eq,
                        right: Box::new(lit_i64(1)),
                    },
                    result: Expr::ScalarFunction {
                        name: ScalarFunction::Upper,
                        args: vec![col_idx("s", 2)],
                    },
                }],
                else_result: Some(Box::new(Expr::Cast {
                    expr: Box::new(col_idx("e", 3)),
                    data_type: DataType::String,
                    safe: false,
                })),
            };
            let indices = collect_column_indices(&expr);
            assert_eq!(indices, FxHashSet::from_iter([0, 1, 2, 3]));
        }
    }

    mod classify_predicates_for_join_tests {
        use super::*;

        #[test]
        fn inner_join_left_only_predicate() {
            let predicates = vec![eq(col_idx("a", 0), lit_i64(1))];
            let left_schema_len = 2;

            let (left, right, post) =
                classify_predicates_for_join(JoinType::Inner, &predicates, left_schema_len);

            assert_eq!(left.len(), 1);
            assert_eq!(right.len(), 0);
            assert_eq!(post.len(), 0);
        }

        #[test]
        fn inner_join_right_only_predicate() {
            let predicates = vec![eq(col_idx("b", 3), lit_i64(2))];
            let left_schema_len = 2;

            let (left, right, post) =
                classify_predicates_for_join(JoinType::Inner, &predicates, left_schema_len);

            assert_eq!(left.len(), 0);
            assert_eq!(right.len(), 1);
            assert_eq!(post.len(), 0);
            match &right[0] {
                Expr::BinaryOp { left, .. } => match left.as_ref() {
                    Expr::Column { index, .. } => {
                        assert_eq!(*index, Some(1));
                    }
                    _ => panic!("expected column"),
                },
                _ => panic!("expected binary op"),
            }
        }

        #[test]
        fn inner_join_both_sides_predicate() {
            let predicates = vec![eq(col_idx("a", 0), col_idx("b", 3))];
            let left_schema_len = 2;

            let (left, right, post) =
                classify_predicates_for_join(JoinType::Inner, &predicates, left_schema_len);

            assert_eq!(left.len(), 0);
            assert_eq!(right.len(), 0);
            assert_eq!(post.len(), 1);
        }

        #[test]
        fn left_join_left_only_predicate() {
            let predicates = vec![eq(col_idx("a", 1), lit_i64(10))];
            let left_schema_len = 3;

            let (left, right, post) =
                classify_predicates_for_join(JoinType::Left, &predicates, left_schema_len);

            assert_eq!(left.len(), 1);
            assert_eq!(right.len(), 0);
            assert_eq!(post.len(), 0);
        }

        #[test]
        fn left_join_right_only_predicate_not_pushed() {
            let predicates = vec![eq(col_idx("b", 4), lit_i64(20))];
            let left_schema_len = 3;

            let (left, right, post) =
                classify_predicates_for_join(JoinType::Left, &predicates, left_schema_len);

            assert_eq!(left.len(), 0);
            assert_eq!(right.len(), 0);
            assert_eq!(post.len(), 1);
        }

        #[test]
        fn right_join_right_only_predicate() {
            let predicates = vec![eq(col_idx("b", 5), lit_i64(30))];
            let left_schema_len = 4;

            let (left, right, post) =
                classify_predicates_for_join(JoinType::Right, &predicates, left_schema_len);

            assert_eq!(left.len(), 0);
            assert_eq!(right.len(), 1);
            assert_eq!(post.len(), 0);
        }

        #[test]
        fn right_join_left_only_predicate_not_pushed() {
            let predicates = vec![eq(col_idx("a", 2), lit_i64(40))];
            let left_schema_len = 4;

            let (left, right, post) =
                classify_predicates_for_join(JoinType::Right, &predicates, left_schema_len);

            assert_eq!(left.len(), 0);
            assert_eq!(right.len(), 0);
            assert_eq!(post.len(), 1);
        }

        #[test]
        fn full_join_no_pushdown() {
            let predicates = vec![
                eq(col_idx("a", 0), lit_i64(1)),
                eq(col_idx("b", 3), lit_i64(2)),
            ];
            let left_schema_len = 2;

            let (left, right, post) =
                classify_predicates_for_join(JoinType::Full, &predicates, left_schema_len);

            assert_eq!(left.len(), 0);
            assert_eq!(right.len(), 0);
            assert_eq!(post.len(), 2);
        }

        #[test]
        fn inner_join_multiple_predicates() {
            let predicates = vec![
                eq(col_idx("a", 0), lit_i64(1)),
                eq(col_idx("b", 3), lit_i64(2)),
                eq(col_idx("x", 0), col_idx("y", 2)),
            ];
            let left_schema_len = 2;

            let (left, right, post) =
                classify_predicates_for_join(JoinType::Inner, &predicates, left_schema_len);

            assert_eq!(left.len(), 1);
            assert_eq!(right.len(), 1);
            assert_eq!(post.len(), 1);
        }

        #[test]
        fn literal_only_predicate_goes_to_both() {
            let predicates = vec![eq(lit_i64(1), lit_i64(1))];
            let left_schema_len = 2;

            let (left, right, post) =
                classify_predicates_for_join(JoinType::Inner, &predicates, left_schema_len);

            assert_eq!(left.len(), 0);
            assert_eq!(right.len(), 0);
            assert_eq!(post.len(), 1);
        }
    }

    mod build_aggregate_output_to_input_map_tests {
        use super::*;

        #[test]
        fn empty_group_by() {
            let group_by: Vec<Expr> = vec![];
            let map = build_aggregate_output_to_input_map(&group_by);
            assert_eq!(map, FxHashMap::default());
        }

        #[test]
        fn single_column() {
            let group_by = vec![col_idx("a", 5)];
            let map = build_aggregate_output_to_input_map(&group_by);
            assert_eq!(map, FxHashMap::from_iter([(0, 5)]));
        }

        #[test]
        fn multiple_columns() {
            let group_by = vec![col_idx("a", 0), col_idx("b", 3), col_idx("c", 7)];
            let map = build_aggregate_output_to_input_map(&group_by);
            assert_eq!(map, FxHashMap::from_iter([(0, 0), (1, 3), (2, 7)]));
        }

        #[test]
        fn column_without_index_skipped() {
            let group_by = vec![col_idx("a", 2), col_no_idx("b"), col_idx("c", 4)];
            let map = build_aggregate_output_to_input_map(&group_by);
            assert_eq!(map, FxHashMap::from_iter([(0, 2), (2, 4)]));
        }

        #[test]
        fn non_column_expressions_skipped() {
            let group_by = vec![
                col_idx("a", 1),
                add(col_idx("b", 2), lit_i64(1)),
                col_idx("c", 3),
            ];
            let map = build_aggregate_output_to_input_map(&group_by);
            assert_eq!(map, FxHashMap::from_iter([(0, 1), (2, 3)]));
        }
    }

    mod remap_predicate_indices_tests {
        use super::*;

        fn output_to_input() -> FxHashMap<usize, usize> {
            FxHashMap::from_iter([(0, 5), (1, 10), (2, 15)])
        }

        #[test]
        fn remap_column_with_index() {
            let map = output_to_input();
            let expr = col_idx("a", 0);
            let result = remap_predicate_indices(&expr, &map);
            match result {
                Some(Expr::Column { index, .. }) => {
                    assert_eq!(index, Some(5));
                }
                _ => panic!("expected column"),
            }
        }

        #[test]
        fn remap_column_not_in_map() {
            let map = output_to_input();
            let expr = col_idx("x", 99);
            let result = remap_predicate_indices(&expr, &map);
            assert!(result.is_none());
        }

        #[test]
        fn remap_column_without_index() {
            let map = output_to_input();
            let expr = col_no_idx("a");
            let result = remap_predicate_indices(&expr, &map);
            assert_eq!(result, Some(expr));
        }

        #[test]
        fn remap_literal() {
            let map = output_to_input();
            let expr = lit_i64(42);
            let result = remap_predicate_indices(&expr, &map);
            assert_eq!(result, Some(expr));
        }

        #[test]
        fn remap_binary_op() {
            let map = output_to_input();
            let expr = eq(col_idx("a", 0), col_idx("b", 1));
            let result = remap_predicate_indices(&expr, &map).unwrap();
            match result {
                Expr::BinaryOp { left, right, .. } => {
                    match left.as_ref() {
                        Expr::Column { index, .. } => assert_eq!(*index, Some(5)),
                        _ => panic!("expected column"),
                    }
                    match right.as_ref() {
                        Expr::Column { index, .. } => assert_eq!(*index, Some(10)),
                        _ => panic!("expected column"),
                    }
                }
                _ => panic!("expected binary op"),
            }
        }

        #[test]
        fn remap_binary_op_fails_if_child_fails() {
            let map = output_to_input();
            let expr = eq(col_idx("a", 0), col_idx("x", 99));
            let result = remap_predicate_indices(&expr, &map);
            assert!(result.is_none());
        }

        #[test]
        fn remap_unary_op() {
            let map = output_to_input();
            let expr = Expr::UnaryOp {
                op: UnaryOp::Not,
                expr: Box::new(col_idx("a", 1)),
            };
            let result = remap_predicate_indices(&expr, &map).unwrap();
            match result {
                Expr::UnaryOp { expr, op } => {
                    assert_eq!(op, UnaryOp::Not);
                    match expr.as_ref() {
                        Expr::Column { index, .. } => assert_eq!(*index, Some(10)),
                        _ => panic!("expected column"),
                    }
                }
                _ => panic!("expected unary op"),
            }
        }

        #[test]
        fn remap_is_null() {
            let map = output_to_input();
            let expr = Expr::IsNull {
                expr: Box::new(col_idx("a", 2)),
                negated: true,
            };
            let result = remap_predicate_indices(&expr, &map).unwrap();
            match result {
                Expr::IsNull { expr, negated } => {
                    assert!(negated);
                    match expr.as_ref() {
                        Expr::Column { index, .. } => assert_eq!(*index, Some(15)),
                        _ => panic!("expected column"),
                    }
                }
                _ => panic!("expected is null"),
            }
        }

        #[test]
        fn remap_is_distinct_from() {
            let map = output_to_input();
            let expr = Expr::IsDistinctFrom {
                left: Box::new(col_idx("a", 0)),
                right: Box::new(col_idx("b", 2)),
                negated: false,
            };
            let result = remap_predicate_indices(&expr, &map).unwrap();
            match result {
                Expr::IsDistinctFrom {
                    left,
                    right,
                    negated,
                } => {
                    assert!(!negated);
                    match left.as_ref() {
                        Expr::Column { index, .. } => assert_eq!(*index, Some(5)),
                        _ => panic!("expected column"),
                    }
                    match right.as_ref() {
                        Expr::Column { index, .. } => assert_eq!(*index, Some(15)),
                        _ => panic!("expected column"),
                    }
                }
                _ => panic!("expected is distinct from"),
            }
        }

        #[test]
        fn remap_cast() {
            let map = output_to_input();
            let expr = Expr::Cast {
                expr: Box::new(col_idx("a", 1)),
                data_type: DataType::String,
                safe: true,
            };
            let result = remap_predicate_indices(&expr, &map).unwrap();
            match result {
                Expr::Cast {
                    expr,
                    data_type,
                    safe,
                } => {
                    assert_eq!(data_type, DataType::String);
                    assert!(safe);
                    match expr.as_ref() {
                        Expr::Column { index, .. } => assert_eq!(*index, Some(10)),
                        _ => panic!("expected column"),
                    }
                }
                _ => panic!("expected cast"),
            }
        }

        #[test]
        fn remap_like() {
            let map = output_to_input();
            let expr = Expr::Like {
                expr: Box::new(col_idx("a", 0)),
                pattern: Box::new(col_idx("p", 1)),
                negated: true,
                case_insensitive: true,
            };
            let result = remap_predicate_indices(&expr, &map).unwrap();
            match result {
                Expr::Like {
                    expr,
                    pattern,
                    negated,
                    case_insensitive,
                } => {
                    assert!(negated);
                    assert!(case_insensitive);
                    match expr.as_ref() {
                        Expr::Column { index, .. } => assert_eq!(*index, Some(5)),
                        _ => panic!("expected column"),
                    }
                    match pattern.as_ref() {
                        Expr::Column { index, .. } => assert_eq!(*index, Some(10)),
                        _ => panic!("expected column"),
                    }
                }
                _ => panic!("expected like"),
            }
        }

        #[test]
        fn remap_in_list() {
            let map = output_to_input();
            let expr = Expr::InList {
                expr: Box::new(col_idx("a", 0)),
                list: vec![col_idx("b", 1), col_idx("c", 2)],
                negated: false,
            };
            let result = remap_predicate_indices(&expr, &map).unwrap();
            match result {
                Expr::InList {
                    expr,
                    list,
                    negated,
                } => {
                    assert!(!negated);
                    match expr.as_ref() {
                        Expr::Column { index, .. } => assert_eq!(*index, Some(5)),
                        _ => panic!("expected column"),
                    }
                    assert_eq!(list.len(), 2);
                    match &list[0] {
                        Expr::Column { index, .. } => assert_eq!(*index, Some(10)),
                        _ => panic!("expected column"),
                    }
                    match &list[1] {
                        Expr::Column { index, .. } => assert_eq!(*index, Some(15)),
                        _ => panic!("expected column"),
                    }
                }
                _ => panic!("expected in list"),
            }
        }

        #[test]
        fn remap_in_list_fails_if_list_item_fails() {
            let map = output_to_input();
            let expr = Expr::InList {
                expr: Box::new(col_idx("a", 0)),
                list: vec![col_idx("b", 1), col_idx("x", 99)],
                negated: false,
            };
            let result = remap_predicate_indices(&expr, &map);
            assert!(result.is_none());
        }

        #[test]
        fn remap_between() {
            let map = output_to_input();
            let expr = Expr::Between {
                expr: Box::new(col_idx("a", 0)),
                low: Box::new(col_idx("b", 1)),
                high: Box::new(col_idx("c", 2)),
                negated: true,
            };
            let result = remap_predicate_indices(&expr, &map).unwrap();
            match result {
                Expr::Between {
                    expr,
                    low,
                    high,
                    negated,
                } => {
                    assert!(negated);
                    match expr.as_ref() {
                        Expr::Column { index, .. } => assert_eq!(*index, Some(5)),
                        _ => panic!("expected column"),
                    }
                    match low.as_ref() {
                        Expr::Column { index, .. } => assert_eq!(*index, Some(10)),
                        _ => panic!("expected column"),
                    }
                    match high.as_ref() {
                        Expr::Column { index, .. } => assert_eq!(*index, Some(15)),
                        _ => panic!("expected column"),
                    }
                }
                _ => panic!("expected between"),
            }
        }

        #[test]
        fn remap_scalar_function() {
            let map = output_to_input();
            let expr = Expr::ScalarFunction {
                name: ScalarFunction::Upper,
                args: vec![col_idx("a", 0), col_idx("b", 1)],
            };
            let result = remap_predicate_indices(&expr, &map).unwrap();
            match result {
                Expr::ScalarFunction { name, args } => {
                    assert_eq!(name, ScalarFunction::Upper);
                    assert_eq!(args.len(), 2);
                    match &args[0] {
                        Expr::Column { index, .. } => assert_eq!(*index, Some(5)),
                        _ => panic!("expected column"),
                    }
                    match &args[1] {
                        Expr::Column { index, .. } => assert_eq!(*index, Some(10)),
                        _ => panic!("expected column"),
                    }
                }
                _ => panic!("expected scalar function"),
            }
        }

        #[test]
        fn remap_scalar_function_fails_if_arg_fails() {
            let map = output_to_input();
            let expr = Expr::ScalarFunction {
                name: ScalarFunction::Upper,
                args: vec![col_idx("a", 0), col_idx("x", 99)],
            };
            let result = remap_predicate_indices(&expr, &map);
            assert!(result.is_none());
        }

        #[test]
        fn remap_unsupported_expression() {
            let map = output_to_input();
            let expr = Expr::Aggregate {
                func: AggregateFunction::Sum,
                args: vec![col_idx("a", 0)],
                distinct: false,
                filter: None,
                order_by: vec![],
                limit: None,
                ignore_nulls: false,
            };
            let result = remap_predicate_indices(&expr, &map);
            assert!(result.is_none());
        }

        #[test]
        fn remap_complex_nested_expression() {
            let map = output_to_input();
            let expr = Expr::BinaryOp {
                left: Box::new(Expr::IsNull {
                    expr: Box::new(col_idx("a", 0)),
                    negated: false,
                }),
                op: BinaryOp::Or,
                right: Box::new(Expr::Cast {
                    expr: Box::new(col_idx("b", 1)),
                    data_type: DataType::Bool,
                    safe: false,
                }),
            };
            let result = remap_predicate_indices(&expr, &map).unwrap();
            match result {
                Expr::BinaryOp { left, right, op } => {
                    assert_eq!(op, BinaryOp::Or);
                    match left.as_ref() {
                        Expr::IsNull { expr, .. } => match expr.as_ref() {
                            Expr::Column { index, .. } => assert_eq!(*index, Some(5)),
                            _ => panic!("expected column"),
                        },
                        _ => panic!("expected is null"),
                    }
                    match right.as_ref() {
                        Expr::Cast { expr, .. } => match expr.as_ref() {
                            Expr::Column { index, .. } => assert_eq!(*index, Some(10)),
                            _ => panic!("expected column"),
                        },
                        _ => panic!("expected cast"),
                    }
                }
                _ => panic!("expected binary op"),
            }
        }
    }

    mod can_push_through_aggregate_tests {
        use super::*;

        #[test]
        fn predicate_on_group_by_column() {
            let predicate = eq(col_idx("a", 0), lit_i64(1));
            let num_group_by_cols = 2;
            assert!(can_push_through_aggregate(&predicate, num_group_by_cols));
        }

        #[test]
        fn predicate_on_multiple_group_by_columns() {
            let predicate = and(
                eq(col_idx("a", 0), lit_i64(1)),
                eq(col_idx("b", 1), lit_i64(2)),
            );
            let num_group_by_cols = 3;
            assert!(can_push_through_aggregate(&predicate, num_group_by_cols));
        }

        #[test]
        fn predicate_on_aggregate_column() {
            let predicate = eq(col_idx("sum", 3), lit_i64(100));
            let num_group_by_cols = 2;
            assert!(!can_push_through_aggregate(&predicate, num_group_by_cols));
        }

        #[test]
        fn predicate_on_mixed_columns() {
            let predicate = and(
                eq(col_idx("a", 0), lit_i64(1)),
                gt(col_idx("sum", 5), lit_i64(100)),
            );
            let num_group_by_cols = 2;
            assert!(!can_push_through_aggregate(&predicate, num_group_by_cols));
        }

        #[test]
        fn literal_only_predicate() {
            let predicate = eq(lit_i64(1), lit_i64(1));
            let num_group_by_cols = 2;
            assert!(can_push_through_aggregate(&predicate, num_group_by_cols));
        }

        #[test]
        fn edge_case_boundary_column() {
            let predicate = eq(col_idx("a", 1), lit_i64(1));
            let num_group_by_cols = 2;
            assert!(can_push_through_aggregate(&predicate, num_group_by_cols));

            let predicate2 = eq(col_idx("a", 2), lit_i64(1));
            assert!(!can_push_through_aggregate(&predicate2, num_group_by_cols));
        }

        #[test]
        fn zero_group_by_columns() {
            let predicate = eq(col_idx("a", 0), lit_i64(1));
            let num_group_by_cols = 0;
            assert!(!can_push_through_aggregate(&predicate, num_group_by_cols));
        }
    }

    mod can_push_through_window_tests {
        use super::*;

        #[test]
        fn predicate_on_input_column() {
            let predicate = eq(col_idx("a", 0), lit_i64(1));
            let input_schema_len = 3;
            assert!(can_push_through_window(&predicate, input_schema_len));
        }

        #[test]
        fn predicate_on_multiple_input_columns() {
            let predicate = and(
                eq(col_idx("a", 0), lit_i64(1)),
                eq(col_idx("b", 2), lit_i64(2)),
            );
            let input_schema_len = 3;
            assert!(can_push_through_window(&predicate, input_schema_len));
        }

        #[test]
        fn predicate_on_window_column() {
            let predicate = eq(col_idx("row_num", 4), lit_i64(1));
            let input_schema_len = 3;
            assert!(!can_push_through_window(&predicate, input_schema_len));
        }

        #[test]
        fn predicate_on_mixed_columns() {
            let predicate = and(
                eq(col_idx("a", 1), lit_i64(1)),
                gt(col_idx("row_num", 5), lit_i64(1)),
            );
            let input_schema_len = 3;
            assert!(!can_push_through_window(&predicate, input_schema_len));
        }

        #[test]
        fn literal_only_predicate() {
            let predicate = eq(lit_i64(1), lit_i64(1));
            let input_schema_len = 3;
            assert!(can_push_through_window(&predicate, input_schema_len));
        }

        #[test]
        fn edge_case_boundary_column() {
            let predicate = eq(col_idx("a", 2), lit_i64(1));
            let input_schema_len = 3;
            assert!(can_push_through_window(&predicate, input_schema_len));

            let predicate2 = eq(col_idx("a", 3), lit_i64(1));
            assert!(!can_push_through_window(&predicate2, input_schema_len));
        }

        #[test]
        fn zero_input_schema_len() {
            let predicate = eq(col_idx("a", 0), lit_i64(1));
            let input_schema_len = 0;
            assert!(!can_push_through_window(&predicate, input_schema_len));
        }
    }
}

#[cfg(test)]
mod sql_optimizer_tests {
    use yachtsql_ir::JoinType;

    use crate::PhysicalPlan;
    use crate::test_utils::{assert_plan, optimize_sql_default};

    mod filter_pushdown {
        use super::*;

        #[test]
        fn filter_on_right_table_pushed_below_join() {
            let plan = optimize_sql_default(
                "SELECT o.id, c.name
                 FROM orders o
                 JOIN customers c ON o.customer_id = c.id
                 WHERE c.country = 'USA'",
            );

            assert_plan!(
                plan,
                Project {
                    input: (HashJoin {
                        left: (TableScan {
                            table_name: "orders"
                        }),
                        right: (Filter {
                            input: (TableScan {
                                table_name: "customers"
                            }),
                            predicate: _
                        }),
                        join_type: JoinType::Inner
                    })
                }
            );
        }

        #[test]
        fn filter_on_left_table_pushed_below_join() {
            let plan = optimize_sql_default(
                "SELECT o.id, c.name
                 FROM orders o
                 JOIN customers c ON o.customer_id = c.id
                 WHERE o.amount > 100",
            );

            assert_plan!(
                plan,
                Project {
                    input: (HashJoin {
                        left: (Filter {
                            input: (TableScan {
                                table_name: "orders"
                            }),
                            predicate: _
                        }),
                        right: (TableScan {
                            table_name: "customers"
                        }),
                        join_type: JoinType::Inner
                    })
                }
            );
        }

        #[test]
        fn filters_on_both_tables_pushed_below_join() {
            let plan = optimize_sql_default(
                "SELECT o.id, c.name
                 FROM orders o
                 JOIN customers c ON o.customer_id = c.id
                 WHERE o.amount > 100 AND c.country = 'USA'",
            );

            assert_plan!(
                plan,
                Project {
                    input: (HashJoin {
                        left: (Filter {
                            input: (TableScan {
                                table_name: "orders"
                            }),
                            predicate: _
                        }),
                        right: (Filter {
                            input: (TableScan {
                                table_name: "customers"
                            }),
                            predicate: _
                        }),
                        join_type: JoinType::Inner
                    })
                }
            );
        }

        #[test]
        fn filter_referencing_both_tables_not_pushed() {
            let plan = optimize_sql_default(
                "SELECT o.id, c.name
                 FROM orders o
                 JOIN customers c ON o.customer_id = c.id
                 WHERE o.amount > c.id",
            );

            assert_plan!(
                plan,
                Project {
                    input: (Filter {
                        input: (HashJoin {
                            left: (TableScan {
                                table_name: "orders"
                            }),
                            right: (TableScan {
                                table_name: "customers"
                            }),
                            join_type: JoinType::Inner
                        }),
                        predicate: _
                    })
                }
            );
        }
    }

    mod join_selection {
        use super::*;

        #[test]
        fn equi_join_uses_hash_join() {
            let plan = optimize_sql_default(
                "SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id",
            );

            assert_plan!(
                plan,
                Project {
                    input: (HashJoin {
                        left: (TableScan {
                            table_name: "orders"
                        }),
                        right: (TableScan {
                            table_name: "customers"
                        }),
                        join_type: JoinType::Inner
                    })
                }
            );
        }

        #[test]
        fn non_equi_join_uses_nested_loop() {
            let plan =
                optimize_sql_default("SELECT * FROM orders o JOIN customers c ON o.amount > c.id");

            assert_plan!(
                plan,
                Project {
                    input: (NestedLoopJoin {
                        left: (TableScan {
                            table_name: "orders"
                        }),
                        right: (TableScan {
                            table_name: "customers"
                        }),
                        join_type: JoinType::Inner
                    })
                }
            );
        }

        #[test]
        fn cross_join_produces_cross_join_node() {
            let plan = optimize_sql_default("SELECT * FROM orders o CROSS JOIN customers c");

            assert_plan!(
                plan,
                Project {
                    input: (CrossJoin {
                        left: (TableScan {
                            table_name: "orders"
                        }),
                        right: (TableScan {
                            table_name: "customers"
                        })
                    })
                }
            );
        }

        #[test]
        fn left_join_preserves_join_type() {
            let plan = optimize_sql_default(
                "SELECT * FROM orders o LEFT JOIN customers c ON o.customer_id = c.id",
            );

            assert_plan!(
                plan,
                Project {
                    input: (HashJoin {
                        left: (TableScan {
                            table_name: "orders"
                        }),
                        right: (TableScan {
                            table_name: "customers"
                        }),
                        join_type: JoinType::Left
                    })
                }
            );
        }
    }

    mod projection_pushdown {
        use super::*;

        #[test]
        fn unused_columns_pruned_from_scan() {
            let plan = optimize_sql_default("SELECT id, amount FROM orders");

            match &plan {
                PhysicalPlan::Project { input, .. } => match input.as_ref() {
                    PhysicalPlan::TableScan { projection, .. } => {
                        let proj = projection.as_ref().expect("projection should exist");
                        assert!(proj.contains(&0), "id (index 0) should be projected");
                        assert!(proj.contains(&2), "amount (index 2) should be projected");
                        assert!(!proj.contains(&1), "customer_id should not be projected");
                        assert!(!proj.contains(&3), "status should not be projected");
                    }
                    other => panic!("Expected TableScan, got {:?}", other),
                },
                other => panic!("Expected Project, got {:?}", other),
            }
        }

        #[test]
        fn join_keys_included_in_projection() {
            let plan = optimize_sql_default(
                "SELECT o.id FROM orders o JOIN customers c ON o.customer_id = c.id",
            );

            match &plan {
                PhysicalPlan::Project { input, .. } => match input.as_ref() {
                    PhysicalPlan::HashJoin { left, right, .. } => {
                        match left.as_ref() {
                            PhysicalPlan::TableScan { projection, .. } => {
                                let proj = projection.as_ref().expect("projection should exist");
                                assert!(proj.contains(&0), "id should be projected");
                                assert!(proj.contains(&1), "customer_id needed for join");
                            }
                            other => panic!("Expected TableScan on left, got {:?}", other),
                        }
                        match right.as_ref() {
                            PhysicalPlan::TableScan { projection, .. } => {
                                let proj = projection.as_ref().expect("projection should exist");
                                assert!(proj.contains(&0), "id needed for join");
                            }
                            other => panic!("Expected TableScan on right, got {:?}", other),
                        }
                    }
                    other => panic!("Expected HashJoin, got {:?}", other),
                },
                other => panic!("Expected Project, got {:?}", other),
            }
        }
    }

    mod sort_limit_optimization {
        use super::*;

        #[test]
        fn sort_with_limit() {
            let plan =
                optimize_sql_default("SELECT id, amount FROM orders ORDER BY amount DESC LIMIT 10");

            assert_plan!(
                plan,
                Project {
                    input: (TopN {
                        input: (TableScan {
                            table_name: "orders"
                        })
                    })
                }
            );
        }

        #[test]
        fn sort_without_limit() {
            let plan = optimize_sql_default("SELECT id, amount FROM orders ORDER BY amount DESC");

            assert_plan!(
                plan,
                Project {
                    input: (Sort {
                        input: (TableScan {
                            table_name: "orders"
                        })
                    })
                }
            );
        }

        #[test]
        fn limit_without_sort() {
            let plan = optimize_sql_default("SELECT id, amount FROM orders LIMIT 10");

            assert_plan!(
                plan,
                Project {
                    input: (Limit {
                        input: (TableScan {
                            table_name: "orders"
                        })
                    })
                }
            );
        }
    }

    mod cross_to_hash_join {
        use super::*;

        #[test]
        fn cross_join_with_equality_becomes_hash_join() {
            let plan = optimize_sql_default(
                "SELECT o.id, c.name
                 FROM orders o, customers c
                 WHERE o.customer_id = c.id",
            );

            assert_plan!(
                plan,
                Project {
                    input: (HashJoin {
                        left: (TableScan {
                            table_name: "orders"
                        }),
                        right: (TableScan {
                            table_name: "customers"
                        }),
                        join_type: JoinType::Inner
                    })
                }
            );
        }

        #[test]
        fn cross_join_with_non_equality_stays_cross_join() {
            let plan = optimize_sql_default(
                "SELECT o.id, c.name
                 FROM orders o, customers c
                 WHERE o.amount > 100",
            );

            match &plan {
                PhysicalPlan::Project { input, .. } => match input.as_ref() {
                    PhysicalPlan::CrossJoin { left, .. } => {
                        assert!(matches!(left.as_ref(), PhysicalPlan::Filter { .. }));
                    }
                    other => panic!(
                        "Expected CrossJoin with filter pushed to left, got {:?}",
                        other
                    ),
                },
                other => panic!("Expected Project, got {:?}", other),
            }
        }

        #[test]
        fn cross_join_with_equality_and_residual() {
            let plan = optimize_sql_default(
                "SELECT o.id, c.name
                 FROM orders o, customers c
                 WHERE o.customer_id = c.id AND o.amount > 100",
            );

            match &plan {
                PhysicalPlan::Project { input, .. } => match input.as_ref() {
                    PhysicalPlan::Filter { input, .. } => {
                        assert!(matches!(input.as_ref(), PhysicalPlan::HashJoin { .. }));
                    }
                    PhysicalPlan::HashJoin { .. } => {}
                    other => panic!("Expected Filter or HashJoin, got {:?}", other),
                },
                other => panic!("Expected Project, got {:?}", other),
            }
        }

        #[test]
        fn multiple_equi_keys_converted() {
            let plan = optimize_sql_default(
                "SELECT o.id
                 FROM orders o, customers c
                 WHERE o.customer_id = c.id AND o.id = c.id",
            );

            match &plan {
                PhysicalPlan::Project { input, .. } => match input.as_ref() {
                    PhysicalPlan::HashJoin {
                        left_keys,
                        right_keys,
                        ..
                    } => {
                        assert_eq!(left_keys.len(), 2);
                        assert_eq!(right_keys.len(), 2);
                    }
                    other => panic!("Expected HashJoin, got {:?}", other),
                },
                other => panic!("Expected Project, got {:?}", other),
            }
        }
    }

    mod sort_elimination {
        use yachtsql_common::types::DataType;

        use super::*;
        use crate::apply_sort_elimination;

        fn count_sorts(plan: &PhysicalPlan, count: &mut usize) {
            match plan {
                PhysicalPlan::Sort { input, .. } => {
                    *count += 1;
                    count_sorts(input, count);
                }
                PhysicalPlan::Project { input, .. } => count_sorts(input, count),
                PhysicalPlan::Filter { input, .. } => count_sorts(input, count),
                PhysicalPlan::Limit { input, .. } => count_sorts(input, count),
                PhysicalPlan::HashJoin { left, right, .. } => {
                    count_sorts(left, count);
                    count_sorts(right, count);
                }
                PhysicalPlan::TopN { input, .. } => count_sorts(input, count),
                _ => {}
            }
        }

        #[test]
        fn subquery_with_different_projections_keeps_both_sorts() {
            let plan = optimize_sql_default(
                "SELECT * FROM (SELECT id FROM orders ORDER BY id) ORDER BY id",
            );

            let mut sort_count = 0;
            count_sorts(&plan, &mut sort_count);
            assert!(
                sort_count >= 1,
                "Expected at least one Sort node, got {}",
                sort_count
            );
        }

        #[test]
        fn different_sort_order_preserved() {
            let plan = optimize_sql_default(
                "SELECT * FROM (SELECT id, amount FROM orders ORDER BY id) ORDER BY amount",
            );

            let mut sort_count = 0;
            count_sorts(&plan, &mut sort_count);
            assert_eq!(
                sort_count, 2,
                "Expected two Sort nodes for different columns"
            );
        }

        #[test]
        fn sort_elimination_removes_redundant_directly_adjacent_sort() {
            use yachtsql_ir::{Expr, PlanField, PlanSchema, SortExpr};

            let schema = PlanSchema::from_fields(vec![
                PlanField::new("id", DataType::Int64),
                PlanField::new("name", DataType::String),
            ]);

            let table_scan = PhysicalPlan::TableScan {
                table_name: "test".to_string(),
                schema: schema.clone(),
                projection: None,
                row_count: None,
            };

            let sort_expr = vec![SortExpr {
                expr: Expr::Column {
                    table: None,
                    name: "id".to_string(),
                    index: Some(0),
                },
                asc: true,
                nulls_first: false,
            }];

            let inner_sort = PhysicalPlan::Sort {
                input: Box::new(table_scan),
                sort_exprs: sort_expr.clone(),
                hints: Default::default(),
            };

            let outer_sort = PhysicalPlan::Sort {
                input: Box::new(inner_sort),
                sort_exprs: sort_expr,
                hints: Default::default(),
            };

            let optimized = apply_sort_elimination(outer_sort);

            let mut sort_count = 0;
            count_sorts(&optimized, &mut sort_count);
            assert_eq!(
                sort_count, 1,
                "Adjacent duplicate sorts should be eliminated"
            );
        }

        #[test]
        fn sort_elimination_keeps_different_sort_orders() {
            use yachtsql_ir::{Expr, PlanField, PlanSchema, SortExpr};

            let schema = PlanSchema::from_fields(vec![
                PlanField::new("id", DataType::Int64),
                PlanField::new("name", DataType::String),
            ]);

            let table_scan = PhysicalPlan::TableScan {
                table_name: "test".to_string(),
                schema: schema.clone(),
                projection: None,
                row_count: None,
            };

            let inner_sort_expr = vec![SortExpr {
                expr: Expr::Column {
                    table: None,
                    name: "id".to_string(),
                    index: Some(0),
                },
                asc: true,
                nulls_first: false,
            }];

            let outer_sort_expr = vec![SortExpr {
                expr: Expr::Column {
                    table: None,
                    name: "name".to_string(),
                    index: Some(1),
                },
                asc: false,
                nulls_first: true,
            }];

            let inner_sort = PhysicalPlan::Sort {
                input: Box::new(table_scan),
                sort_exprs: inner_sort_expr,
                hints: Default::default(),
            };

            let outer_sort = PhysicalPlan::Sort {
                input: Box::new(inner_sort),
                sort_exprs: outer_sort_expr,
                hints: Default::default(),
            };

            let optimized = apply_sort_elimination(outer_sort);

            let mut sort_count = 0;
            count_sorts(&optimized, &mut sort_count);
            assert_eq!(
                sort_count, 2,
                "Different sort orders should both be preserved"
            );
        }
    }

    mod limit_pushdown {
        use super::*;

        #[test]
        fn limit_pushed_through_project() {
            let plan = optimize_sql_default("SELECT id, amount FROM orders LIMIT 10");

            assert_plan!(
                plan,
                Project {
                    input: (Limit {
                        input: (TableScan {
                            table_name: "orders"
                        })
                    })
                }
            );
        }

        #[test]
        fn limit_pushed_through_union_all() {
            let plan = optimize_sql_default(
                "SELECT id FROM orders
                 UNION ALL
                 SELECT id FROM customers
                 LIMIT 10",
            );

            match &plan {
                PhysicalPlan::Limit { input, .. } => match input.as_ref() {
                    PhysicalPlan::Union { inputs, all, .. } => {
                        assert!(*all, "Expected UNION ALL");
                        for branch in inputs {
                            assert!(
                                matches!(branch, PhysicalPlan::Limit { .. }),
                                "Expected Limit pushed to each branch"
                            );
                        }
                    }
                    other => panic!("Expected Union, got {:?}", other),
                },
                other => panic!("Expected Limit at top, got {:?}", other),
            }
        }

        #[test]
        fn limit_not_pushed_through_union_distinct() {
            let plan = optimize_sql_default(
                "SELECT id FROM orders
                 UNION
                 SELECT id FROM customers
                 LIMIT 10",
            );

            match &plan {
                PhysicalPlan::Limit { input, .. } => match input.as_ref() {
                    PhysicalPlan::Union { inputs, all, .. } => {
                        assert!(!*all, "Expected UNION (not ALL)");
                        for branch in inputs {
                            assert!(
                                !matches!(branch, PhysicalPlan::Limit { .. }),
                                "Limit should not be pushed through UNION DISTINCT"
                            );
                        }
                    }
                    other => panic!("Expected Union, got {:?}", other),
                },
                other => panic!("Expected Limit at top, got {:?}", other),
            }
        }

        #[test]
        fn limit_with_offset_adds_both_to_union_branches() {
            let plan = optimize_sql_default(
                "SELECT id FROM orders
                 UNION ALL
                 SELECT id FROM customers
                 LIMIT 10 OFFSET 5",
            );

            match &plan {
                PhysicalPlan::Limit {
                    input,
                    limit,
                    offset,
                } => {
                    assert_eq!(*limit, Some(10));
                    assert_eq!(*offset, Some(5));
                    match input.as_ref() {
                        PhysicalPlan::Union { inputs, .. } => {
                            for branch in inputs {
                                match branch {
                                    PhysicalPlan::Limit {
                                        limit: branch_limit,
                                        ..
                                    } => {
                                        assert_eq!(
                                            *branch_limit,
                                            Some(15),
                                            "Branch limit should be limit + offset"
                                        );
                                    }
                                    _ => panic!("Expected Limit in branch"),
                                }
                            }
                        }
                        other => panic!("Expected Union, got {:?}", other),
                    }
                }
                other => panic!("Expected Limit at top, got {:?}", other),
            }
        }

        #[test]
        fn limit_not_pushed_through_aggregate() {
            let plan =
                optimize_sql_default("SELECT status, COUNT(*) FROM orders GROUP BY status LIMIT 5");

            match &plan {
                PhysicalPlan::Project { input, .. } => match input.as_ref() {
                    PhysicalPlan::Limit { input, .. } => {
                        assert!(
                            matches!(input.as_ref(), PhysicalPlan::HashAggregate { .. }),
                            "Limit should not be pushed through aggregate"
                        );
                    }
                    other => panic!("Expected Limit, got {:?}", other),
                },
                other => panic!("Expected Project, got {:?}", other),
            }
        }
    }

    mod predicate_simplification {
        use yachtsql_ir::{BinaryOp, Expr};

        use super::*;

        #[test]
        fn in_single_element_becomes_equality() {
            let plan = optimize_sql_default("SELECT * FROM orders WHERE id IN (1)");

            match &plan {
                PhysicalPlan::Project { input, .. } => match input.as_ref() {
                    PhysicalPlan::Filter { predicate, .. } => match predicate {
                        Expr::BinaryOp { op, .. } => {
                            assert_eq!(*op, BinaryOp::Eq, "IN (1) should become = 1");
                        }
                        other => panic!("Expected BinaryOp Eq, got {:?}", other),
                    },
                    other => panic!("Expected Filter, got {:?}", other),
                },
                other => panic!("Expected Project, got {:?}", other),
            }
        }

        #[test]
        fn in_multiple_elements_preserved() {
            let plan = optimize_sql_default("SELECT * FROM orders WHERE id IN (1, 2, 3)");

            match &plan {
                PhysicalPlan::Project { input, .. } => match input.as_ref() {
                    PhysicalPlan::Filter { predicate, .. } => {
                        assert!(
                            matches!(predicate, Expr::InList { .. }),
                            "IN (1,2,3) should remain as InList"
                        );
                    }
                    other => panic!("Expected Filter, got {:?}", other),
                },
                other => panic!("Expected Project, got {:?}", other),
            }
        }

        #[test]
        fn not_in_single_element_becomes_not_equal() {
            let plan = optimize_sql_default("SELECT * FROM orders WHERE id NOT IN (1)");

            match &plan {
                PhysicalPlan::Project { input, .. } => match input.as_ref() {
                    PhysicalPlan::Filter { predicate, .. } => match predicate {
                        Expr::BinaryOp { op, .. } => {
                            assert_eq!(*op, BinaryOp::NotEq, "NOT IN (1) should become != 1");
                        }
                        other => panic!("Expected BinaryOp NotEq, got {:?}", other),
                    },
                    other => panic!("Expected Filter, got {:?}", other),
                },
                other => panic!("Expected Project, got {:?}", other),
            }
        }

        #[test]
        fn between_equal_bounds_becomes_equality() {
            let plan =
                optimize_sql_default("SELECT * FROM orders WHERE amount BETWEEN 100 AND 100");

            match &plan {
                PhysicalPlan::Project { input, .. } => match input.as_ref() {
                    PhysicalPlan::Filter { predicate, .. } => match predicate {
                        Expr::BinaryOp { op, .. } => {
                            assert_eq!(
                                *op,
                                BinaryOp::Eq,
                                "BETWEEN 100 AND 100 should become = 100"
                            );
                        }
                        other => panic!("Expected BinaryOp Eq, got {:?}", other),
                    },
                    other => panic!("Expected Filter, got {:?}", other),
                },
                other => panic!("Expected Project, got {:?}", other),
            }
        }

        #[test]
        fn between_different_bounds_preserved() {
            let plan =
                optimize_sql_default("SELECT * FROM orders WHERE amount BETWEEN 100 AND 200");

            match &plan {
                PhysicalPlan::Project { input, .. } => match input.as_ref() {
                    PhysicalPlan::Filter { predicate, .. } => {
                        assert!(
                            matches!(predicate, Expr::Between { .. }),
                            "BETWEEN with different bounds should remain"
                        );
                    }
                    other => panic!("Expected Filter, got {:?}", other),
                },
                other => panic!("Expected Project, got {:?}", other),
            }
        }
    }

    mod short_circuit_ordering {
        use yachtsql_ir::{BinaryOp, Expr};
        use yachtsql_parser::parse_and_plan;

        use super::*;
        use crate::test_utils::test_catalog;
        use crate::{OptimizationLevel, OptimizerSettings, optimize_with_settings};

        fn optimize_aggressive(sql: &str) -> PhysicalPlan {
            let catalog = test_catalog();
            let logical = parse_and_plan(sql, &catalog).expect("failed to parse SQL");
            let settings = OptimizerSettings::with_level(OptimizationLevel::Aggressive);
            optimize_with_settings(&logical, &settings).expect("failed to optimize")
        }

        #[test]
        fn and_predicates_reordered_by_selectivity() {
            let plan = optimize_aggressive(
                "SELECT * FROM orders WHERE status IS NOT NULL AND id = 1 AND amount IS NULL",
            );

            match &plan {
                PhysicalPlan::Project { input, .. } => match input.as_ref() {
                    PhysicalPlan::Filter { predicate, .. } => {
                        let predicates = collect_and_predicates(predicate);
                        assert!(predicates.len() >= 2, "Expected multiple AND predicates");
                        if let Some(first) = predicates.first() {
                            assert!(
                                matches!(first, Expr::IsNull { negated: false, .. })
                                    || matches!(
                                        first,
                                        Expr::BinaryOp {
                                            op: BinaryOp::Eq,
                                            ..
                                        }
                                    ),
                                "Most selective predicate (IS NULL or =) should be first"
                            );
                        }
                    }
                    other => panic!("Expected Filter, got {:?}", other),
                },
                other => panic!("Expected Project, got {:?}", other),
            }
        }

        fn collect_and_predicates(expr: &Expr) -> Vec<&Expr> {
            match expr {
                Expr::BinaryOp {
                    left,
                    op: BinaryOp::And,
                    right,
                } => {
                    let mut result = collect_and_predicates(left);
                    result.push(right.as_ref());
                    result
                }
                other => vec![other],
            }
        }

        #[test]
        fn or_predicates_reordered_by_selectivity() {
            let plan = optimize_aggressive(
                "SELECT * FROM orders WHERE amount IS NULL OR status IS NOT NULL OR id = 1",
            );

            match &plan {
                PhysicalPlan::Project { input, .. } => match input.as_ref() {
                    PhysicalPlan::Filter { predicate, .. } => {
                        let predicates = collect_or_predicates(predicate);
                        assert!(predicates.len() >= 2, "Expected multiple OR predicates");
                        if let Some(first) = predicates.first() {
                            assert!(
                                matches!(first, Expr::IsNull { negated: true, .. }),
                                "Least selective predicate (IS NOT NULL) should be first for OR"
                            );
                        }
                    }
                    other => panic!("Expected Filter, got {:?}", other),
                },
                other => panic!("Expected Project, got {:?}", other),
            }
        }

        fn collect_or_predicates(expr: &Expr) -> Vec<&Expr> {
            match expr {
                Expr::BinaryOp {
                    left,
                    op: BinaryOp::Or,
                    right,
                } => {
                    let mut result = collect_or_predicates(left);
                    result.push(right.as_ref());
                    result
                }
                other => vec![other],
            }
        }

        #[test]
        fn not_applied_at_standard_level() {
            let plan =
                optimize_sql_default("SELECT * FROM orders WHERE status IS NOT NULL AND id = 1");

            match &plan {
                PhysicalPlan::Project { input, .. } => match input.as_ref() {
                    PhysicalPlan::Filter { predicate, .. } => {
                        let predicates = collect_and_predicates(predicate);
                        assert!(predicates.len() >= 2, "Expected multiple AND predicates");
                    }
                    other => panic!("Expected Filter, got {:?}", other),
                },
                other => panic!("Expected Project, got {:?}", other),
            }
        }
    }

    mod trivial_predicate_removal {
        use super::*;

        #[test]
        fn where_true_removed() {
            let plan = optimize_sql_default("SELECT * FROM orders WHERE true");

            assert_plan!(
                plan,
                Project {
                    input: (TableScan {
                        table_name: "orders"
                    })
                }
            );
        }

        #[test]
        fn where_false_becomes_empty() {
            let plan = optimize_sql_default("SELECT * FROM orders WHERE false");

            fn is_empty(plan: &PhysicalPlan) -> bool {
                match plan {
                    PhysicalPlan::Empty { .. } => true,
                    PhysicalPlan::Project { input, .. } => is_empty(input),
                    _ => false,
                }
            }

            assert!(
                is_empty(&plan),
                "WHERE false should become Empty, got {:?}",
                plan
            );
        }
    }

    mod empty_propagation {
        use super::*;

        #[test]
        fn empty_union_branch_removed() {
            let plan = optimize_sql_default(
                "SELECT id FROM orders WHERE false
                 UNION ALL
                 SELECT id FROM customers",
            );

            fn find_table_scan(plan: &PhysicalPlan) -> Option<&str> {
                match plan {
                    PhysicalPlan::TableScan { table_name, .. } => Some(table_name),
                    PhysicalPlan::Project { input, .. } => find_table_scan(input),
                    PhysicalPlan::Limit { input, .. } => find_table_scan(input),
                    _ => None,
                }
            }

            match &plan {
                PhysicalPlan::Project { input, .. } => match input.as_ref() {
                    PhysicalPlan::TableScan { table_name, .. } => {
                        assert!(
                            table_name.eq_ignore_ascii_case("customers"),
                            "If only one branch left, should be customers, got {}",
                            table_name
                        );
                    }
                    other => {
                        if let Some(name) = find_table_scan(other) {
                            assert!(
                                name.eq_ignore_ascii_case("customers"),
                                "Should have customers table"
                            );
                        } else {
                            panic!("Expected TableScan somewhere in plan, got {:?}", other);
                        }
                    }
                },
                PhysicalPlan::TableScan { table_name, .. } => {
                    assert!(
                        table_name.eq_ignore_ascii_case("customers"),
                        "Should be customers table"
                    );
                }
                other => {
                    if let Some(name) = find_table_scan(other) {
                        assert!(
                            name.eq_ignore_ascii_case("customers"),
                            "Should have customers table"
                        );
                    } else {
                        panic!("Expected plan with customers table, got {:?}", other);
                    }
                }
            }
        }

        #[test]
        fn inner_join_with_empty_becomes_empty() {
            let plan = optimize_sql_default(
                "SELECT o.id, c.name
                 FROM orders o
                 JOIN (SELECT * FROM customers WHERE false) c ON o.customer_id = c.id",
            );

            fn is_empty(plan: &PhysicalPlan) -> bool {
                match plan {
                    PhysicalPlan::Empty { .. } => true,
                    PhysicalPlan::Project { input, .. } => is_empty(input),
                    _ => false,
                }
            }

            assert!(
                is_empty(&plan),
                "Inner join with empty should become empty, got {:?}",
                plan
            );
        }
    }

    mod outer_to_inner_join {
        use super::*;

        #[test]
        fn left_join_with_is_not_null_becomes_inner() {
            let plan = optimize_sql_default(
                "SELECT o.id, c.name
                 FROM orders o
                 LEFT JOIN customers c ON o.customer_id = c.id
                 WHERE c.id IS NOT NULL",
            );

            fn find_inner_join(plan: &PhysicalPlan) -> bool {
                match plan {
                    PhysicalPlan::HashJoin { join_type, .. } => *join_type == JoinType::Inner,
                    PhysicalPlan::Project { input, .. } => find_inner_join(input),
                    PhysicalPlan::Filter { input, .. } => find_inner_join(input),
                    _ => false,
                }
            }

            assert!(
                find_inner_join(&plan),
                "LEFT JOIN should become INNER when filtering on right side IS NOT NULL, got {:?}",
                plan
            );
        }

        #[test]
        fn left_join_with_equality_on_right_becomes_inner() {
            let plan = optimize_sql_default(
                "SELECT o.id, c.name
                 FROM orders o
                 LEFT JOIN customers c ON o.customer_id = c.id
                 WHERE c.country = 'USA'",
            );

            fn find_inner_join(plan: &PhysicalPlan) -> bool {
                match plan {
                    PhysicalPlan::HashJoin { join_type, .. } => *join_type == JoinType::Inner,
                    PhysicalPlan::Project { input, .. } => find_inner_join(input),
                    PhysicalPlan::Filter { input, .. } => find_inner_join(input),
                    _ => false,
                }
            }

            assert!(
                find_inner_join(&plan),
                "LEFT JOIN should become INNER when filtering on right side, got {:?}",
                plan
            );
        }

        #[test]
        fn left_join_with_is_null_stays_left() {
            let plan = optimize_sql_default(
                "SELECT o.id, c.name
                 FROM orders o
                 LEFT JOIN customers c ON o.customer_id = c.id
                 WHERE c.id IS NULL",
            );

            assert_plan!(
                plan,
                Project {
                    input: (Filter {
                        input: (HashJoin {
                            left: (TableScan {
                                table_name: "orders"
                            }),
                            right: (TableScan {
                                table_name: "customers"
                            }),
                            join_type: JoinType::Left
                        })
                    })
                }
            );
        }

        #[test]
        fn left_join_with_filter_on_left_only_stays_left() {
            let plan = optimize_sql_default(
                "SELECT o.id, c.name
                 FROM orders o
                 LEFT JOIN customers c ON o.customer_id = c.id
                 WHERE o.amount > 100",
            );

            assert_plan!(
                plan,
                Project {
                    input: (HashJoin {
                        left: (Filter {
                            input: (TableScan {
                                table_name: "orders"
                            })
                        }),
                        right: (TableScan {
                            table_name: "customers"
                        }),
                        join_type: JoinType::Left
                    })
                }
            );
        }

        #[test]
        fn left_join_with_comparison_on_right_becomes_inner() {
            let plan = optimize_sql_default(
                "SELECT o.id, c.name
                 FROM orders o
                 LEFT JOIN customers c ON o.customer_id = c.id
                 WHERE c.id > 5",
            );

            fn find_inner_join(plan: &PhysicalPlan) -> bool {
                match plan {
                    PhysicalPlan::HashJoin { join_type, .. } => *join_type == JoinType::Inner,
                    PhysicalPlan::Project { input, .. } => find_inner_join(input),
                    PhysicalPlan::Filter { input, .. } => find_inner_join(input),
                    _ => false,
                }
            }

            assert!(
                find_inner_join(&plan),
                "LEFT JOIN should become INNER when filtering on right side, got {:?}",
                plan
            );
        }
    }

    mod subquery_unnesting {
        use super::*;

        #[test]
        fn in_subquery_becomes_left_semi_join() {
            let plan = optimize_sql_default(
                "SELECT id, amount
                 FROM orders
                 WHERE customer_id IN (SELECT id FROM customers WHERE country = 'USA')",
            );

            assert_plan!(
                plan,
                Project {
                    input: (HashJoin {
                        left: (TableScan {
                            table_name: "orders"
                        }),
                        right: (_),
                        join_type: JoinType::LeftSemi
                    })
                }
            );
        }

        #[test]
        fn not_in_subquery_becomes_left_anti_join() {
            let plan = optimize_sql_default(
                "SELECT id, amount
                 FROM orders
                 WHERE customer_id NOT IN (SELECT id FROM customers WHERE country = 'USA')",
            );

            assert_plan!(
                plan,
                Project {
                    input: (HashJoin {
                        left: (TableScan {
                            table_name: "orders"
                        }),
                        right: (_),
                        join_type: JoinType::LeftAnti
                    })
                }
            );
        }

        #[test]
        fn in_subquery_with_additional_predicate() {
            let plan = optimize_sql_default(
                "SELECT id, amount
                 FROM orders
                 WHERE customer_id IN (SELECT id FROM customers) AND amount > 100",
            );

            assert_plan!(
                plan,
                Project {
                    input: (HashJoin {
                        left: (Filter {
                            input: (TableScan {
                                table_name: "orders"
                            })
                        }),
                        right: (_),
                        join_type: JoinType::LeftSemi
                    })
                }
            );
        }
    }

    mod join_elimination {
        use super::*;

        #[test]
        fn left_join_eliminated_when_right_unused() {
            let plan = optimize_sql_default(
                "SELECT o.id, o.amount
                 FROM orders o
                 LEFT JOIN customers c ON o.customer_id = c.id",
            );

            fn find_join(plan: &PhysicalPlan) -> bool {
                match plan {
                    PhysicalPlan::HashJoin { .. } => true,
                    PhysicalPlan::NestedLoopJoin { .. } => true,
                    PhysicalPlan::Project { input, .. } => find_join(input),
                    PhysicalPlan::Filter { input, .. } => find_join(input),
                    _ => false,
                }
            }

            assert!(
                !find_join(&plan),
                "LEFT JOIN should be eliminated when right side is unused, got {:?}",
                plan
            );
        }

        #[test]
        fn left_join_preserved_when_right_used() {
            let plan = optimize_sql_default(
                "SELECT o.id, c.name
                 FROM orders o
                 LEFT JOIN customers c ON o.customer_id = c.id",
            );

            fn find_join(plan: &PhysicalPlan) -> bool {
                match plan {
                    PhysicalPlan::HashJoin { .. } => true,
                    PhysicalPlan::NestedLoopJoin { .. } => true,
                    PhysicalPlan::Project { input, .. } => find_join(input),
                    PhysicalPlan::Filter { input, .. } => find_join(input),
                    _ => false,
                }
            }

            assert!(
                find_join(&plan),
                "LEFT JOIN should be preserved when right side is used, got {:?}",
                plan
            );
        }

        #[test]
        fn cross_join_preserved_even_when_side_unused() {
            let plan = optimize_sql_default(
                "SELECT o.id
                 FROM orders o
                 CROSS JOIN customers c",
            );

            fn find_cross_join(plan: &PhysicalPlan) -> bool {
                match plan {
                    PhysicalPlan::CrossJoin { .. } => true,
                    PhysicalPlan::Project { input, .. } => find_cross_join(input),
                    PhysicalPlan::Filter { input, .. } => find_cross_join(input),
                    _ => false,
                }
            }

            assert!(
                find_cross_join(&plan),
                "CROSS JOIN should be preserved (cardinality change), got {:?}",
                plan
            );
        }
    }

    mod filter_pushdown_aggregate {
        use super::*;

        #[test]
        fn filter_on_group_by_column_pushed_below_aggregate() {
            let plan = optimize_sql_default(
                "SELECT customer_id, SUM(amount) as total
                 FROM orders
                 GROUP BY customer_id
                 HAVING customer_id > 10",
            );

            assert_plan!(
                plan,
                Project {
                    input: (HashAggregate {
                        input: (Filter { input: (_) })
                    })
                }
            );
        }

        #[test]
        fn filter_on_aggregate_result_stays_above() {
            let plan = optimize_sql_default(
                "SELECT customer_id, SUM(amount) as total
                 FROM orders
                 GROUP BY customer_id
                 HAVING SUM(amount) > 1000",
            );

            assert_plan!(
                plan,
                Project {
                    input: (Filter {
                        input: (HashAggregate { input: (_) })
                    })
                }
            );
        }

        #[test]
        fn mixed_having_splits_predicates() {
            let plan = optimize_sql_default(
                "SELECT customer_id, SUM(amount) as total
                 FROM orders
                 GROUP BY customer_id
                 HAVING customer_id > 10 AND SUM(amount) > 1000",
            );

            assert_plan!(
                plan,
                Project {
                    input: (Filter {
                        input: (HashAggregate {
                            input: (Filter { input: (_) })
                        })
                    })
                }
            );
        }
    }

    mod distinct_elimination {
        use super::*;

        #[test]
        fn distinct_on_aggregate_with_group_by_preserved() {
            let plan = optimize_sql_default(
                "SELECT DISTINCT customer_id, COUNT(*)
                 FROM orders
                 GROUP BY customer_id",
            );

            assert_plan!(
                plan,
                Distinct {
                    input: (Project {
                        input: (HashAggregate { input: (_) })
                    })
                }
            );
        }

        #[test]
        fn distinct_on_limit_one_eliminated() {
            let plan =
                optimize_sql_default("SELECT DISTINCT * FROM (SELECT * FROM orders LIMIT 1) t");

            fn has_distinct(plan: &PhysicalPlan) -> bool {
                match plan {
                    PhysicalPlan::Distinct { .. } => true,
                    PhysicalPlan::Project { input, .. } => has_distinct(input),
                    PhysicalPlan::Filter { input, .. } => has_distinct(input),
                    PhysicalPlan::Limit { input, .. } => has_distinct(input),
                    _ => false,
                }
            }

            assert!(
                !has_distinct(&plan),
                "DISTINCT should be eliminated when input is LIMIT 1"
            );
        }

        #[test]
        fn distinct_on_regular_table_preserved() {
            let plan = optimize_sql_default("SELECT DISTINCT status FROM orders");

            assert_plan!(
                plan,
                Distinct {
                    input: (Project { input: (_) })
                }
            );
        }
    }

    mod project_merging {
        use super::*;

        #[test]
        fn nested_selects_merged_into_single_project() {
            let plan = optimize_sql_default(
                "SELECT id FROM (SELECT id, customer_id FROM (SELECT * FROM orders) t1) t2",
            );

            assert_plan!(
                plan,
                Project {
                    input: (TableScan {
                        table_name: "orders"
                    })
                }
            );
        }

        #[test]
        fn expression_composition_works() {
            let plan =
                optimize_sql_default("SELECT x * 2 FROM (SELECT amount + 1 AS x FROM orders) t");

            assert_plan!(
                plan,
                Project {
                    input: (TableScan {
                        table_name: "orders"
                    })
                }
            );
        }
    }

    mod topn_pushdown {
        use super::*;

        fn count_limits_in_union(plan: &PhysicalPlan) -> usize {
            match plan {
                PhysicalPlan::TopN { input, .. } | PhysicalPlan::Limit { input, .. } => {
                    1 + count_limits_in_union(input)
                }
                PhysicalPlan::Union {
                    inputs, all: true, ..
                } => inputs.iter().map(count_limits_in_union).sum(),
                PhysicalPlan::Project { input, .. }
                | PhysicalPlan::Sort { input, .. }
                | PhysicalPlan::Filter { input, .. } => count_limits_in_union(input),
                _ => 0,
            }
        }

        #[test]
        fn topn_pushed_through_union_all() {
            let plan = optimize_sql_default(
                "SELECT * FROM (
                    SELECT id, amount FROM orders
                    UNION ALL
                    SELECT id, amount FROM orders
                 ) t
                 ORDER BY amount
                 LIMIT 10",
            );

            let limit_count = count_limits_in_union(&plan);
            assert!(
                limit_count >= 1,
                "Should have TopN/Limit in plan with UNION ALL (got {} limit nodes)",
                limit_count
            );
        }

        #[test]
        fn union_distinct_query_has_limit() {
            let plan = optimize_sql_default(
                "SELECT * FROM (
                    SELECT id, amount FROM orders
                    UNION
                    SELECT id, amount FROM orders
                 ) t
                 ORDER BY amount
                 LIMIT 10",
            );

            fn has_limit_or_topn(plan: &PhysicalPlan) -> bool {
                match plan {
                    PhysicalPlan::TopN { .. } | PhysicalPlan::Limit { .. } => true,
                    PhysicalPlan::Project { input, .. }
                    | PhysicalPlan::Sort { input, .. }
                    | PhysicalPlan::Filter { input, .. }
                    | PhysicalPlan::Distinct { input } => has_limit_or_topn(input),
                    PhysicalPlan::Union { inputs, .. } => inputs.iter().any(has_limit_or_topn),
                    _ => false,
                }
            }

            assert!(
                has_limit_or_topn(&plan),
                "UNION DISTINCT query with LIMIT should have limit/topn in plan"
            );
        }
    }

    mod predicate_inference {
        use yachtsql_parser::parse_and_plan;

        use super::*;
        use crate::test_utils::test_catalog;
        use crate::{OptimizationLevel, OptimizerSettings, optimize_with_settings};

        fn optimize_aggressive(sql: &str) -> PhysicalPlan {
            let catalog = test_catalog();
            let logical = parse_and_plan(sql, &catalog).expect("failed to parse SQL");
            let settings = OptimizerSettings::with_level(OptimizationLevel::Aggressive);
            optimize_with_settings(&logical, &settings).expect("failed to optimize")
        }

        fn count_filters(plan: &PhysicalPlan) -> usize {
            match plan {
                PhysicalPlan::Filter { input, .. } => 1 + count_filters(input),
                PhysicalPlan::Project { input, .. } => count_filters(input),
                PhysicalPlan::HashJoin { left, right, .. } => {
                    count_filters(left) + count_filters(right)
                }
                _ => 0,
            }
        }

        #[test]
        fn transitivity_infers_predicates() {
            let plan = optimize_aggressive(
                "SELECT *
                 FROM orders o
                 JOIN customers c ON o.customer_id = c.id
                 WHERE o.customer_id = 5",
            );

            let filter_count = count_filters(&plan);
            assert!(
                filter_count >= 1,
                "Should have at least one filter after inference, got {}",
                filter_count
            );
        }
    }

    mod join_condition_decomposition {
        use super::*;
        use crate::test_utils::{is_eq_column_literal, is_gt_column_literal};

        #[test]
        fn left_join_with_right_side_filter_uses_hash_join() {
            let plan = optimize_sql_default(
                "SELECT c.id, o.amount
                 FROM customers c
                 LEFT JOIN orders o ON c.id = o.customer_id AND o.status = 'Completed'",
            );

            assert_plan!(
                plan,
                Project {
                    input: (HashJoin {
                        left: (TableScan {
                            table_name: "customers"
                        }),
                        right: (Filter {
                            input: (TableScan {
                                table_name: "orders"
                            }),
                            predicate: |p| is_eq_column_literal(p, "status", "Completed")
                        }),
                        join_type: JoinType::Left,
                        join_on: [("id", "customer_id")]
                    })
                }
            );
        }

        #[test]
        fn inner_join_with_mixed_conditions_uses_hash_join() {
            let plan = optimize_sql_default(
                "SELECT c.id, o.amount
                 FROM customers c
                 INNER JOIN orders o ON c.id = o.customer_id AND c.country = 'US' AND o.amount > 100",
            );

            assert_plan!(
                plan,
                Project {
                    input: (HashJoin {
                        left: (Filter {
                            input: (TableScan {
                                table_name: "customers"
                            }),
                            predicate: |p| is_eq_column_literal(p, "country", "US")
                        }),
                        right: (Filter {
                            input: (TableScan {
                                table_name: "orders"
                            }),
                            predicate: |p| is_gt_column_literal(p, "amount", 100)
                        }),
                        join_type: JoinType::Inner,
                        join_on: [("id", "customer_id")]
                    })
                }
            );
        }

        #[test]
        fn right_join_with_left_side_filter_uses_hash_join() {
            let plan = optimize_sql_default(
                "SELECT c.id, o.amount
                 FROM customers c
                 RIGHT JOIN orders o ON c.id = o.customer_id AND c.country = 'US'",
            );

            assert_plan!(
                plan,
                Project {
                    input: (HashJoin {
                        left: (Filter {
                            input: (TableScan {
                                table_name: "customers"
                            }),
                            predicate: |p| is_eq_column_literal(p, "country", "US")
                        }),
                        right: (TableScan {
                            table_name: "orders"
                        }),
                        join_type: JoinType::Right,
                        join_on: [("id", "customer_id")]
                    })
                }
            );
        }

        #[test]
        fn full_join_with_non_equi_predicate_uses_post_join_filter() {
            let plan = optimize_sql_default(
                "SELECT c.id, o.amount
                 FROM customers c
                 FULL OUTER JOIN orders o ON c.id = o.customer_id AND c.country = 'US'",
            );

            assert_plan!(
                plan,
                Project {
                    input: (Filter {
                        input: (HashJoin {
                            left: (TableScan {
                                table_name: "customers"
                            }),
                            right: (TableScan {
                                table_name: "orders"
                            }),
                            join_type: JoinType::Right,
                            join_on: [("id", "customer_id")]
                        }),
                        predicate: |p| is_eq_column_literal(p, "country", "US")
                    })
                }
            );
        }

        #[test]
        fn pure_equi_join_uses_hash_join_without_filters() {
            let plan = optimize_sql_default(
                "SELECT c.id, o.amount
                 FROM customers c
                 LEFT JOIN orders o ON c.id = o.customer_id",
            );

            assert_plan!(
                plan,
                Project {
                    input: (HashJoin {
                        left: (TableScan {
                            table_name: "customers"
                        }),
                        right: (TableScan {
                            table_name: "orders"
                        }),
                        join_type: JoinType::Left,
                        join_on: [("id", "customer_id")]
                    })
                }
            );
        }

        #[test]
        fn no_equi_keys_uses_nested_loop_join() {
            let plan = optimize_sql_default(
                "SELECT c.id, o.amount
                 FROM customers c
                 INNER JOIN orders o ON c.id > o.customer_id",
            );

            assert_plan!(
                plan,
                Project {
                    input: (NestedLoopJoin {
                        left: (TableScan {
                            table_name: "customers"
                        }),
                        right: (TableScan {
                            table_name: "orders"
                        }),
                        join_type: JoinType::Inner,
                        condition: ("id", ">", "customer_id")
                    })
                }
            );
        }

        #[test]
        fn multi_key_equi_join_uses_hash_join() {
            let plan = optimize_sql_default(
                "SELECT c.id, o.amount
                 FROM customers c
                 INNER JOIN orders o ON c.id = o.customer_id AND c.name = o.status",
            );

            assert_plan!(
                plan,
                Project {
                    input: (HashJoin {
                        left: (TableScan {
                            table_name: "customers"
                        }),
                        right: (TableScan {
                            table_name: "orders"
                        }),
                        join_type: JoinType::Inner,
                        join_on: [("id", "customer_id"), ("name", "status")]
                    })
                }
            );
        }
    }
}
