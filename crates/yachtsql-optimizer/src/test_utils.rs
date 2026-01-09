use rustc_hash::FxHashMap;
use yachtsql_common::types::DataType;
use yachtsql_parser::{CatalogProvider, FunctionDefinition, ViewDefinition, parse_and_plan};
use yachtsql_storage::{Field, Schema};

use crate::{PhysicalPlan, optimize};

pub(crate) struct MockCatalog {
    tables: FxHashMap<String, Schema>,
}

impl MockCatalog {
    pub(crate) fn new() -> Self {
        Self {
            tables: FxHashMap::default(),
        }
    }

    pub(crate) fn with_table(mut self, name: &str, schema: Schema) -> Self {
        self.tables.insert(name.to_uppercase(), schema);
        self
    }
}

impl CatalogProvider for MockCatalog {
    fn get_table_schema(&self, name: &str) -> Option<Schema> {
        self.tables.get(&name.to_uppercase()).cloned()
    }

    fn get_view(&self, _name: &str) -> Option<ViewDefinition> {
        None
    }

    fn get_function(&self, _name: &str) -> Option<FunctionDefinition> {
        None
    }
}

fn orders_schema() -> Schema {
    Schema::from_fields(vec![
        Field::nullable("id", DataType::Int64),
        Field::nullable("customer_id", DataType::Int64),
        Field::nullable("amount", DataType::Float64),
        Field::nullable("status", DataType::String),
    ])
}

fn customers_schema() -> Schema {
    Schema::from_fields(vec![
        Field::nullable("id", DataType::Int64),
        Field::nullable("name", DataType::String),
        Field::nullable("country", DataType::String),
    ])
}

fn products_schema() -> Schema {
    Schema::from_fields(vec![
        Field::nullable("id", DataType::Int64),
        Field::nullable("name", DataType::String),
        Field::nullable("price", DataType::Float64),
        Field::nullable("category", DataType::String),
    ])
}

pub(crate) fn test_catalog() -> MockCatalog {
    MockCatalog::new()
        .with_table("orders", orders_schema())
        .with_table("customers", customers_schema())
        .with_table("products", products_schema())
}

pub(crate) fn optimize_sql<C: CatalogProvider>(sql: &str, catalog: &C) -> PhysicalPlan {
    let logical = parse_and_plan(sql, catalog).expect("failed to parse SQL");
    optimize(&logical).expect("failed to optimize plan")
}

pub(crate) fn optimize_sql_default(sql: &str) -> PhysicalPlan {
    optimize_sql(sql, &test_catalog())
}

#[allow(dead_code)]
pub(crate) fn get_plan_children(plan: &PhysicalPlan) -> Vec<&PhysicalPlan> {
    match plan {
        PhysicalPlan::Project { input, .. }
        | PhysicalPlan::Filter { input, .. }
        | PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::Limit { input, .. }
        | PhysicalPlan::TopN { input, .. }
        | PhysicalPlan::HashAggregate { input, .. }
        | PhysicalPlan::Distinct { input }
        | PhysicalPlan::Window { input, .. }
        | PhysicalPlan::Unnest { input, .. }
        | PhysicalPlan::Qualify { input, .. }
        | PhysicalPlan::Sample { input, .. }
        | PhysicalPlan::GapFill { input, .. } => vec![input.as_ref()],

        PhysicalPlan::HashJoin { left, right, .. }
        | PhysicalPlan::NestedLoopJoin { left, right, .. }
        | PhysicalPlan::CrossJoin { left, right, .. }
        | PhysicalPlan::Intersect { left, right, .. }
        | PhysicalPlan::Except { left, right, .. } => vec![left.as_ref(), right.as_ref()],

        PhysicalPlan::Union { inputs, .. } => inputs.iter().collect(),

        PhysicalPlan::WithCte { body, .. } => vec![body.as_ref()],

        _ => vec![],
    }
}

macro_rules! assert_plan {
    ($plan:expr, _) => {};

    ($plan:expr, TableScan { table_name: $name:expr }) => {
        match &$plan {
            PhysicalPlan::TableScan { table_name, .. } => {
                assert_eq!(table_name, $name, "TableScan table_name mismatch");
            }
            other => panic!(
                "Expected TableScan, got {:?}",
                std::mem::discriminant(other)
            ),
        }
    };

    ($plan:expr, TableScan { table_name: $name:expr, projection: $proj:expr }) => {
        match &$plan {
            PhysicalPlan::TableScan {
                table_name,
                projection,
                ..
            } => {
                assert_eq!(table_name, $name, "TableScan table_name mismatch");
                assert_eq!(projection, &$proj, "TableScan projection mismatch");
            }
            other => panic!(
                "Expected TableScan, got {:?}",
                std::mem::discriminant(other)
            ),
        }
    };

    ($plan:expr, Filter { input: ($($input:tt)+) }) => {
        match &$plan {
            PhysicalPlan::Filter { input, .. } => {
                let _ = &input;
                assert_plan!(**input, $($input)+);
            }
            other => panic!("Expected Filter, got {:?}", std::mem::discriminant(other)),
        }
    };

    ($plan:expr, Filter { input: ($($input:tt)+), predicate: _ }) => {
        match &$plan {
            PhysicalPlan::Filter { input, .. } => {
                let _ = &input;
                assert_plan!(**input, $($input)+);
            }
            other => panic!("Expected Filter, got {:?}", std::mem::discriminant(other)),
        }
    };

    ($plan:expr, Project { input: ($($input:tt)+) }) => {
        match &$plan {
            PhysicalPlan::Project { input, .. } => {
                let _ = &input;
                assert_plan!(**input, $($input)+);
            }
            other => panic!("Expected Project, got {:?}", std::mem::discriminant(other)),
        }
    };

    ($plan:expr, HashJoin { left: ($($left:tt)+), right: ($($right:tt)+), join_type: $jt:expr }) => {
        match &$plan {
            PhysicalPlan::HashJoin {
                left,
                right,
                join_type,
                ..
            } => {
                assert_eq!(*join_type, $jt, "HashJoin join_type mismatch");
                assert_plan!(**left, $($left)+);
                assert_plan!(**right, $($right)+);
            }
            other => panic!(
                "Expected HashJoin, got {:?}",
                std::mem::discriminant(other)
            ),
        }
    };

    ($plan:expr, HashJoin { left: ($($left:tt)+), right: ($($right:tt)+) }) => {
        match &$plan {
            PhysicalPlan::HashJoin { left, right, .. } => {
                assert_plan!(**left, $($left)+);
                assert_plan!(**right, $($right)+);
            }
            other => panic!(
                "Expected HashJoin, got {:?}",
                std::mem::discriminant(other)
            ),
        }
    };

    ($plan:expr, NestedLoopJoin { left: ($($left:tt)+), right: ($($right:tt)+), join_type: $jt:expr }) => {
        match &$plan {
            PhysicalPlan::NestedLoopJoin {
                left,
                right,
                join_type,
                ..
            } => {
                assert_eq!(*join_type, $jt, "NestedLoopJoin join_type mismatch");
                assert_plan!(**left, $($left)+);
                assert_plan!(**right, $($right)+);
            }
            other => panic!(
                "Expected NestedLoopJoin, got {:?}",
                std::mem::discriminant(other)
            ),
        }
    };

    ($plan:expr, CrossJoin { left: ($($left:tt)+), right: ($($right:tt)+) }) => {
        match &$plan {
            PhysicalPlan::CrossJoin { left, right, .. } => {
                assert_plan!(**left, $($left)+);
                assert_plan!(**right, $($right)+);
            }
            other => panic!(
                "Expected CrossJoin, got {:?}",
                std::mem::discriminant(other)
            ),
        }
    };

    ($plan:expr, Sort { input: ($($input:tt)+) }) => {
        match &$plan {
            PhysicalPlan::Sort { input, .. } => {
                assert_plan!(**input, $($input)+);
            }
            other => panic!("Expected Sort, got {:?}", std::mem::discriminant(other)),
        }
    };

    ($plan:expr, Limit { input: ($($input:tt)+) }) => {
        match &$plan {
            PhysicalPlan::Limit { input, .. } => {
                assert_plan!(**input, $($input)+);
            }
            other => panic!("Expected Limit, got {:?}", std::mem::discriminant(other)),
        }
    };

    ($plan:expr, Limit { input: ($($input:tt)+), limit: $lim:expr }) => {
        match &$plan {
            PhysicalPlan::Limit { input, limit, .. } => {
                assert_eq!(*limit, $lim, "Limit value mismatch");
                assert_plan!(**input, $($input)+);
            }
            other => panic!("Expected Limit, got {:?}", std::mem::discriminant(other)),
        }
    };

    ($plan:expr, TopN { input: ($($input:tt)+), limit: $lim:expr }) => {
        match &$plan {
            PhysicalPlan::TopN { input, limit, .. } => {
                assert_eq!(*limit, $lim, "TopN limit mismatch");
                assert_plan!(**input, $($input)+);
            }
            other => panic!("Expected TopN, got {:?}", std::mem::discriminant(other)),
        }
    };

    ($plan:expr, TopN { input: ($($input:tt)+) }) => {
        match &$plan {
            PhysicalPlan::TopN { input, .. } => {
                assert_plan!(**input, $($input)+);
            }
            other => panic!("Expected TopN, got {:?}", std::mem::discriminant(other)),
        }
    };

    ($plan:expr, HashAggregate { input: ($($input:tt)+) }) => {
        match &$plan {
            PhysicalPlan::HashAggregate { input, .. } => {
                let _ = &input;
                assert_plan!(**input, $($input)+);
            }
            other => panic!(
                "Expected HashAggregate, got {:?}",
                std::mem::discriminant(other)
            ),
        }
    };

    ($plan:expr, Distinct { input: ($($input:tt)+) }) => {
        match &$plan {
            PhysicalPlan::Distinct { input } => {
                let _ = &input;
                assert_plan!(**input, $($input)+);
            }
            other => panic!(
                "Expected Distinct, got {:?}",
                std::mem::discriminant(other)
            ),
        }
    };

    ($plan:expr, Union { inputs: [$($($input:tt)+),+] }) => {
        match &$plan {
            PhysicalPlan::Union { inputs, .. } => {
                let mut idx = 0;
                $(
                    assert_plan!(inputs[idx], $($input)+);
                    idx += 1;
                )+
                let _ = idx;
            }
            other => panic!(
                "Expected Union, got {:?}",
                std::mem::discriminant(other)
            ),
        }
    };

    ($plan:expr, Union { all: $all:expr }) => {
        match &$plan {
            PhysicalPlan::Union { all, .. } => {
                assert_eq!(*all, $all, "Union all mismatch");
            }
            other => panic!(
                "Expected Union, got {:?}",
                std::mem::discriminant(other)
            ),
        }
    };

    ($plan:expr, Empty) => {
        match &$plan {
            PhysicalPlan::Empty { .. } => {}
            other => panic!(
                "Expected Empty, got {:?}",
                std::mem::discriminant(other)
            ),
        }
    };

    ($plan:expr, Window { input: ($($input:tt)+) }) => {
        match &$plan {
            PhysicalPlan::Window { input, .. } => {
                assert_plan!(**input, $($input)+);
            }
            other => panic!(
                "Expected Window, got {:?}",
                std::mem::discriminant(other)
            ),
        }
    };

    ($plan:expr, contains HashJoin { join_type: $jt:expr }) => {
        fn find_join(plan: &PhysicalPlan, target: yachtsql_ir::JoinType) -> bool {
            match plan {
                PhysicalPlan::HashJoin { join_type, .. } if *join_type == target => true,
                PhysicalPlan::NestedLoopJoin { join_type, .. } if *join_type == target => true,
                _ => crate::test_utils::get_plan_children(plan).iter().any(|c| find_join(c, target)),
            }
        }
        assert!(
            find_join(&$plan, $jt),
            "Expected plan to contain HashJoin/NestedLoopJoin with join_type {:?}",
            $jt
        );
    };

    ($plan:expr, contains Distinct) => {
        fn find_distinct(plan: &PhysicalPlan) -> bool {
            match plan {
                PhysicalPlan::Distinct { .. } => true,
                _ => crate::test_utils::get_plan_children(plan).iter().any(|c| find_distinct(c)),
            }
        }
        assert!(find_distinct(&$plan), "Expected plan to contain Distinct");
    };

    ($plan:expr, contains Filter) => {
        fn find_filter(plan: &PhysicalPlan) -> bool {
            match plan {
                PhysicalPlan::Filter { .. } => true,
                _ => crate::test_utils::get_plan_children(plan).iter().any(|c| find_filter(c)),
            }
        }
        assert!(find_filter(&$plan), "Expected plan to contain Filter");
    };

    ($plan:expr, contains HashAggregate) => {
        fn find_agg(plan: &PhysicalPlan) -> bool {
            match plan {
                PhysicalPlan::HashAggregate { .. } => true,
                _ => crate::test_utils::get_plan_children(plan).iter().any(|c| find_agg(c)),
            }
        }
        assert!(find_agg(&$plan), "Expected plan to contain HashAggregate");
    };

    ($plan:expr, contains TopN) => {
        fn find_topn(plan: &PhysicalPlan) -> bool {
            match plan {
                PhysicalPlan::TopN { .. } => true,
                _ => crate::test_utils::get_plan_children(plan).iter().any(|c| find_topn(c)),
            }
        }
        assert!(find_topn(&$plan), "Expected plan to contain TopN");
    };

    ($plan:expr, contains Limit) => {
        fn find_limit(plan: &PhysicalPlan) -> bool {
            match plan {
                PhysicalPlan::Limit { .. } => true,
                _ => crate::test_utils::get_plan_children(plan).iter().any(|c| find_limit(c)),
            }
        }
        assert!(find_limit(&$plan), "Expected plan to contain Limit");
    };

    ($plan:expr, not_contains Distinct) => {
        fn find_distinct(plan: &PhysicalPlan) -> bool {
            match plan {
                PhysicalPlan::Distinct { .. } => true,
                _ => crate::test_utils::get_plan_children(plan).iter().any(|c| find_distinct(c)),
            }
        }
        assert!(!find_distinct(&$plan), "Expected plan to NOT contain Distinct");
    };

    ($plan:expr, not_contains Filter) => {
        fn find_filter(plan: &PhysicalPlan) -> bool {
            match plan {
                PhysicalPlan::Filter { .. } => true,
                _ => crate::test_utils::get_plan_children(plan).iter().any(|c| find_filter(c)),
            }
        }
        assert!(!find_filter(&$plan), "Expected plan to NOT contain Filter");
    };
}

pub(crate) use assert_plan;
