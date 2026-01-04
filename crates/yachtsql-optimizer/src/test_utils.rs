use rustc_hash::FxHashMap;
use yachtsql_common::types::DataType;
use yachtsql_parser::{CatalogProvider, FunctionDefinition, ViewDefinition, parse_and_plan};
use yachtsql_storage::{Field, Schema};

use crate::{OptimizedLogicalPlan, optimize};

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

pub(crate) fn optimize_sql<C: CatalogProvider>(sql: &str, catalog: &C) -> OptimizedLogicalPlan {
    let logical = parse_and_plan(sql, catalog).expect("failed to parse SQL");
    optimize(&logical).expect("failed to optimize plan")
}

pub(crate) fn optimize_sql_default(sql: &str) -> OptimizedLogicalPlan {
    optimize_sql(sql, &test_catalog())
}

macro_rules! assert_plan {
    ($plan:expr, _) => {};

    ($plan:expr, TableScan { table_name: $name:expr }) => {
        match &$plan {
            OptimizedLogicalPlan::TableScan { table_name, .. } => {
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
            OptimizedLogicalPlan::TableScan {
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
            OptimizedLogicalPlan::Filter { input, .. } => {
                assert_plan!(**input, $($input)+);
            }
            other => panic!("Expected Filter, got {:?}", std::mem::discriminant(other)),
        }
    };

    ($plan:expr, Filter { input: ($($input:tt)+), predicate: _ }) => {
        match &$plan {
            OptimizedLogicalPlan::Filter { input, .. } => {
                assert_plan!(**input, $($input)+);
            }
            other => panic!("Expected Filter, got {:?}", std::mem::discriminant(other)),
        }
    };

    ($plan:expr, Project { input: ($($input:tt)+) }) => {
        match &$plan {
            OptimizedLogicalPlan::Project { input, .. } => {
                assert_plan!(**input, $($input)+);
            }
            other => panic!("Expected Project, got {:?}", std::mem::discriminant(other)),
        }
    };

    ($plan:expr, HashJoin { left: ($($left:tt)+), right: ($($right:tt)+), join_type: $jt:expr }) => {
        match &$plan {
            OptimizedLogicalPlan::HashJoin {
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
            OptimizedLogicalPlan::HashJoin { left, right, .. } => {
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
            OptimizedLogicalPlan::NestedLoopJoin {
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
            OptimizedLogicalPlan::CrossJoin { left, right, .. } => {
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
            OptimizedLogicalPlan::Sort { input, .. } => {
                assert_plan!(**input, $($input)+);
            }
            other => panic!("Expected Sort, got {:?}", std::mem::discriminant(other)),
        }
    };

    ($plan:expr, Limit { input: ($($input:tt)+) }) => {
        match &$plan {
            OptimizedLogicalPlan::Limit { input, .. } => {
                assert_plan!(**input, $($input)+);
            }
            other => panic!("Expected Limit, got {:?}", std::mem::discriminant(other)),
        }
    };

    ($plan:expr, Limit { input: ($($input:tt)+), limit: $lim:expr }) => {
        match &$plan {
            OptimizedLogicalPlan::Limit { input, limit, .. } => {
                assert_eq!(*limit, $lim, "Limit value mismatch");
                assert_plan!(**input, $($input)+);
            }
            other => panic!("Expected Limit, got {:?}", std::mem::discriminant(other)),
        }
    };

    ($plan:expr, TopN { input: ($($input:tt)+), limit: $lim:expr }) => {
        match &$plan {
            OptimizedLogicalPlan::TopN { input, limit, .. } => {
                assert_eq!(*limit, $lim, "TopN limit mismatch");
                assert_plan!(**input, $($input)+);
            }
            other => panic!("Expected TopN, got {:?}", std::mem::discriminant(other)),
        }
    };

    ($plan:expr, TopN { input: ($($input:tt)+) }) => {
        match &$plan {
            OptimizedLogicalPlan::TopN { input, .. } => {
                assert_plan!(**input, $($input)+);
            }
            other => panic!("Expected TopN, got {:?}", std::mem::discriminant(other)),
        }
    };

    ($plan:expr, HashAggregate { input: ($($input:tt)+) }) => {
        match &$plan {
            OptimizedLogicalPlan::HashAggregate { input, .. } => {
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
            OptimizedLogicalPlan::Distinct { input } => {
                assert_plan!(**input, $($input)+);
            }
            other => panic!(
                "Expected Distinct, got {:?}",
                std::mem::discriminant(other)
            ),
        }
    };
}

pub(crate) use assert_plan;
