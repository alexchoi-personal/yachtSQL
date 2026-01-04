#![coverage(off)]

pub(crate) mod aggregate;
pub mod concurrent;
pub(crate) mod window;

use std::collections::HashMap;

use yachtsql_storage::{Field, FieldMode, Schema};

pub fn plan_schema_to_schema(plan_schema: &yachtsql_ir::PlanSchema) -> Schema {
    let mut schema = Schema::new();
    let mut name_counts: HashMap<String, usize> = HashMap::new();

    for field in &plan_schema.fields {
        let mode = if field.nullable {
            FieldMode::Nullable
        } else {
            FieldMode::Required
        };

        let base_name = &field.name;
        let count = name_counts.entry(base_name.clone()).or_insert(0);
        let storage_name = if *count > 0 {
            format!("{}_{}", base_name, count)
        } else {
            base_name.clone()
        };
        *count += 1;

        let mut storage_field = Field::new(&storage_name, field.data_type.clone(), mode);
        if let Some(ref table) = field.table {
            storage_field = storage_field.with_source_table(table.clone());
        }
        schema.add_field(storage_field);
    }
    schema
}
