#![coverage(off)]

use serde::{Deserialize, Serialize};
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::{DataType, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum FieldMode {
    #[default]
    Nullable,
    Required,
    Repeated,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Field {
    pub name: String,
    pub data_type: DataType,
    pub mode: FieldMode,
    pub description: Option<String>,
    pub source_table: Option<String>,
    pub default_value: Option<Value>,
    pub collation: Option<String>,
}

impl Field {
    pub fn new(name: impl Into<String>, data_type: DataType, mode: FieldMode) -> Self {
        Self {
            name: name.into(),
            data_type,
            mode,
            description: None,
            source_table: None,
            default_value: None,
            collation: None,
        }
    }

    pub fn nullable(name: impl Into<String>, data_type: DataType) -> Self {
        Self::new(name, data_type, FieldMode::Nullable)
    }

    pub fn required(name: impl Into<String>, data_type: DataType) -> Self {
        Self::new(name, data_type, FieldMode::Required)
    }

    pub fn repeated(name: impl Into<String>, data_type: DataType) -> Self {
        Self::new(name, data_type, FieldMode::Repeated)
    }

    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    pub fn with_source_table(mut self, table: impl Into<String>) -> Self {
        self.source_table = Some(table.into());
        self
    }

    pub fn with_default(mut self, default_value: Value) -> Self {
        self.default_value = Some(default_value);
        self
    }

    pub fn with_collation(mut self, collation: impl Into<String>) -> Self {
        self.collation = Some(collation.into());
        self
    }

    pub fn is_nullable(&self) -> bool {
        self.mode == FieldMode::Nullable
    }

    pub fn is_repeated(&self) -> bool {
        self.mode == FieldMode::Repeated
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Schema {
    fields: Vec<Field>,
}

impl Schema {
    pub fn new() -> Self {
        Self { fields: Vec::new() }
    }

    pub fn from_fields(fields: Vec<Field>) -> Self {
        Self { fields }
    }

    pub fn add_field(&mut self, field: Field) {
        self.fields.push(field);
    }

    pub fn remove_field(&mut self, index: usize) {
        if index < self.fields.len() {
            self.fields.remove(index);
        }
    }

    pub fn rename_field(&mut self, index: usize, new_name: String) {
        if let Some(field) = self.fields.get_mut(index) {
            field.name = new_name;
        }
    }

    pub fn fields(&self) -> &[Field] {
        &self.fields
    }

    pub fn field(&self, name: &str) -> Option<&Field> {
        self.fields.iter().find(|f| f.name == name)
    }

    pub fn field_index(&self, name: &str) -> Option<usize> {
        self.fields.iter().position(|f| f.name == name)
    }

    pub fn field_index_qualified(&self, name: &str, table: Option<&str>) -> Option<usize> {
        match table {
            Some(tbl) => {
                let qualified_name = format!("{}.{}", tbl, name);
                if let Some(idx) = self
                    .fields
                    .iter()
                    .position(|f| f.name.eq_ignore_ascii_case(&qualified_name))
                {
                    return Some(idx);
                }
                self.fields.iter().position(|f| {
                    f.name.eq_ignore_ascii_case(name)
                        && f.source_table.as_ref().is_some_and(|src| {
                            src.eq_ignore_ascii_case(tbl)
                                || src
                                    .to_lowercase()
                                    .ends_with(&format!(".{}", tbl.to_lowercase()))
                        })
                })
            }
            None => self.field_index(name),
        }
    }

    pub fn field_count(&self) -> usize {
        self.fields.len()
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    pub fn validate(&self) -> Result<()> {
        let mut seen = std::collections::HashSet::new();
        for field in &self.fields {
            if !seen.insert(&field.name) {
                return Err(Error::schema_mismatch(format!(
                    "Duplicate field name: {}",
                    field.name
                )));
            }
        }
        Ok(())
    }

    pub fn is_compatible_with(&self, other: &Schema) -> bool {
        if self.fields.len() != other.fields.len() {
            return false;
        }

        for (f1, f2) in self.fields.iter().zip(other.fields.iter()) {
            if f1.name != f2.name || f1.data_type != f2.data_type {
                return false;
            }
        }

        true
    }
}

impl Default for Schema {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_field_mode_default() {
        let mode: FieldMode = Default::default();
        assert_eq!(mode, FieldMode::Nullable);
    }

    #[test]
    fn test_field_mode_equality() {
        assert_eq!(FieldMode::Nullable, FieldMode::Nullable);
        assert_eq!(FieldMode::Required, FieldMode::Required);
        assert_eq!(FieldMode::Repeated, FieldMode::Repeated);
        assert_ne!(FieldMode::Nullable, FieldMode::Required);
        assert_ne!(FieldMode::Nullable, FieldMode::Repeated);
        assert_ne!(FieldMode::Required, FieldMode::Repeated);
    }

    #[test]
    fn test_field_new() {
        let field = Field::new("test_field", DataType::Int64, FieldMode::Required);
        assert_eq!(field.name, "test_field");
        assert_eq!(field.data_type, DataType::Int64);
        assert_eq!(field.mode, FieldMode::Required);
        assert!(field.description.is_none());
        assert!(field.source_table.is_none());
        assert!(field.default_value.is_none());
        assert!(field.collation.is_none());
    }

    #[test]
    fn test_field_nullable() {
        let field = Field::nullable("nullable_field", DataType::String);
        assert_eq!(field.name, "nullable_field");
        assert_eq!(field.data_type, DataType::String);
        assert_eq!(field.mode, FieldMode::Nullable);
    }

    #[test]
    fn test_field_required() {
        let field = Field::required("required_field", DataType::Bool);
        assert_eq!(field.name, "required_field");
        assert_eq!(field.data_type, DataType::Bool);
        assert_eq!(field.mode, FieldMode::Required);
    }

    #[test]
    fn test_field_repeated() {
        let field = Field::repeated("repeated_field", DataType::Float64);
        assert_eq!(field.name, "repeated_field");
        assert_eq!(field.data_type, DataType::Float64);
        assert_eq!(field.mode, FieldMode::Repeated);
    }

    #[test]
    fn test_field_with_description() {
        let field = Field::nullable("field", DataType::Int64).with_description("A test field");
        assert_eq!(field.description, Some("A test field".to_string()));
    }

    #[test]
    fn test_field_with_source_table() {
        let field = Field::nullable("field", DataType::Int64).with_source_table("users");
        assert_eq!(field.source_table, Some("users".to_string()));
    }

    #[test]
    fn test_field_with_default() {
        let field = Field::nullable("field", DataType::Int64).with_default(Value::Int64(42));
        assert_eq!(field.default_value, Some(Value::Int64(42)));
    }

    #[test]
    fn test_field_with_collation() {
        let field = Field::nullable("field", DataType::String).with_collation("unicode:ci");
        assert_eq!(field.collation, Some("unicode:ci".to_string()));
    }

    #[test]
    fn test_field_builder_chain() {
        let field = Field::nullable("name", DataType::String)
            .with_description("User name")
            .with_source_table("users")
            .with_default(Value::String("unknown".to_string()))
            .with_collation("en_US");

        assert_eq!(field.name, "name");
        assert_eq!(field.data_type, DataType::String);
        assert_eq!(field.mode, FieldMode::Nullable);
        assert_eq!(field.description, Some("User name".to_string()));
        assert_eq!(field.source_table, Some("users".to_string()));
        assert_eq!(
            field.default_value,
            Some(Value::String("unknown".to_string()))
        );
        assert_eq!(field.collation, Some("en_US".to_string()));
    }

    #[test]
    fn test_field_is_nullable() {
        assert!(Field::nullable("f", DataType::Int64).is_nullable());
        assert!(!Field::required("f", DataType::Int64).is_nullable());
        assert!(!Field::repeated("f", DataType::Int64).is_nullable());
    }

    #[test]
    fn test_field_is_repeated() {
        assert!(!Field::nullable("f", DataType::Int64).is_repeated());
        assert!(!Field::required("f", DataType::Int64).is_repeated());
        assert!(Field::repeated("f", DataType::Int64).is_repeated());
    }

    #[test]
    fn test_schema_new() {
        let schema = Schema::new();
        assert_eq!(schema.field_count(), 0);
        assert!(schema.is_empty());
    }

    #[test]
    fn test_schema_default() {
        let schema: Schema = Default::default();
        assert_eq!(schema.field_count(), 0);
        assert!(schema.is_empty());
    }

    #[test]
    fn test_schema_from_fields() {
        let fields = vec![
            Field::nullable("id", DataType::Int64),
            Field::nullable("name", DataType::String),
        ];
        let schema = Schema::from_fields(fields);
        assert_eq!(schema.field_count(), 2);
        assert!(!schema.is_empty());
    }

    #[test]
    fn test_schema_add_field() {
        let mut schema = Schema::new();
        assert_eq!(schema.field_count(), 0);

        schema.add_field(Field::nullable("id", DataType::Int64));
        assert_eq!(schema.field_count(), 1);

        schema.add_field(Field::nullable("name", DataType::String));
        assert_eq!(schema.field_count(), 2);
    }

    #[test]
    fn test_schema_remove_field() {
        let mut schema = Schema::from_fields(vec![
            Field::nullable("id", DataType::Int64),
            Field::nullable("name", DataType::String),
            Field::nullable("age", DataType::Int64),
        ]);

        schema.remove_field(1);
        assert_eq!(schema.field_count(), 2);
        assert!(schema.field("id").is_some());
        assert!(schema.field("name").is_none());
        assert!(schema.field("age").is_some());
    }

    #[test]
    fn test_schema_remove_field_out_of_bounds() {
        let mut schema = Schema::from_fields(vec![Field::nullable("id", DataType::Int64)]);

        schema.remove_field(10);
        assert_eq!(schema.field_count(), 1);
    }

    #[test]
    fn test_schema_rename_field() {
        let mut schema = Schema::from_fields(vec![
            Field::nullable("id", DataType::Int64),
            Field::nullable("name", DataType::String),
        ]);

        schema.rename_field(1, "full_name".to_string());
        assert!(schema.field("name").is_none());
        assert!(schema.field("full_name").is_some());
    }

    #[test]
    fn test_schema_rename_field_out_of_bounds() {
        let mut schema = Schema::from_fields(vec![Field::nullable("id", DataType::Int64)]);

        schema.rename_field(10, "new_name".to_string());
        assert!(schema.field("id").is_some());
        assert!(schema.field("new_name").is_none());
    }

    #[test]
    fn test_schema_fields() {
        let schema = Schema::from_fields(vec![
            Field::nullable("id", DataType::Int64),
            Field::nullable("name", DataType::String),
        ]);

        let fields = schema.fields();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].name, "id");
        assert_eq!(fields[1].name, "name");
    }

    #[test]
    fn test_schema_field() {
        let schema = Schema::from_fields(vec![
            Field::nullable("id", DataType::Int64),
            Field::nullable("name", DataType::String),
        ]);

        assert!(schema.field("id").is_some());
        assert_eq!(schema.field("id").unwrap().data_type, DataType::Int64);
        assert!(schema.field("name").is_some());
        assert!(schema.field("missing").is_none());
    }

    #[test]
    fn test_schema_field_index() {
        let schema = Schema::from_fields(vec![
            Field::nullable("id", DataType::Int64),
            Field::nullable("name", DataType::String),
            Field::nullable("age", DataType::Int64),
        ]);

        assert_eq!(schema.field_index("id"), Some(0));
        assert_eq!(schema.field_index("name"), Some(1));
        assert_eq!(schema.field_index("age"), Some(2));
        assert_eq!(schema.field_index("missing"), None);
    }

    #[test]
    fn test_schema_field_index_qualified_no_table() {
        let schema = Schema::from_fields(vec![
            Field::nullable("id", DataType::Int64),
            Field::nullable("name", DataType::String),
        ]);

        assert_eq!(schema.field_index_qualified("id", None), Some(0));
        assert_eq!(schema.field_index_qualified("name", None), Some(1));
        assert_eq!(schema.field_index_qualified("missing", None), None);
    }

    #[test]
    fn test_schema_field_index_qualified_with_qualified_name() {
        let schema = Schema::from_fields(vec![
            Field::nullable("users.id", DataType::Int64),
            Field::nullable("users.name", DataType::String),
        ]);

        assert_eq!(schema.field_index_qualified("id", Some("users")), Some(0));
        assert_eq!(schema.field_index_qualified("name", Some("users")), Some(1));
        assert_eq!(schema.field_index_qualified("missing", Some("users")), None);
    }

    #[test]
    fn test_schema_field_index_qualified_with_source_table() {
        let mut schema = Schema::new();
        schema.add_field(Field::nullable("id", DataType::Int64).with_source_table("users"));
        schema.add_field(Field::nullable("name", DataType::String).with_source_table("users"));
        schema.add_field(Field::nullable("id", DataType::Int64).with_source_table("orders"));

        assert_eq!(schema.field_index_qualified("id", Some("users")), Some(0));
        assert_eq!(schema.field_index_qualified("name", Some("users")), Some(1));
        assert_eq!(schema.field_index_qualified("id", Some("orders")), Some(2));
    }

    #[test]
    fn test_schema_field_index_qualified_case_insensitive() {
        let schema = Schema::from_fields(vec![Field::nullable("Users.ID", DataType::Int64)]);

        assert_eq!(schema.field_index_qualified("id", Some("users")), Some(0));
        assert_eq!(schema.field_index_qualified("ID", Some("USERS")), Some(0));
    }

    #[test]
    fn test_schema_field_index_qualified_with_source_table_suffix() {
        let mut schema = Schema::new();
        schema.add_field(
            Field::nullable("id", DataType::Int64).with_source_table("project.dataset.users"),
        );

        assert_eq!(schema.field_index_qualified("id", Some("users")), Some(0));
    }

    #[test]
    fn test_schema_field_count() {
        let schema = Schema::from_fields(vec![
            Field::nullable("a", DataType::Int64),
            Field::nullable("b", DataType::Int64),
            Field::nullable("c", DataType::Int64),
        ]);
        assert_eq!(schema.field_count(), 3);
    }

    #[test]
    fn test_schema_is_empty() {
        let empty_schema = Schema::new();
        assert!(empty_schema.is_empty());

        let non_empty_schema = Schema::from_fields(vec![Field::nullable("a", DataType::Int64)]);
        assert!(!non_empty_schema.is_empty());
    }

    #[test]
    fn test_schema_validate_success() {
        let schema = Schema::from_fields(vec![
            Field::nullable("id", DataType::Int64),
            Field::nullable("name", DataType::String),
        ]);
        assert!(schema.validate().is_ok());
    }

    #[test]
    fn test_schema_validate_duplicate_fields() {
        let schema = Schema::from_fields(vec![
            Field::nullable("id", DataType::Int64),
            Field::nullable("id", DataType::String),
        ]);
        let result = schema.validate();
        assert!(result.is_err());
    }

    #[test]
    fn test_schema_validate_empty() {
        let schema = Schema::new();
        assert!(schema.validate().is_ok());
    }

    #[test]
    fn test_schema_is_compatible_with_same_schema() {
        let schema1 = Schema::from_fields(vec![
            Field::nullable("id", DataType::Int64),
            Field::nullable("name", DataType::String),
        ]);
        let schema2 = Schema::from_fields(vec![
            Field::nullable("id", DataType::Int64),
            Field::nullable("name", DataType::String),
        ]);
        assert!(schema1.is_compatible_with(&schema2));
    }

    #[test]
    fn test_schema_is_compatible_with_different_lengths() {
        let schema1 = Schema::from_fields(vec![Field::nullable("id", DataType::Int64)]);
        let schema2 = Schema::from_fields(vec![
            Field::nullable("id", DataType::Int64),
            Field::nullable("name", DataType::String),
        ]);
        assert!(!schema1.is_compatible_with(&schema2));
    }

    #[test]
    fn test_schema_is_compatible_with_different_names() {
        let schema1 = Schema::from_fields(vec![Field::nullable("id", DataType::Int64)]);
        let schema2 = Schema::from_fields(vec![Field::nullable("user_id", DataType::Int64)]);
        assert!(!schema1.is_compatible_with(&schema2));
    }

    #[test]
    fn test_schema_is_compatible_with_different_types() {
        let schema1 = Schema::from_fields(vec![Field::nullable("id", DataType::Int64)]);
        let schema2 = Schema::from_fields(vec![Field::nullable("id", DataType::String)]);
        assert!(!schema1.is_compatible_with(&schema2));
    }

    #[test]
    fn test_schema_is_compatible_ignores_mode() {
        let schema1 = Schema::from_fields(vec![Field::nullable("id", DataType::Int64)]);
        let schema2 = Schema::from_fields(vec![Field::required("id", DataType::Int64)]);
        assert!(schema1.is_compatible_with(&schema2));
    }

    #[test]
    fn test_schema_is_compatible_empty_schemas() {
        let schema1 = Schema::new();
        let schema2 = Schema::new();
        assert!(schema1.is_compatible_with(&schema2));
    }

    #[test]
    fn test_field_clone() {
        let field = Field::nullable("test", DataType::Int64)
            .with_description("desc")
            .with_source_table("tbl");
        let cloned = field.clone();
        assert_eq!(field, cloned);
    }

    #[test]
    fn test_schema_clone() {
        let schema = Schema::from_fields(vec![
            Field::nullable("id", DataType::Int64),
            Field::nullable("name", DataType::String),
        ]);
        let cloned = schema.clone();
        assert_eq!(schema, cloned);
    }

    #[test]
    fn test_field_mode_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(FieldMode::Nullable);
        set.insert(FieldMode::Required);
        set.insert(FieldMode::Repeated);
        assert_eq!(set.len(), 3);

        set.insert(FieldMode::Nullable);
        assert_eq!(set.len(), 3);
    }

    #[test]
    fn test_field_mode_copy() {
        let mode = FieldMode::Required;
        let copied = mode;
        assert_eq!(mode, copied);
    }

    #[test]
    fn test_field_debug() {
        let field = Field::nullable("test", DataType::Int64);
        let debug_str = format!("{:?}", field);
        assert!(debug_str.contains("test"));
        assert!(debug_str.contains("Int64"));
    }

    #[test]
    fn test_schema_debug() {
        let schema = Schema::from_fields(vec![Field::nullable("id", DataType::Int64)]);
        let debug_str = format!("{:?}", schema);
        assert!(debug_str.contains("id"));
    }

    #[test]
    fn test_field_mode_debug() {
        assert!(format!("{:?}", FieldMode::Nullable).contains("Nullable"));
        assert!(format!("{:?}", FieldMode::Required).contains("Required"));
        assert!(format!("{:?}", FieldMode::Repeated).contains("Repeated"));
    }
}
