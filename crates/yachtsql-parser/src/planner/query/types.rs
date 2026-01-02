#![coverage(off)]

use sqlparser::ast;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::{DataType, StructField};
use yachtsql_ir::{ConstraintType, DateTimeField, Expr, PlanField, PlanSchema, TableConstraint};
use yachtsql_storage::Schema;

use super::super::object_name_to_raw_string;
use super::Planner;
use crate::CatalogProvider;
use crate::expr_planner::ExprPlanner;

impl<'a, C: CatalogProvider> Planner<'a, C> {
    pub(super) fn infer_expr_type(&self, expr: &Expr, schema: &PlanSchema) -> DataType {
        Self::compute_expr_type(expr, schema)
    }

    pub(super) fn compute_expr_type(expr: &Expr, schema: &PlanSchema) -> DataType {
        match expr {
            Expr::Literal(lit) => lit.data_type(),
            Expr::Column { name, index, .. } => if let Some(idx) = index {
                schema.fields.get(*idx).map(|f| f.data_type.clone())
            } else {
                schema.field(name).map(|f| f.data_type.clone())
            }
            .unwrap_or(DataType::Unknown),
            Expr::BinaryOp { left, op, right } => {
                use yachtsql_ir::BinaryOp;
                match op {
                    BinaryOp::Eq
                    | BinaryOp::NotEq
                    | BinaryOp::Lt
                    | BinaryOp::LtEq
                    | BinaryOp::Gt
                    | BinaryOp::GtEq
                    | BinaryOp::And
                    | BinaryOp::Or => DataType::Bool,
                    BinaryOp::Concat => DataType::String,
                    _ => {
                        let left_type = Self::compute_expr_type(left, schema);
                        let right_type = Self::compute_expr_type(right, schema);
                        if left_type == DataType::Float64 || right_type == DataType::Float64 {
                            DataType::Float64
                        } else if left_type == DataType::Int64 || right_type == DataType::Int64 {
                            DataType::Int64
                        } else {
                            left_type
                        }
                    }
                }
            }
            Expr::UnaryOp { op, expr } => {
                use yachtsql_ir::UnaryOp;
                match op {
                    UnaryOp::Not => DataType::Bool,
                    _ => Self::compute_expr_type(expr, schema),
                }
            }
            Expr::Aggregate { func, args, .. } => {
                use yachtsql_ir::AggregateFunction;
                match func {
                    AggregateFunction::Count
                    | AggregateFunction::CountIf
                    | AggregateFunction::Grouping
                    | AggregateFunction::GroupingId => DataType::Int64,
                    AggregateFunction::Min
                    | AggregateFunction::MinIf
                    | AggregateFunction::Max
                    | AggregateFunction::MaxIf => {
                        if let Some(first_arg) = args.first() {
                            Self::compute_expr_type(first_arg, schema)
                        } else {
                            DataType::Unknown
                        }
                    }
                    AggregateFunction::Avg
                    | AggregateFunction::AvgIf
                    | AggregateFunction::Sum
                    | AggregateFunction::SumIf
                    | AggregateFunction::Stddev
                    | AggregateFunction::StddevPop
                    | AggregateFunction::StddevSamp
                    | AggregateFunction::Variance
                    | AggregateFunction::VarPop
                    | AggregateFunction::VarSamp
                    | AggregateFunction::Corr
                    | AggregateFunction::CovarPop
                    | AggregateFunction::CovarSamp => DataType::Float64,
                    AggregateFunction::ArrayAgg | AggregateFunction::ApproxQuantiles => {
                        DataType::Array(Box::new(DataType::Unknown))
                    }
                    AggregateFunction::StringAgg | AggregateFunction::XmlAgg => DataType::String,
                    AggregateFunction::AnyValue => DataType::Unknown,
                    AggregateFunction::LogicalAnd | AggregateFunction::LogicalOr => DataType::Bool,
                    AggregateFunction::BitAnd
                    | AggregateFunction::BitOr
                    | AggregateFunction::BitXor => DataType::Int64,
                    AggregateFunction::ApproxCountDistinct => DataType::Int64,
                    AggregateFunction::ApproxTopCount | AggregateFunction::ApproxTopSum => {
                        DataType::Array(Box::new(DataType::Struct(vec![])))
                    }
                }
            }
            Expr::Cast { data_type, .. } => data_type.clone(),
            Expr::IsNull { .. }
            | Expr::InList { .. }
            | Expr::Between { .. }
            | Expr::Like { .. }
            | Expr::IsDistinctFrom { .. }
            | Expr::Exists { .. }
            | Expr::InSubquery { .. }
            | Expr::InUnnest { .. } => DataType::Bool,
            Expr::Alias { expr, .. } => Self::compute_expr_type(expr, schema),
            Expr::Window { func, args, .. } => {
                use yachtsql_ir::WindowFunction;
                match func {
                    WindowFunction::RowNumber
                    | WindowFunction::Rank
                    | WindowFunction::DenseRank
                    | WindowFunction::Ntile => DataType::Int64,
                    WindowFunction::PercentRank | WindowFunction::CumeDist => DataType::Float64,
                    WindowFunction::Lead
                    | WindowFunction::Lag
                    | WindowFunction::FirstValue
                    | WindowFunction::LastValue
                    | WindowFunction::NthValue => {
                        if let Some(first_arg) = args.first() {
                            Self::compute_expr_type(first_arg, schema)
                        } else {
                            DataType::Unknown
                        }
                    }
                }
            }
            Expr::AggregateWindow { func, args, .. } => {
                use yachtsql_ir::AggregateFunction;
                match func {
                    AggregateFunction::Count | AggregateFunction::CountIf => DataType::Int64,
                    AggregateFunction::Min
                    | AggregateFunction::MinIf
                    | AggregateFunction::Max
                    | AggregateFunction::MaxIf => {
                        if let Some(first_arg) = args.first() {
                            Self::compute_expr_type(first_arg, schema)
                        } else {
                            DataType::Unknown
                        }
                    }
                    AggregateFunction::Avg
                    | AggregateFunction::AvgIf
                    | AggregateFunction::Sum
                    | AggregateFunction::SumIf => DataType::Float64,
                    _ => DataType::Unknown,
                }
            }
            Expr::ScalarFunction { name, args } => {
                Self::infer_scalar_function_type(name, args, schema)
            }
            Expr::Case {
                when_clauses,
                else_result,
                ..
            } => {
                if let Some(first_clause) = when_clauses.first() {
                    Self::compute_expr_type(&first_clause.result, schema)
                } else if let Some(else_expr) = else_result {
                    Self::compute_expr_type(else_expr, schema)
                } else {
                    DataType::Unknown
                }
            }
            Expr::Extract { field, .. } => match field {
                DateTimeField::Date => DataType::Date,
                DateTimeField::Time => DataType::Time,
                DateTimeField::Year
                | DateTimeField::IsoYear
                | DateTimeField::Quarter
                | DateTimeField::Month
                | DateTimeField::Week(_)
                | DateTimeField::IsoWeek
                | DateTimeField::Day
                | DateTimeField::DayOfWeek
                | DateTimeField::DayOfYear
                | DateTimeField::Hour
                | DateTimeField::Minute
                | DateTimeField::Second
                | DateTimeField::Millisecond
                | DateTimeField::Microsecond
                | DateTimeField::Nanosecond
                | DateTimeField::Datetime
                | DateTimeField::Timezone
                | DateTimeField::TimezoneHour
                | DateTimeField::TimezoneMinute => DataType::Int64,
            },
            Expr::TypedString { data_type, .. } => data_type.clone(),
            Expr::Array {
                elements,
                element_type,
            } => {
                let elem_type = element_type.clone().unwrap_or_else(|| {
                    elements
                        .first()
                        .map(|e| Self::compute_expr_type(e, schema))
                        .unwrap_or(DataType::Unknown)
                });
                DataType::Array(Box::new(elem_type))
            }
            Expr::ArrayAccess { array, .. } => {
                let array_type = Self::compute_expr_type(array, schema);
                match array_type {
                    DataType::Array(inner) => *inner,
                    _ => DataType::Unknown,
                }
            }
            Expr::Struct { fields } => {
                let struct_fields = fields
                    .iter()
                    .enumerate()
                    .map(|(i, (name, expr))| StructField {
                        name: name.clone().unwrap_or_else(|| format!("_field{}", i)),
                        data_type: Self::compute_expr_type(expr, schema),
                    })
                    .collect();
                DataType::Struct(struct_fields)
            }
            Expr::StructAccess { expr, field } => {
                Self::resolve_struct_field_type(expr, field, schema)
            }
            Expr::ArraySubquery(plan) => {
                let subquery_schema = plan.schema();
                if subquery_schema.fields.len() == 1 {
                    DataType::Array(Box::new(subquery_schema.fields[0].data_type.clone()))
                } else {
                    let struct_fields: Vec<StructField> = subquery_schema
                        .fields
                        .iter()
                        .map(|f| StructField {
                            name: f.name.clone(),
                            data_type: f.data_type.clone(),
                        })
                        .collect();
                    DataType::Array(Box::new(DataType::Struct(struct_fields)))
                }
            }
            _ => DataType::Unknown,
        }
    }

    fn infer_scalar_function_type(
        name: &yachtsql_ir::ScalarFunction,
        args: &[Expr],
        schema: &PlanSchema,
    ) -> DataType {
        use yachtsql_ir::ScalarFunction;
        match name {
            ScalarFunction::CurrentDate
            | ScalarFunction::Date
            | ScalarFunction::DateAdd
            | ScalarFunction::DateSub
            | ScalarFunction::DateTrunc
            | ScalarFunction::DateFromUnixDate
            | ScalarFunction::LastDay
            | ScalarFunction::ParseDate => DataType::Date,

            ScalarFunction::CurrentTime
            | ScalarFunction::Time
            | ScalarFunction::TimeTrunc
            | ScalarFunction::ParseTime => DataType::Time,

            ScalarFunction::CurrentDatetime
            | ScalarFunction::Datetime
            | ScalarFunction::DatetimeTrunc
            | ScalarFunction::ParseDatetime => DataType::DateTime,

            ScalarFunction::CurrentTimestamp
            | ScalarFunction::Timestamp
            | ScalarFunction::TimestampTrunc
            | ScalarFunction::TimestampMicros
            | ScalarFunction::TimestampMillis
            | ScalarFunction::TimestampSeconds
            | ScalarFunction::ParseTimestamp => DataType::Timestamp,

            ScalarFunction::DateDiff
            | ScalarFunction::UnixDate
            | ScalarFunction::UnixMicros
            | ScalarFunction::UnixMillis
            | ScalarFunction::UnixSeconds
            | ScalarFunction::Length
            | ScalarFunction::ByteLength
            | ScalarFunction::CharLength
            | ScalarFunction::Strpos
            | ScalarFunction::Instr
            | ScalarFunction::Ascii
            | ScalarFunction::ArrayLength
            | ScalarFunction::Sign
            | ScalarFunction::FarmFingerprint
            | ScalarFunction::Int64FromJson
            | ScalarFunction::BitCount => DataType::Int64,

            ScalarFunction::Abs
            | ScalarFunction::Sqrt
            | ScalarFunction::Cbrt
            | ScalarFunction::Power
            | ScalarFunction::Pow
            | ScalarFunction::Exp
            | ScalarFunction::Ln
            | ScalarFunction::Log
            | ScalarFunction::Log10
            | ScalarFunction::Sin
            | ScalarFunction::Cos
            | ScalarFunction::Tan
            | ScalarFunction::Asin
            | ScalarFunction::Acos
            | ScalarFunction::Atan
            | ScalarFunction::Atan2
            | ScalarFunction::Pi
            | ScalarFunction::Rand
            | ScalarFunction::RandCanonical
            | ScalarFunction::SafeDivide
            | ScalarFunction::IeeeDivide
            | ScalarFunction::Float64FromJson => DataType::Float64,

            ScalarFunction::Floor
            | ScalarFunction::Ceil
            | ScalarFunction::Round
            | ScalarFunction::Trunc
            | ScalarFunction::Mod
            | ScalarFunction::Div
            | ScalarFunction::Greatest
            | ScalarFunction::Least
            | ScalarFunction::SafeMultiply
            | ScalarFunction::SafeAdd
            | ScalarFunction::SafeSubtract
            | ScalarFunction::SafeNegate => {
                if let Some(first_arg) = args.first() {
                    Self::compute_expr_type(first_arg, schema)
                } else {
                    DataType::Float64
                }
            }

            ScalarFunction::Upper
            | ScalarFunction::Lower
            | ScalarFunction::Trim
            | ScalarFunction::LTrim
            | ScalarFunction::RTrim
            | ScalarFunction::Substr
            | ScalarFunction::Concat
            | ScalarFunction::Replace
            | ScalarFunction::Reverse
            | ScalarFunction::Left
            | ScalarFunction::Right
            | ScalarFunction::Repeat
            | ScalarFunction::Lpad
            | ScalarFunction::Rpad
            | ScalarFunction::Initcap
            | ScalarFunction::Format
            | ScalarFunction::FormatDate
            | ScalarFunction::FormatDatetime
            | ScalarFunction::FormatTimestamp
            | ScalarFunction::FormatTime
            | ScalarFunction::String
            | ScalarFunction::ToJsonString
            | ScalarFunction::Chr
            | ScalarFunction::ToBase64
            | ScalarFunction::ToHex
            | ScalarFunction::Md5
            | ScalarFunction::Sha1
            | ScalarFunction::Sha256
            | ScalarFunction::Sha512
            | ScalarFunction::GenerateUuid
            | ScalarFunction::ArrayToString
            | ScalarFunction::RegexpExtract
            | ScalarFunction::RegexpReplace
            | ScalarFunction::JsonValue
            | ScalarFunction::JsonExtractScalar
            | ScalarFunction::TypeOf => DataType::String,

            ScalarFunction::StartsWith
            | ScalarFunction::EndsWith
            | ScalarFunction::Contains
            | ScalarFunction::RegexpContains
            | ScalarFunction::IsNan
            | ScalarFunction::IsInf
            | ScalarFunction::BoolFromJson => DataType::Bool,

            ScalarFunction::Split
            | ScalarFunction::ArrayConcat
            | ScalarFunction::ArrayReverse
            | ScalarFunction::GenerateArray
            | ScalarFunction::GenerateDateArray
            | ScalarFunction::GenerateTimestampArray => {
                DataType::Array(Box::new(DataType::Unknown))
            }

            ScalarFunction::FromBase64 | ScalarFunction::FromHex => DataType::Bytes,

            ScalarFunction::ToJson
            | ScalarFunction::ParseJson
            | ScalarFunction::JsonQuery
            | ScalarFunction::JsonExtract => DataType::Json,

            ScalarFunction::MakeInterval
            | ScalarFunction::JustifyDays
            | ScalarFunction::JustifyHours
            | ScalarFunction::JustifyInterval => DataType::Interval,

            ScalarFunction::Range => {
                if let Some(first_arg) = args.first() {
                    let element_type = Self::compute_expr_type(first_arg, schema);
                    DataType::Range(Box::new(element_type))
                } else {
                    DataType::Range(Box::new(DataType::Unknown))
                }
            }

            ScalarFunction::Coalesce
            | ScalarFunction::IfNull
            | ScalarFunction::Ifnull
            | ScalarFunction::NullIf
            | ScalarFunction::Nvl
            | ScalarFunction::Nvl2
            | ScalarFunction::If => {
                if let Some(first_arg) = args.first() {
                    Self::compute_expr_type(first_arg, schema)
                } else {
                    DataType::Unknown
                }
            }

            ScalarFunction::Cast | ScalarFunction::SafeCast | ScalarFunction::SafeConvert => {
                DataType::Unknown
            }

            ScalarFunction::Custom(name) => Self::infer_custom_function_type(name),

            _ => DataType::Unknown,
        }
    }

    fn infer_custom_function_type(name: &str) -> DataType {
        let upper = name.to_uppercase();
        match upper.as_str() {
            "ST_CONTAINS" | "ST_WITHIN" | "ST_INTERSECTS" | "ST_COVERS" | "ST_COVEREDBY"
            | "ST_DISJOINT" | "ST_TOUCHES" | "ST_EQUALS" | "ST_DWITHIN" | "ST_ISCLOSED"
            | "ST_ISEMPTY" | "ST_ISCOLLECTION" | "ST_ISRING" => DataType::Bool,

            "ST_GEOGFROMTEXT"
            | "ST_GEOGPOINT"
            | "ST_GEOGFROMGEOJSON"
            | "ST_CENTROID"
            | "ST_BUFFER"
            | "ST_CONVEXHULL"
            | "ST_SIMPLIFY"
            | "ST_SNAPTOGRID"
            | "ST_BOUNDARY"
            | "ST_STARTPOINT"
            | "ST_ENDPOINT"
            | "ST_POINTN"
            | "ST_CLOSESTPOINT"
            | "ST_UNION"
            | "ST_INTERSECTION"
            | "ST_DIFFERENCE"
            | "ST_MAKELINE"
            | "ST_MAKEPOLYGON"
            | "ST_BUFFERWITHTOLERANCE"
            | "ST_GEOGPOINTFROMGEOHASH" => DataType::Geography,

            "ST_ASTEXT" | "ST_GEOMETRYTYPE" | "ST_GEOHASH" => DataType::String,

            "ST_ASGEOJSON" => DataType::Json,

            "ST_ASBINARY" => DataType::Bytes,

            "ST_X" | "ST_Y" | "ST_AREA" | "ST_LENGTH" | "ST_PERIMETER" | "ST_DISTANCE"
            | "ST_MAXDISTANCE" => DataType::Float64,

            "ST_DIMENSION" | "ST_NUMPOINTS" => DataType::Int64,

            "NET.IP_IN_NET" | "NET.IP_IS_PRIVATE" => DataType::Bool,

            "NET.IP_FROM_STRING" | "NET.IPV4_FROM_INT64" | "NET.IP_TRUNC" | "NET.IP_NET_MASK" => {
                DataType::Bytes
            }

            "NET.IP_TO_STRING" | "NET.HOST" | "NET.PUBLIC_SUFFIX" | "NET.REG_DOMAIN" => {
                DataType::String
            }

            "NET.IPV4_TO_INT64" => DataType::Int64,

            "BOOL" | "RANGE_CONTAINS" | "RANGE_OVERLAPS" => DataType::Bool,

            "INT64" | "SAFE_CAST_INT64" | "BIT_COUNT" | "INT64_FROM_JSON" => DataType::Int64,

            "FLOAT64" | "SAFE_CAST_FLOAT64" | "FLOAT64_FROM_JSON" => DataType::Float64,

            "STRING" | "SAFE_CAST_STRING" | "STRING_FROM_JSON" | "JSON_VALUE" => DataType::String,

            "STRUCT" => DataType::Struct(vec![]),

            "JSON_TYPE" | "JSON" | "PARSE_JSON" | "JSON_QUERY" | "JSON_EXTRACT" | "JSON_SET"
            | "JSON_REMOVE" | "JSON_STRIP_NULLS" => DataType::Json,

            "JSON_ARRAY" | "JSON_QUERY_ARRAY" | "JSON_VALUE_ARRAY" | "JSON_EXTRACT_ARRAY" => {
                DataType::Array(Box::new(DataType::Unknown))
            }

            "TIMESTAMP_DIFF" => DataType::Int64,

            _ => DataType::Unknown,
        }
    }

    pub(super) fn resolve_struct_field_type(
        expr: &Expr,
        field: &str,
        schema: &PlanSchema,
    ) -> DataType {
        match expr {
            Expr::Struct { fields } => {
                for (name, field_expr) in fields {
                    if name.as_deref() == Some(field) {
                        return Self::compute_expr_type(field_expr, schema);
                    }
                }
                DataType::Unknown
            }
            Expr::StructAccess {
                expr: inner_expr,
                field: inner_field,
            } => {
                let inner_result = Self::find_struct_field_expr(inner_expr, inner_field);
                if let Some(inner_struct_expr) = inner_result {
                    Self::resolve_struct_field_type(&inner_struct_expr, field, schema)
                } else {
                    DataType::Unknown
                }
            }
            Expr::Column { name, index, .. } => {
                let col_type = index
                    .and_then(|i| schema.fields.get(i))
                    .map(|f| f.data_type.clone())
                    .unwrap_or_else(|| {
                        schema
                            .fields
                            .iter()
                            .find(|f| f.name.eq_ignore_ascii_case(name))
                            .map(|f| f.data_type.clone())
                            .unwrap_or(DataType::Unknown)
                    });
                if let DataType::Struct(struct_fields) = col_type {
                    let field_lower = field.to_lowercase();
                    for sf in struct_fields {
                        if sf.name.to_lowercase() == field_lower {
                            return sf.data_type.clone();
                        }
                    }
                }
                DataType::Unknown
            }
            _ => DataType::Unknown,
        }
    }

    fn find_struct_field_expr(expr: &Expr, field: &str) -> Option<Expr> {
        match expr {
            Expr::Struct { fields } => {
                for (name, field_expr) in fields {
                    if name.as_deref() == Some(field) {
                        return Some(field_expr.clone());
                    }
                }
                None
            }
            Expr::StructAccess {
                expr: inner_expr,
                field: inner_field,
            } => {
                let inner_result = Self::find_struct_field_expr(inner_expr, inner_field);
                inner_result
                    .and_then(|inner_struct| Self::find_struct_field_expr(&inner_struct, field))
            }
            _ => None,
        }
    }

    pub(in crate::planner) fn storage_schema_to_plan_schema(
        &self,
        schema: &Schema,
        table: Option<&str>,
    ) -> PlanSchema {
        let fields = schema
            .fields()
            .iter()
            .map(|f| PlanField {
                name: f.name.clone(),
                data_type: f.data_type.clone(),
                nullable: f.is_nullable(),
                table: table.map(String::from),
            })
            .collect();
        PlanSchema::from_fields(fields)
    }

    pub(super) fn rename_schema(&self, schema: &PlanSchema, new_table: &str) -> PlanSchema {
        let fields = schema
            .fields
            .iter()
            .map(|f| PlanField {
                name: f.name.clone(),
                data_type: f.data_type.clone(),
                nullable: f.nullable,
                table: Some(new_table.to_string()),
            })
            .collect();
        PlanSchema::from_fields(fields)
    }

    pub(in crate::planner) fn sql_type_to_data_type(&self, sql_type: &ast::DataType) -> DataType {
        Self::convert_sql_type(sql_type)
    }

    pub(in crate::planner) fn convert_sql_type(sql_type: &ast::DataType) -> DataType {
        match sql_type {
            ast::DataType::Boolean | ast::DataType::Bool => DataType::Bool,
            ast::DataType::Int64 | ast::DataType::BigInt(_) => DataType::Int64,
            ast::DataType::Float64 | ast::DataType::Double(_) => DataType::Float64,
            ast::DataType::Numeric(info) | ast::DataType::Decimal(info) => {
                let ps = match info {
                    ast::ExactNumberInfo::PrecisionAndScale(p, s) => Some((*p as u8, *s as u8)),
                    ast::ExactNumberInfo::Precision(p) => Some((*p as u8, 0)),
                    ast::ExactNumberInfo::None => None,
                };
                DataType::Numeric(ps)
            }
            ast::DataType::BigNumeric(_) => DataType::BigNumeric,
            ast::DataType::String(_) | ast::DataType::Varchar(_) | ast::DataType::Text => {
                DataType::String
            }
            ast::DataType::Bytes(_) | ast::DataType::Bytea => DataType::Bytes,
            ast::DataType::Date => DataType::Date,
            ast::DataType::Time(..) => DataType::Time,
            ast::DataType::Datetime(_) => DataType::DateTime,
            ast::DataType::Timestamp(..) => DataType::Timestamp,
            ast::DataType::JSON | ast::DataType::JSONB => DataType::Json,
            ast::DataType::Array(inner) => {
                let element_type = match inner {
                    ast::ArrayElemTypeDef::AngleBracket(dt) => Self::convert_sql_type(dt),
                    ast::ArrayElemTypeDef::SquareBracket(dt, _) => Self::convert_sql_type(dt),
                    ast::ArrayElemTypeDef::Parenthesis(dt) => Self::convert_sql_type(dt),
                    ast::ArrayElemTypeDef::None => DataType::Unknown,
                };
                DataType::Array(Box::new(element_type))
            }
            ast::DataType::Interval { .. } => DataType::Interval,
            ast::DataType::Range(inner) => DataType::Range(Box::new(Self::convert_sql_type(inner))),
            ast::DataType::Custom(name, modifiers) => {
                let type_name = object_name_to_raw_string(name).to_uppercase();
                match type_name.as_str() {
                    "GEOGRAPHY" => DataType::Geography,
                    "RANGE" => {
                        if let Some(inner_type_str) = modifiers.first() {
                            let inner_type =
                                Self::parse_range_inner_type(&inner_type_str.to_string());
                            DataType::Range(Box::new(inner_type))
                        } else {
                            DataType::Range(Box::new(DataType::Unknown))
                        }
                    }
                    "RANGE_DATE" => DataType::Range(Box::new(DataType::Date)),
                    "RANGE_DATETIME" => DataType::Range(Box::new(DataType::DateTime)),
                    "RANGE_TIMESTAMP" => DataType::Range(Box::new(DataType::Timestamp)),
                    _ => DataType::Unknown,
                }
            }
            ast::DataType::Struct(fields, _) => {
                let struct_fields: Vec<StructField> = fields
                    .iter()
                    .map(|f| StructField {
                        name: f
                            .field_name
                            .as_ref()
                            .map(|n| n.value.clone())
                            .unwrap_or_default(),
                        data_type: Self::convert_sql_type(&f.field_type),
                    })
                    .collect();
                DataType::Struct(struct_fields)
            }
            _ => DataType::Unknown,
        }
    }

    fn parse_range_inner_type(type_str: &str) -> DataType {
        match type_str.to_uppercase().as_str() {
            "DATE" => DataType::Date,
            "DATETIME" => DataType::DateTime,
            "TIMESTAMP" => DataType::Timestamp,
            _ => DataType::Unknown,
        }
    }

    pub(in crate::planner) fn plan_table_constraint(
        &self,
        constraint: &ast::TableConstraint,
    ) -> Result<TableConstraint> {
        let (name, constraint_type) = match constraint {
            ast::TableConstraint::Unique { name, columns, .. } => {
                let col_names: Vec<String> =
                    columns.iter().map(|c| c.column.expr.to_string()).collect();
                (
                    name.clone().map(|n| n.value),
                    ConstraintType::Unique { columns: col_names },
                )
            }
            ast::TableConstraint::PrimaryKey { name, columns, .. } => {
                let col_names: Vec<String> =
                    columns.iter().map(|c| c.column.expr.to_string()).collect();
                (
                    name.clone().map(|n| n.value),
                    ConstraintType::PrimaryKey { columns: col_names },
                )
            }
            ast::TableConstraint::ForeignKey {
                name,
                columns,
                foreign_table,
                referred_columns,
                ..
            } => {
                let col_names: Vec<String> = columns.iter().map(|c| c.value.clone()).collect();
                let ref_cols: Vec<String> =
                    referred_columns.iter().map(|c| c.value.clone()).collect();
                let ctype = ConstraintType::ForeignKey {
                    columns: col_names,
                    references_table: foreign_table.to_string(),
                    references_columns: ref_cols,
                };
                (name.clone().map(|n| n.value), ctype)
            }
            ast::TableConstraint::Check { name, expr, .. } => {
                let empty_schema = PlanSchema::new();
                let check_expr = ExprPlanner::plan_expr(expr, &empty_schema)?;
                (
                    name.clone().map(|n| n.value),
                    ConstraintType::Check { expr: check_expr },
                )
            }
            _ => {
                return Err(Error::unsupported(format!(
                    "Unsupported table constraint: {:?}",
                    constraint
                )));
            }
        };

        Ok(TableConstraint {
            name,
            constraint_type,
        })
    }
}
