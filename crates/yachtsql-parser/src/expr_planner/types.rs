#![coverage(off)]

use sqlparser::ast;
use yachtsql_common::error::Result;
use yachtsql_common::types::DataType;

pub fn plan_data_type(sql_type: &ast::DataType) -> Result<DataType> {
    match sql_type {
        ast::DataType::Boolean | ast::DataType::Bool => Ok(DataType::Bool),
        ast::DataType::Int64 | ast::DataType::BigInt(_) => Ok(DataType::Int64),
        ast::DataType::Float64 | ast::DataType::Double(_) => Ok(DataType::Float64),
        ast::DataType::Numeric(info) | ast::DataType::Decimal(info) => {
            let ps = match info {
                ast::ExactNumberInfo::PrecisionAndScale(p, s) => Some((*p as u8, *s as u8)),
                ast::ExactNumberInfo::Precision(p) => Some((*p as u8, 0)),
                ast::ExactNumberInfo::None => None,
            };
            Ok(DataType::Numeric(ps))
        }
        ast::DataType::BigNumeric(_) => Ok(DataType::BigNumeric),
        ast::DataType::String(_) | ast::DataType::Varchar(_) | ast::DataType::Text => {
            Ok(DataType::String)
        }
        ast::DataType::Bytes(_) | ast::DataType::Bytea => Ok(DataType::Bytes),
        ast::DataType::Date => Ok(DataType::Date),
        ast::DataType::Time(..) => Ok(DataType::Time),
        ast::DataType::Datetime(_) => Ok(DataType::DateTime),
        ast::DataType::Timestamp(..) => Ok(DataType::Timestamp),
        ast::DataType::JSON | ast::DataType::JSONB => Ok(DataType::Json),
        ast::DataType::Interval { .. } => Ok(DataType::Interval),
        ast::DataType::Array(elem_def) => {
            let inner = match elem_def {
                ast::ArrayElemTypeDef::AngleBracket(dt)
                | ast::ArrayElemTypeDef::SquareBracket(dt, _)
                | ast::ArrayElemTypeDef::Parenthesis(dt) => plan_data_type(dt)?,
                ast::ArrayElemTypeDef::None => DataType::Unknown,
            };
            Ok(DataType::Array(Box::new(inner)))
        }
        ast::DataType::Struct(fields, _) => {
            let struct_fields = fields
                .iter()
                .enumerate()
                .map(|(i, f)| {
                    let name = f
                        .field_name
                        .as_ref()
                        .map(|id| id.value.clone())
                        .unwrap_or_else(|| format!("_field{}", i));
                    let field_type = plan_data_type(&f.field_type).unwrap_or(DataType::Unknown);
                    yachtsql_common::types::StructField {
                        name,
                        data_type: field_type,
                    }
                })
                .collect();
            Ok(DataType::Struct(struct_fields))
        }
        ast::DataType::Range(inner) => {
            let inner_type = plan_data_type(inner)?;
            Ok(DataType::Range(Box::new(inner_type)))
        }
        _ => Ok(DataType::Unknown),
    }
}
