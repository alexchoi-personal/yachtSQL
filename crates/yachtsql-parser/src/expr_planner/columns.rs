#![coverage(off)]

use sqlparser::ast;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::DataType;
use yachtsql_ir::{Expr, JsonPathElement, Literal, PlanSchema};

use super::datetime::is_date_part_keyword;

pub fn resolve_column(name: &str, table: Option<&str>, schema: &PlanSchema) -> Result<Expr> {
    let index = schema.field_index_qualified(name, table);

    if index.is_none() && table.is_none() && is_date_part_keyword(name) {
        return Ok(Expr::Literal(Literal::String(name.to_uppercase())));
    }

    if index.is_none() && table.is_none() {
        let struct_fields: Vec<_> = schema
            .fields
            .iter()
            .enumerate()
            .filter(|(_, f)| {
                f.table
                    .as_ref()
                    .is_some_and(|t| t.eq_ignore_ascii_case(name))
            })
            .collect();
        if !struct_fields.is_empty() {
            let field_exprs: Vec<(Option<String>, Expr)> = struct_fields
                .iter()
                .map(|(i, f)| {
                    (
                        Some(f.name.clone()),
                        Expr::Column {
                            table: Some(name.to_string()),
                            name: f.name.clone(),
                            index: Some(*i),
                        },
                    )
                })
                .collect();
            return Ok(Expr::Struct {
                fields: field_exprs,
            });
        }
    }

    Ok(Expr::Column {
        table: table.map(String::from),
        name: name.to_string(),
        index,
    })
}

pub fn resolve_compound_identifier(parts: &[ast::Ident], schema: &PlanSchema) -> Result<Expr> {
    if parts.is_empty() {
        return Err(Error::InvalidQuery("Empty compound identifier".into()));
    }

    if parts.len() == 1 {
        let name = &parts[0].value;
        return resolve_column(name, None, schema);
    }

    let first_part = &parts[0].value;

    if first_part.starts_with('@') {
        let mut expr = Expr::Variable {
            name: first_part.clone(),
        };
        for part in &parts[1..] {
            expr = Expr::StructAccess {
                expr: Box::new(expr),
                field: part.value.clone(),
            };
        }
        return Ok(expr);
    }

    if let Some((idx, field)) = schema.field_by_name(first_part) {
        match &field.data_type {
            DataType::Struct(_) => {
                let mut expr = Expr::Column {
                    table: None,
                    name: first_part.clone(),
                    index: Some(idx),
                };
                for part in &parts[1..] {
                    expr = Expr::StructAccess {
                        expr: Box::new(expr),
                        field: part.value.clone(),
                    };
                }
                return Ok(expr);
            }
            DataType::Json => {
                let base = Expr::Column {
                    table: None,
                    name: first_part.clone(),
                    index: Some(idx),
                };
                let path: Vec<JsonPathElement> = parts[1..]
                    .iter()
                    .map(|p| JsonPathElement::Key(p.value.clone()))
                    .collect();
                return Ok(Expr::JsonAccess {
                    expr: Box::new(base),
                    path,
                });
            }
            _ => {}
        }
    }

    if parts.len() >= 3 {
        let table_alias = &parts[0].value;
        let col_name = &parts[1].value;
        for (idx, field) in schema.fields.iter().enumerate() {
            if field.name.eq_ignore_ascii_case(col_name)
                && field
                    .table
                    .as_ref()
                    .is_some_and(|t| t.eq_ignore_ascii_case(table_alias))
            {
                if let DataType::Struct(_) = &field.data_type {
                    let mut expr = Expr::Column {
                        table: Some(table_alias.clone()),
                        name: col_name.clone(),
                        index: Some(idx),
                    };
                    for part in &parts[2..] {
                        expr = Expr::StructAccess {
                            expr: Box::new(expr),
                            field: part.value.clone(),
                        };
                    }
                    return Ok(expr);
                }
                if let DataType::Json = &field.data_type {
                    let base = Expr::Column {
                        table: Some(table_alias.clone()),
                        name: col_name.clone(),
                        index: Some(idx),
                    };
                    let path: Vec<JsonPathElement> = parts[2..]
                        .iter()
                        .map(|p| JsonPathElement::Key(p.value.clone()))
                        .collect();
                    return Ok(Expr::JsonAccess {
                        expr: Box::new(base),
                        path,
                    });
                }
            }
        }
    }

    let (table, name) = (
        Some(
            parts[..parts.len() - 1]
                .iter()
                .map(|p| p.value.clone())
                .collect::<Vec<_>>()
                .join("."),
        ),
        parts.last().map(|p| p.value.clone()).unwrap_or_default(),
    );

    resolve_column(&name, table.as_deref(), schema)
}
