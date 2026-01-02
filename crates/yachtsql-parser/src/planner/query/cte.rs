#![coverage(off)]

use sqlparser::ast::{self, SetExpr, TableFactor};
use yachtsql_common::error::Result;
use yachtsql_ir::{CteDefinition, PlanField, PlanSchema};

use super::super::object_name_to_raw_string;
use super::Planner;
use crate::CatalogProvider;

impl<'a, C: CatalogProvider> Planner<'a, C> {
    pub(super) fn plan_ctes(&self, with_clause: &ast::With) -> Result<Vec<CteDefinition>> {
        let mut ctes = Vec::new();
        for cte in &with_clause.cte_tables {
            let name = cte.alias.name.value.to_uppercase();
            let columns: Vec<String> = cte
                .alias
                .columns
                .iter()
                .map(|c| c.name.value.clone())
                .collect();
            let columns = if columns.is_empty() {
                None
            } else {
                Some(columns)
            };

            let cte_query = if with_clause.recursive {
                let anchor_schema =
                    self.get_recursive_cte_anchor_schema(&cte.query, &name, &columns)?;
                self.cte_schemas
                    .borrow_mut()
                    .insert(name.clone(), anchor_schema);
                self.plan_query(&cte.query)?
            } else {
                self.plan_query(&cte.query)?
            };

            let cte_schema = if let Some(ref col_names) = columns {
                let fields = cte_query
                    .schema()
                    .fields
                    .iter()
                    .zip(col_names.iter())
                    .map(|(f, col_name)| PlanField {
                        name: col_name.clone(),
                        data_type: f.data_type.clone(),
                        nullable: f.nullable,
                        table: Some(name.clone()),
                    })
                    .collect();
                PlanSchema::from_fields(fields)
            } else {
                let fields = cte_query
                    .schema()
                    .fields
                    .iter()
                    .map(|f| PlanField {
                        name: f.name.clone(),
                        data_type: f.data_type.clone(),
                        nullable: f.nullable,
                        table: Some(name.clone()),
                    })
                    .collect();
                PlanSchema::from_fields(fields)
            };
            self.cte_schemas
                .borrow_mut()
                .insert(name.clone(), cte_schema);

            let materialized = cte
                .materialized
                .as_ref()
                .map(|m| matches!(m, ast::CteAsMaterialized::Materialized));
            ctes.push(CteDefinition {
                name,
                columns,
                query: Box::new(cte_query),
                recursive: with_clause.recursive,
                materialized,
            });
        }
        Ok(ctes)
    }

    fn get_recursive_cte_anchor_schema(
        &self,
        query: &ast::Query,
        cte_name: &str,
        columns: &Option<Vec<String>>,
    ) -> Result<PlanSchema> {
        let anchor_expr = self.find_anchor_set_expr(&query.body, cte_name);
        let anchor_plan = self.plan_set_expr(anchor_expr)?;

        let fields = if let Some(col_names) = columns {
            anchor_plan
                .schema()
                .fields
                .iter()
                .zip(col_names.iter())
                .map(|(f, col_name)| PlanField {
                    name: col_name.clone(),
                    data_type: f.data_type.clone(),
                    nullable: f.nullable,
                    table: Some(cte_name.to_string()),
                })
                .collect()
        } else {
            anchor_plan
                .schema()
                .fields
                .iter()
                .map(|f| PlanField {
                    name: f.name.clone(),
                    data_type: f.data_type.clone(),
                    nullable: f.nullable,
                    table: Some(cte_name.to_string()),
                })
                .collect()
        };

        Ok(PlanSchema::from_fields(fields))
    }

    fn find_anchor_set_expr<'b>(&self, set_expr: &'b SetExpr, cte_name: &str) -> &'b SetExpr {
        match set_expr {
            SetExpr::SetOperation {
                op: ast::SetOperator::Union,
                left,
                ..
            } => {
                if !self.set_expr_references_table(left, cte_name) {
                    return left;
                }
                self.find_anchor_set_expr(left, cte_name)
            }
            _ => set_expr,
        }
    }

    fn set_expr_references_table(&self, set_expr: &SetExpr, table_name: &str) -> bool {
        match set_expr {
            SetExpr::Select(select) => self.select_references_table(select, table_name),
            SetExpr::Query(query) => self.query_references_table(query, table_name),
            SetExpr::SetOperation { left, right, .. } => {
                self.set_expr_references_table(left, table_name)
                    || self.set_expr_references_table(right, table_name)
            }
            SetExpr::Values(_) => false,
            _ => false,
        }
    }

    fn select_references_table(&self, select: &ast::Select, table_name: &str) -> bool {
        for table_with_joins in &select.from {
            if self.table_factor_references_table(&table_with_joins.relation, table_name) {
                return true;
            }
            for join in &table_with_joins.joins {
                if self.table_factor_references_table(&join.relation, table_name) {
                    return true;
                }
            }
        }
        false
    }

    fn table_factor_references_table(&self, factor: &TableFactor, table_name: &str) -> bool {
        match factor {
            TableFactor::Table { name, .. } => {
                object_name_to_raw_string(name).eq_ignore_ascii_case(table_name)
            }
            TableFactor::Derived { subquery, .. } => {
                self.query_references_table(subquery, table_name)
            }
            _ => false,
        }
    }

    fn query_references_table(&self, query: &ast::Query, table_name: &str) -> bool {
        self.set_expr_references_table(&query.body, table_name)
    }
}
