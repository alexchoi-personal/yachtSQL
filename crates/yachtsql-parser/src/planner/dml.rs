#![coverage(off)]

use sqlparser::ast::{self, TableFactor};
use yachtsql_common::error::{Error, Result};
use yachtsql_ir::{Assignment, LogicalPlan, MergeClause, PlanSchema};

use super::{Planner, object_name_to_raw_string, table_object_to_raw_string};
use crate::CatalogProvider;
use crate::expr_planner::ExprPlanner;

impl<'a, C: CatalogProvider> Planner<'a, C> {
    pub(super) fn plan_insert(&self, insert: &ast::Insert) -> Result<LogicalPlan> {
        let table_name = table_object_to_raw_string(&insert.table);

        let columns: Vec<String> = insert.columns.iter().map(|c| c.value.clone()).collect();

        let source = if let Some(ref src) = insert.source {
            self.plan_query(src)?
        } else {
            return Err(Error::parse_error("INSERT requires a source"));
        };

        Ok(LogicalPlan::Insert {
            table_name,
            columns,
            source: Box::new(source),
        })
    }

    pub(super) fn plan_update(
        &self,
        table: &ast::TableWithJoins,
        assignments: &[ast::Assignment],
        from: Option<&ast::UpdateTableFromKind>,
        selection: Option<&ast::Expr>,
    ) -> Result<LogicalPlan> {
        let (table_name, alias) = match &table.relation {
            TableFactor::Table { name, alias, .. } => (
                object_name_to_raw_string(name),
                alias.as_ref().map(|a| a.name.value.clone()),
            ),
            _ => return Err(Error::parse_error("UPDATE requires a table name")),
        };

        let storage_schema = self
            .catalog
            .get_table_schema(&table_name)
            .ok_or_else(|| Error::table_not_found(&table_name))?;
        let target_qualifier = alias.as_deref().or(Some(&table_name));
        let target_schema = self.storage_schema_to_plan_schema(&storage_schema, target_qualifier);

        let (from_plan, combined_schema) = match from {
            Some(ast::UpdateTableFromKind::AfterSet(from_tables))
            | Some(ast::UpdateTableFromKind::BeforeSet(from_tables)) => {
                let from_plan = self.plan_from(from_tables)?;
                let from_schema = from_plan.schema();
                let mut combined_fields = target_schema.fields.clone();
                combined_fields.extend(from_schema.fields.clone());
                (
                    Some(Box::new(from_plan)),
                    PlanSchema::from_fields(combined_fields),
                )
            }
            None => (None, target_schema.clone()),
        };

        self.with_outer_schema(&combined_schema);
        let subquery_planner = |query: &ast::Query| self.plan_query(query);

        let mut plan_assignments = Vec::new();
        for assign in assignments {
            let column = match &assign.target {
                ast::AssignmentTarget::ColumnName(names) => names.to_string(),
                ast::AssignmentTarget::Tuple(parts) => parts
                    .iter()
                    .map(|p| p.to_string())
                    .collect::<Vec<_>>()
                    .join(", "),
            };
            let value = ExprPlanner::plan_expr_with_subquery(
                &assign.value,
                &combined_schema,
                Some(&subquery_planner),
            )?;
            plan_assignments.push(Assignment { column, value });
        }
        let filter = selection
            .map(|s| {
                ExprPlanner::plan_expr_with_subquery(s, &combined_schema, Some(&subquery_planner))
            })
            .transpose()?;

        self.clear_outer_schema();

        Ok(LogicalPlan::Update {
            table_name,
            alias,
            assignments: plan_assignments,
            from: from_plan,
            filter,
        })
    }

    pub(super) fn plan_delete(&self, delete: &ast::Delete) -> Result<LogicalPlan> {
        let (table_name, alias) = match &delete.from {
            ast::FromTable::WithFromKeyword(tables) => {
                tables.first().and_then(|t| match &t.relation {
                    TableFactor::Table { name, alias, .. } => Some((
                        object_name_to_raw_string(name),
                        alias.as_ref().map(|a| a.name.value.clone()),
                    )),
                    _ => None,
                })
            }
            ast::FromTable::WithoutKeyword(tables) => {
                tables.first().and_then(|t| match &t.relation {
                    TableFactor::Table { name, alias, .. } => Some((
                        object_name_to_raw_string(name),
                        alias.as_ref().map(|a| a.name.value.clone()),
                    )),
                    _ => None,
                })
            }
        }
        .ok_or_else(|| Error::parse_error("DELETE requires a table name"))?;

        let storage_schema = self
            .catalog
            .get_table_schema(&table_name)
            .ok_or_else(|| Error::table_not_found(&table_name))?;
        let schema_qualifier = alias.as_deref().or(Some(&table_name));
        let schema = self.storage_schema_to_plan_schema(&storage_schema, schema_qualifier);

        let subquery_planner = |query: &ast::Query| self.plan_query(query);
        let filter = delete
            .selection
            .as_ref()
            .map(|s| ExprPlanner::plan_expr_with_subquery(s, &schema, Some(&subquery_planner)))
            .transpose()?;

        Ok(LogicalPlan::Delete {
            table_name,
            alias,
            filter,
        })
    }

    pub(super) fn plan_merge(
        &self,
        table: &TableFactor,
        source: &TableFactor,
        on: &ast::Expr,
        clauses: &[ast::MergeClause],
    ) -> Result<LogicalPlan> {
        let target_name = match table {
            TableFactor::Table { name, .. } => object_name_to_raw_string(name),
            _ => return Err(Error::parse_error("MERGE target must be a table")),
        };

        let target_alias = match table {
            TableFactor::Table { alias, .. } => alias.as_ref().map(|a| a.name.value.clone()),
            _ => None,
        };

        let target_storage_schema = self
            .catalog
            .get_table_schema(&target_name)
            .ok_or_else(|| Error::table_not_found(&target_name))?;
        let target_schema = self.storage_schema_to_plan_schema(
            &target_storage_schema,
            target_alias.as_deref().or(Some(&target_name)),
        );

        let source_plan = self.plan_table_factor(source, None)?;
        let source_schema = source_plan.schema().clone();

        let source_alias = match source {
            TableFactor::Table { alias, .. } => alias.as_ref().map(|a| a.name.value.clone()),
            TableFactor::Derived { alias, .. } => alias.as_ref().map(|a| a.name.value.clone()),
            _ => None,
        };

        let source_schema_with_alias = if let Some(ref alias) = source_alias {
            PlanSchema::from_fields(
                source_schema
                    .fields
                    .iter()
                    .map(|f| {
                        let mut field = f.clone();
                        field.table = Some(alias.clone());
                        let base_name = f.name.split('.').next_back().unwrap_or(&f.name);
                        field.name = format!("{}.{}", alias, base_name);
                        field
                    })
                    .collect(),
            )
        } else {
            source_schema.clone()
        };

        let combined_schema = target_schema
            .clone()
            .merge(source_schema_with_alias.clone());

        let subquery_planner = |query: &ast::Query| self.plan_query(query);
        let on_expr =
            ExprPlanner::plan_expr_with_subquery(on, &combined_schema, Some(&subquery_planner))?;

        let mut merge_clauses = Vec::new();
        for clause in clauses {
            let planned_clause = self.plan_merge_clause(
                clause,
                &combined_schema,
                &target_schema,
                &source_schema_with_alias,
                &subquery_planner,
            )?;
            merge_clauses.push(planned_clause);
        }

        Ok(LogicalPlan::Merge {
            target_table: target_name,
            source: Box::new(source_plan),
            on: on_expr,
            clauses: merge_clauses,
        })
    }

    pub(super) fn plan_merge_clause<F>(
        &self,
        clause: &ast::MergeClause,
        combined_schema: &PlanSchema,
        target_schema: &PlanSchema,
        _source_schema: &PlanSchema,
        subquery_planner: &F,
    ) -> Result<MergeClause>
    where
        F: Fn(&ast::Query) -> Result<LogicalPlan>,
    {
        let condition = clause
            .predicate
            .as_ref()
            .map(|p| {
                ExprPlanner::plan_expr_with_subquery(p, combined_schema, Some(subquery_planner))
            })
            .transpose()?;

        match &clause.clause_kind {
            ast::MergeClauseKind::Matched => match &clause.action {
                ast::MergeAction::Update { assignments } => {
                    let mut plan_assignments = Vec::new();
                    for assign in assignments {
                        let column = match &assign.target {
                            ast::AssignmentTarget::ColumnName(names) => names.to_string(),
                            ast::AssignmentTarget::Tuple(parts) => parts
                                .iter()
                                .map(|p| p.to_string())
                                .collect::<Vec<_>>()
                                .join(", "),
                        };
                        let value = ExprPlanner::plan_expr_with_subquery(
                            &assign.value,
                            combined_schema,
                            Some(subquery_planner),
                        )?;
                        plan_assignments.push(Assignment { column, value });
                    }
                    Ok(MergeClause::MatchedUpdate {
                        condition,
                        assignments: plan_assignments,
                    })
                }
                ast::MergeAction::Delete => Ok(MergeClause::MatchedDelete { condition }),
                ast::MergeAction::Insert(_) => Err(Error::parse_error(
                    "INSERT action not valid for WHEN MATCHED",
                )),
            },
            ast::MergeClauseKind::NotMatched | ast::MergeClauseKind::NotMatchedByTarget => {
                match &clause.action {
                    ast::MergeAction::Insert(insert_expr) => {
                        let columns: Vec<String> = insert_expr
                            .columns
                            .iter()
                            .map(|c| c.value.clone())
                            .collect();

                        let values = match &insert_expr.kind {
                            ast::MergeInsertKind::Row => Vec::new(),
                            ast::MergeInsertKind::Values(vals) => {
                                if let Some(first_row) = vals.rows.first() {
                                    first_row
                                        .iter()
                                        .map(|e| {
                                            ExprPlanner::plan_expr_with_subquery(
                                                e,
                                                combined_schema,
                                                Some(subquery_planner),
                                            )
                                        })
                                        .collect::<Result<Vec<_>>>()?
                                } else {
                                    Vec::new()
                                }
                            }
                        };

                        Ok(MergeClause::NotMatched {
                            condition,
                            columns,
                            values,
                        })
                    }
                    ast::MergeAction::Update { .. } | ast::MergeAction::Delete => Err(
                        Error::parse_error("UPDATE/DELETE actions not valid for WHEN NOT MATCHED"),
                    ),
                }
            }
            ast::MergeClauseKind::NotMatchedBySource => match &clause.action {
                ast::MergeAction::Update { assignments } => {
                    let mut plan_assignments = Vec::new();
                    for assign in assignments {
                        let column = match &assign.target {
                            ast::AssignmentTarget::ColumnName(names) => names.to_string(),
                            ast::AssignmentTarget::Tuple(parts) => parts
                                .iter()
                                .map(|p| p.to_string())
                                .collect::<Vec<_>>()
                                .join(", "),
                        };
                        let value = ExprPlanner::plan_expr_with_subquery(
                            &assign.value,
                            target_schema,
                            Some(subquery_planner),
                        )?;
                        plan_assignments.push(Assignment { column, value });
                    }
                    Ok(MergeClause::NotMatchedBySource {
                        condition,
                        assignments: plan_assignments,
                    })
                }
                ast::MergeAction::Delete => Ok(MergeClause::NotMatchedBySourceDelete { condition }),
                ast::MergeAction::Insert(_) => Err(Error::parse_error(
                    "INSERT action not valid for WHEN NOT MATCHED BY SOURCE",
                )),
            },
        }
    }
}
