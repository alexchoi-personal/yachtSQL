#![coverage(off)]

use sqlparser::ast;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::DataType;
use yachtsql_ir::{
    BinaryOp, Expr, Literal, LogicalPlan, PlanField, PlanSchema, WhenClause, WindowSpec,
};

use super::super::object_name_to_raw_string;
use super::Planner;
use crate::CatalogProvider;
use crate::expr_planner::ExprPlanner;

impl<'a, C: CatalogProvider> Planner<'a, C> {
    pub(super) fn plan_projection(
        &self,
        input: LogicalPlan,
        items: &[ast::SelectItem],
        named_windows: &[ast::NamedWindowDefinition],
    ) -> Result<LogicalPlan> {
        let mut expressions = Vec::new();
        let mut fields = Vec::new();
        let subquery_planner = |query: &ast::Query| self.plan_query(query);
        let udf_resolver = |name: &str| self.catalog.get_function(name);

        for item in items {
            match item {
                ast::SelectItem::UnnamedExpr(expr) => {
                    let planned_expr = ExprPlanner::plan_expr_with_udf_resolver(
                        expr,
                        input.schema(),
                        Some(&subquery_planner),
                        named_windows,
                        Some(&udf_resolver),
                    )?;
                    let name = self.expr_name(expr);
                    let data_type = self.infer_expr_type(&planned_expr, input.schema());
                    fields.push(PlanField::new(name, data_type));
                    expressions.push(planned_expr);
                }
                ast::SelectItem::ExprWithAlias { expr, alias } => {
                    let planned_expr = ExprPlanner::plan_expr_with_udf_resolver(
                        expr,
                        input.schema(),
                        Some(&subquery_planner),
                        named_windows,
                        Some(&udf_resolver),
                    )?;
                    let data_type = self.infer_expr_type(&planned_expr, input.schema());
                    fields.push(PlanField::new(alias.value.clone(), data_type));
                    expressions.push(planned_expr);
                }
                ast::SelectItem::Wildcard(opts) => {
                    let except_cols = Self::get_except_columns(opts);
                    let replace_map =
                        Self::get_replace_columns(opts, input.schema(), named_windows)?;
                    for (i, field) in input.schema().fields.iter().enumerate() {
                        if !except_cols.contains(&field.name.to_lowercase()) {
                            if let Some((replaced_expr, data_type)) =
                                replace_map.get(&field.name.to_lowercase())
                            {
                                expressions.push(replaced_expr.clone());
                                fields.push(PlanField::new(field.name.clone(), data_type.clone()));
                            } else {
                                expressions.push(Expr::Column {
                                    table: field.table.clone(),
                                    name: field.name.clone(),
                                    index: Some(i),
                                });
                                fields.push(field.clone());
                            }
                        }
                    }
                }
                ast::SelectItem::QualifiedWildcard(kind, _) => match kind {
                    ast::SelectItemQualifiedWildcardKind::ObjectName(obj_name) => {
                        let table_name = object_name_to_raw_string(obj_name).to_uppercase();
                        for (i, field) in input.schema().fields.iter().enumerate() {
                            if field
                                .table
                                .as_ref()
                                .is_some_and(|t| t.to_uppercase() == table_name)
                            {
                                expressions.push(Expr::Column {
                                    table: field.table.clone(),
                                    name: field.name.clone(),
                                    index: Some(i),
                                });
                                fields.push(field.clone());
                            }
                        }
                    }
                    ast::SelectItemQualifiedWildcardKind::Expr(expr) => {
                        let planned_expr = ExprPlanner::plan_expr_with_udf_resolver(
                            expr,
                            input.schema(),
                            Some(&subquery_planner),
                            named_windows,
                            Some(&udf_resolver),
                        )?;
                        let expr_type = self.infer_expr_type(&planned_expr, input.schema());
                        match expr_type {
                            DataType::Struct(struct_fields) => {
                                for struct_field in struct_fields {
                                    expressions.push(Expr::StructAccess {
                                        expr: Box::new(planned_expr.clone()),
                                        field: struct_field.name.clone(),
                                    });
                                    fields.push(PlanField::new(
                                        struct_field.name.clone(),
                                        struct_field.data_type.clone(),
                                    ));
                                }
                            }
                            _ => {
                                return Err(Error::invalid_query(format!(
                                    "Cannot use .* on non-struct type: {:?}",
                                    expr_type
                                )));
                            }
                        }
                    }
                },
            }
        }

        let mut window_funcs: Vec<Expr> = Vec::new();
        let mut window_expr_indices = Vec::new();
        for (i, expr) in expressions.iter().enumerate() {
            if Self::expr_has_window(expr)
                && let Some(wf) = Self::extract_window_function(expr)
            {
                window_funcs.push(wf);
                window_expr_indices.push(i);
            }
        }

        if window_funcs.is_empty() {
            return Ok(LogicalPlan::Project {
                input: Box::new(input),
                expressions,
                schema: PlanSchema::from_fields(fields),
            });
        }

        let window_groups = Self::group_windows_by_spec(&window_funcs);

        let mut current_plan = input;
        let mut all_window_exprs: Vec<Expr> = Vec::new();

        for group in &window_groups {
            let _base_field_count = current_plan.schema().fields.len();
            let mut window_schema_fields = current_plan.schema().fields.clone();
            for (i, wf) in group.iter().enumerate() {
                let window_type = Self::compute_expr_type(wf, current_plan.schema());
                window_schema_fields.push(PlanField::new(
                    format!("__window_{}", all_window_exprs.len() + i),
                    window_type,
                ));
            }
            let window_schema = PlanSchema::from_fields(window_schema_fields);

            current_plan = LogicalPlan::Window {
                input: Box::new(current_plan),
                window_exprs: group.clone(),
                schema: window_schema,
            };

            all_window_exprs.extend(group.iter().cloned());
        }

        let window_schema = current_plan.schema().clone();
        let input_field_count = window_schema.fields.len() - all_window_exprs.len();

        let mut new_expressions = Vec::new();
        let mut window_offset = 0usize;
        for (i, expr) in expressions.iter().enumerate() {
            if window_expr_indices.contains(&i) {
                let window_func = &window_funcs[window_offset];
                let window_idx = all_window_exprs
                    .iter()
                    .position(|w| w == window_func)
                    .unwrap_or(window_offset);
                let col_idx = input_field_count + window_idx;
                let col_name = format!("__window_{}", window_idx);
                let replaced = Self::replace_window_with_column(expr.clone(), &col_name, col_idx);
                new_expressions.push(Self::remap_column_indices(replaced, &window_schema));
                window_offset += 1;
            } else {
                new_expressions.push(Self::remap_column_indices(expr.clone(), &window_schema));
            }
        }

        Ok(LogicalPlan::Project {
            input: Box::new(current_plan),
            expressions: new_expressions,
            schema: PlanSchema::from_fields(fields),
        })
    }

    pub(super) fn expr_has_window(expr: &Expr) -> bool {
        match expr {
            Expr::Window { .. } | Expr::AggregateWindow { .. } => true,
            Expr::BinaryOp { left, right, .. } => {
                Self::expr_has_window(left) || Self::expr_has_window(right)
            }
            Expr::UnaryOp { expr, .. } => Self::expr_has_window(expr),
            Expr::Case {
                operand,
                when_clauses,
                else_result,
            } => {
                operand.as_ref().is_some_and(|e| Self::expr_has_window(e))
                    || when_clauses.iter().any(|w| {
                        Self::expr_has_window(&w.condition) || Self::expr_has_window(&w.result)
                    })
                    || else_result
                        .as_ref()
                        .is_some_and(|e| Self::expr_has_window(e))
            }
            Expr::Cast { expr, .. } => Self::expr_has_window(expr),
            Expr::ScalarFunction { args, .. } => args.iter().any(Self::expr_has_window),
            Expr::Alias { expr, .. } => Self::expr_has_window(expr),
            _ => false,
        }
    }

    pub(super) fn extract_window_function(expr: &Expr) -> Option<Expr> {
        match expr {
            Expr::Window { .. } | Expr::AggregateWindow { .. } => Some(expr.clone()),
            Expr::BinaryOp { left, right, .. } => {
                Self::extract_window_function(left).or_else(|| Self::extract_window_function(right))
            }
            Expr::UnaryOp { expr, .. } => Self::extract_window_function(expr),
            Expr::Case {
                operand,
                when_clauses,
                else_result,
            } => {
                if let Some(op) = operand
                    && let Some(wf) = Self::extract_window_function(op)
                {
                    return Some(wf);
                }
                for clause in when_clauses {
                    if let Some(wf) = Self::extract_window_function(&clause.condition) {
                        return Some(wf);
                    }
                    if let Some(wf) = Self::extract_window_function(&clause.result) {
                        return Some(wf);
                    }
                }
                if let Some(e) = else_result {
                    return Self::extract_window_function(e);
                }
                None
            }
            Expr::Cast { expr, .. } => Self::extract_window_function(expr),
            Expr::ScalarFunction { args, .. } => {
                for arg in args {
                    if let Some(wf) = Self::extract_window_function(arg) {
                        return Some(wf);
                    }
                }
                None
            }
            Expr::Alias { expr, .. } => Self::extract_window_function(expr),
            _ => None,
        }
    }

    pub(super) fn replace_window_with_column(expr: Expr, col_name: &str, col_idx: usize) -> Expr {
        match expr {
            Expr::Window { .. } | Expr::AggregateWindow { .. } => Expr::Column {
                table: None,
                name: col_name.to_string(),
                index: Some(col_idx),
            },
            Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
                left: Box::new(Self::replace_window_with_column(*left, col_name, col_idx)),
                op,
                right: Box::new(Self::replace_window_with_column(*right, col_name, col_idx)),
            },
            Expr::UnaryOp { op, expr } => Expr::UnaryOp {
                op,
                expr: Box::new(Self::replace_window_with_column(*expr, col_name, col_idx)),
            },
            Expr::Case {
                operand,
                when_clauses,
                else_result,
            } => Expr::Case {
                operand: operand
                    .map(|e| Box::new(Self::replace_window_with_column(*e, col_name, col_idx))),
                when_clauses: when_clauses
                    .into_iter()
                    .map(|w| WhenClause {
                        condition: Self::replace_window_with_column(w.condition, col_name, col_idx),
                        result: Self::replace_window_with_column(w.result, col_name, col_idx),
                    })
                    .collect(),
                else_result: else_result
                    .map(|e| Box::new(Self::replace_window_with_column(*e, col_name, col_idx))),
            },
            Expr::Cast {
                expr,
                data_type,
                safe,
            } => Expr::Cast {
                expr: Box::new(Self::replace_window_with_column(*expr, col_name, col_idx)),
                data_type,
                safe,
            },
            Expr::ScalarFunction { name, args } => Expr::ScalarFunction {
                name,
                args: args
                    .into_iter()
                    .map(|a| Self::replace_window_with_column(a, col_name, col_idx))
                    .collect(),
            },
            Expr::Alias { expr, name } => Expr::Alias {
                expr: Box::new(Self::replace_window_with_column(*expr, col_name, col_idx)),
                name,
            },
            other => other,
        }
    }

    pub(super) fn extract_all_window_functions(expr: &Expr) -> Vec<Expr> {
        let mut windows = Vec::new();
        Self::collect_window_functions(expr, &mut windows);
        windows
    }

    fn collect_window_functions(expr: &Expr, windows: &mut Vec<Expr>) {
        match expr {
            Expr::Window { .. } | Expr::AggregateWindow { .. } => {
                if !windows.iter().any(|w| w == expr) {
                    windows.push(expr.clone());
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::collect_window_functions(left, windows);
                Self::collect_window_functions(right, windows);
            }
            Expr::UnaryOp { expr, .. } => {
                Self::collect_window_functions(expr, windows);
            }
            Expr::Case {
                operand,
                when_clauses,
                else_result,
            } => {
                if let Some(op) = operand {
                    Self::collect_window_functions(op, windows);
                }
                for clause in when_clauses {
                    Self::collect_window_functions(&clause.condition, windows);
                    Self::collect_window_functions(&clause.result, windows);
                }
                if let Some(e) = else_result {
                    Self::collect_window_functions(e, windows);
                }
            }
            Expr::Cast { expr, .. } => {
                Self::collect_window_functions(expr, windows);
            }
            Expr::ScalarFunction { args, .. } => {
                for arg in args {
                    Self::collect_window_functions(arg, windows);
                }
            }
            Expr::Alias { expr, .. } => {
                Self::collect_window_functions(expr, windows);
            }
            _ => {}
        }
    }

    pub(super) fn group_windows_by_spec(windows: &[Expr]) -> Vec<Vec<Expr>> {
        let mut groups: Vec<(WindowSpec, Vec<Expr>)> = Vec::new();
        for wf in windows {
            let spec = Self::get_window_spec(wf);
            if let Some((_, group)) = groups.iter_mut().find(|(s, _)| s == &spec) {
                group.push(wf.clone());
            } else {
                groups.push((spec, vec![wf.clone()]));
            }
        }
        groups.into_iter().map(|(_, g)| g).collect()
    }

    fn get_window_spec(expr: &Expr) -> WindowSpec {
        match expr {
            Expr::Window {
                partition_by,
                order_by,
                frame,
                ..
            } => WindowSpec {
                partition_by: partition_by.clone(),
                order_by: order_by.clone(),
                frame: frame.clone(),
            },
            Expr::AggregateWindow {
                partition_by,
                order_by,
                frame,
                ..
            } => WindowSpec {
                partition_by: partition_by.clone(),
                order_by: order_by.clone(),
                frame: frame.clone(),
            },
            _ => WindowSpec {
                partition_by: vec![],
                order_by: vec![],
                frame: None,
            },
        }
    }

    pub(super) fn replace_windows_with_columns(
        expr: Expr,
        window_exprs: &[Expr],
        base_col_idx: usize,
    ) -> Expr {
        match &expr {
            Expr::Window { .. } | Expr::AggregateWindow { .. } => {
                for (i, wf) in window_exprs.iter().enumerate() {
                    if wf == &expr {
                        return Expr::Column {
                            table: None,
                            name: format!("__qualify_window_{}", i),
                            index: Some(base_col_idx + i),
                        };
                    }
                }
                expr
            }
            Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
                left: Box::new(Self::replace_windows_with_columns(
                    *left.clone(),
                    window_exprs,
                    base_col_idx,
                )),
                op: *op,
                right: Box::new(Self::replace_windows_with_columns(
                    *right.clone(),
                    window_exprs,
                    base_col_idx,
                )),
            },
            Expr::UnaryOp { op, expr: inner } => Expr::UnaryOp {
                op: *op,
                expr: Box::new(Self::replace_windows_with_columns(
                    *inner.clone(),
                    window_exprs,
                    base_col_idx,
                )),
            },
            Expr::Case {
                operand,
                when_clauses,
                else_result,
            } => Expr::Case {
                operand: operand.clone().map(|e| {
                    Box::new(Self::replace_windows_with_columns(
                        *e,
                        window_exprs,
                        base_col_idx,
                    ))
                }),
                when_clauses: when_clauses
                    .iter()
                    .map(|w| WhenClause {
                        condition: Self::replace_windows_with_columns(
                            w.condition.clone(),
                            window_exprs,
                            base_col_idx,
                        ),
                        result: Self::replace_windows_with_columns(
                            w.result.clone(),
                            window_exprs,
                            base_col_idx,
                        ),
                    })
                    .collect(),
                else_result: else_result.clone().map(|e| {
                    Box::new(Self::replace_windows_with_columns(
                        *e,
                        window_exprs,
                        base_col_idx,
                    ))
                }),
            },
            Expr::Cast {
                expr: inner,
                data_type,
                safe,
            } => Expr::Cast {
                expr: Box::new(Self::replace_windows_with_columns(
                    *inner.clone(),
                    window_exprs,
                    base_col_idx,
                )),
                data_type: data_type.clone(),
                safe: *safe,
            },
            Expr::ScalarFunction { name, args } => Expr::ScalarFunction {
                name: name.clone(),
                args: args
                    .iter()
                    .map(|a| {
                        Self::replace_windows_with_columns(a.clone(), window_exprs, base_col_idx)
                    })
                    .collect(),
            },
            Expr::Alias { expr: inner, name } => Expr::Alias {
                expr: Box::new(Self::replace_windows_with_columns(
                    *inner.clone(),
                    window_exprs,
                    base_col_idx,
                )),
                name: name.clone(),
            },
            _ => expr,
        }
    }

    pub(super) fn remap_column_indices(expr: Expr, schema: &PlanSchema) -> Expr {
        match expr {
            Expr::Column { table, name, .. } => {
                let idx = schema
                    .fields
                    .iter()
                    .position(|f| f.name == name && (table.is_none() || f.table == table));
                Expr::Column {
                    table,
                    name,
                    index: idx,
                }
            }
            Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
                left: Box::new(Self::remap_column_indices(*left, schema)),
                op,
                right: Box::new(Self::remap_column_indices(*right, schema)),
            },
            Expr::UnaryOp { op, expr } => Expr::UnaryOp {
                op,
                expr: Box::new(Self::remap_column_indices(*expr, schema)),
            },
            Expr::Cast {
                expr,
                data_type,
                safe,
            } => Expr::Cast {
                expr: Box::new(Self::remap_column_indices(*expr, schema)),
                data_type,
                safe,
            },
            Expr::ScalarFunction { name, args } => Expr::ScalarFunction {
                name,
                args: args
                    .into_iter()
                    .map(|a| Self::remap_column_indices(a, schema))
                    .collect(),
            },
            Expr::Alias { expr, name } => Expr::Alias {
                expr: Box::new(Self::remap_column_indices(*expr, schema)),
                name,
            },
            other => other,
        }
    }

    pub(super) fn get_except_columns(
        opts: &ast::WildcardAdditionalOptions,
    ) -> std::collections::HashSet<String> {
        opts.opt_except
            .as_ref()
            .map(|except| {
                let mut cols = std::collections::HashSet::new();
                cols.insert(except.first_element.value.to_lowercase());
                for ident in &except.additional_elements {
                    cols.insert(ident.value.to_lowercase());
                }
                cols
            })
            .unwrap_or_default()
    }

    pub(super) fn get_replace_columns(
        opts: &ast::WildcardAdditionalOptions,
        schema: &PlanSchema,
        named_windows: &[ast::NamedWindowDefinition],
    ) -> Result<rustc_hash::FxHashMap<String, (Expr, DataType)>> {
        let mut replace_map = rustc_hash::FxHashMap::default();
        if let Some(replace) = &opts.opt_replace {
            for item in &replace.items {
                let col_name = item.column_name.value.to_lowercase();
                let expr = ExprPlanner::plan_expr_with_named_windows(
                    &item.expr,
                    schema,
                    None,
                    named_windows,
                )?;
                let data_type = Self::infer_expr_type_static(&expr, schema);
                replace_map.insert(col_name, (expr, data_type));
            }
        }
        Ok(replace_map)
    }

    pub(in crate::planner) fn infer_expr_type_static(expr: &Expr, schema: &PlanSchema) -> DataType {
        match expr {
            Expr::Literal(lit) => match lit {
                Literal::Int64(_) => DataType::Int64,
                Literal::Float64(_) => DataType::Float64,
                Literal::String(_) => DataType::String,
                Literal::Bool(_) => DataType::Bool,
                Literal::Null => DataType::String,
                Literal::Bytes(_) => DataType::Bytes,
                Literal::Date(_) => DataType::Date,
                Literal::Time(_) => DataType::Time,
                Literal::Timestamp(_) => DataType::Timestamp,
                Literal::Datetime(_) => DataType::DateTime,
                Literal::Numeric(_) => DataType::Numeric(None),
                Literal::BigNumeric(_) => DataType::BigNumeric,
                Literal::Interval { .. } => DataType::Interval,
                Literal::Array(_) => DataType::Array(Box::new(DataType::String)),
                Literal::Struct(_) => DataType::Struct(vec![]),
                Literal::Json(_) => DataType::Json,
            },
            Expr::Column { index, .. } => index
                .and_then(|i| schema.fields.get(i))
                .map(|f| f.data_type.clone())
                .unwrap_or(DataType::String),
            Expr::BinaryOp { left, op, right } => {
                let left_type = Self::infer_expr_type_static(left, schema);
                let right_type = Self::infer_expr_type_static(right, schema);
                match op {
                    BinaryOp::Eq
                    | BinaryOp::NotEq
                    | BinaryOp::Lt
                    | BinaryOp::LtEq
                    | BinaryOp::Gt
                    | BinaryOp::GtEq
                    | BinaryOp::And
                    | BinaryOp::Or => DataType::Bool,
                    BinaryOp::Add
                    | BinaryOp::Sub
                    | BinaryOp::Mul
                    | BinaryOp::Div
                    | BinaryOp::Mod => {
                        if matches!(left_type, DataType::Float64)
                            || matches!(right_type, DataType::Float64)
                        {
                            DataType::Float64
                        } else if matches!(left_type, DataType::Numeric(_))
                            || matches!(right_type, DataType::Numeric(_))
                        {
                            DataType::Numeric(None)
                        } else {
                            DataType::Int64
                        }
                    }
                    BinaryOp::Concat => DataType::String,
                    BinaryOp::BitwiseAnd
                    | BinaryOp::BitwiseOr
                    | BinaryOp::BitwiseXor
                    | BinaryOp::ShiftLeft
                    | BinaryOp::ShiftRight => DataType::Int64,
                }
            }
            Expr::Cast { data_type, .. } => data_type.clone(),
            _ => DataType::String,
        }
    }
}
