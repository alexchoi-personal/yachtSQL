#![coverage(off)]

pub mod handlers;

use std::collections::HashMap;

pub use handlers::extract_field;
use handlers::{
    eval_alias, eval_array, eval_array_access, eval_at_time_zone, eval_between, eval_binary_op_ext,
    eval_extract, eval_in_list, eval_in_unnest, eval_interval, eval_is_distinct_from,
    eval_json_access, eval_like, eval_overlay, eval_position, eval_scalar_function, eval_struct,
    eval_struct_access, eval_substring, eval_trim, eval_typed_string, eval_unary_op_ext,
    eval_variable,
};
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;
use yachtsql_ir::{BinaryOp, Expr, Literal, UnaryOp};
use yachtsql_storage::{Column, Schema, Table};

use crate::value_evaluator::UserFunctionDef;

pub struct ColumnarEvaluator<'a> {
    schema: &'a Schema,
    variables: Option<&'a HashMap<String, Value>>,
    system_variables: Option<&'a HashMap<String, Value>>,
    user_functions: Option<&'a HashMap<String, UserFunctionDef>>,
}

impl<'a> ColumnarEvaluator<'a> {
    pub fn new(schema: &'a Schema) -> Self {
        Self {
            schema,
            variables: None,
            system_variables: None,
            user_functions: None,
        }
    }

    pub fn with_variables(mut self, variables: &'a HashMap<String, Value>) -> Self {
        self.variables = Some(variables);
        self
    }

    pub fn with_system_variables(mut self, system_variables: &'a HashMap<String, Value>) -> Self {
        self.system_variables = Some(system_variables);
        self
    }

    pub fn with_user_functions(
        mut self,
        user_functions: &'a HashMap<String, UserFunctionDef>,
    ) -> Self {
        self.user_functions = Some(user_functions);
        self
    }

    pub fn variables(&self) -> Option<&'a HashMap<String, Value>> {
        self.variables
    }

    pub fn system_variables(&self) -> Option<&'a HashMap<String, Value>> {
        self.system_variables
    }

    pub fn user_functions(&self) -> Option<&'a HashMap<String, UserFunctionDef>> {
        self.user_functions
    }

    pub fn can_evaluate(&self, expr: &Expr) -> bool {
        match expr {
            Expr::Literal(_) => true,
            Expr::Column { .. } => true,
            Expr::BinaryOp { left, op, right } => {
                self.can_evaluate_binary_op(*op)
                    && self.can_evaluate(left)
                    && self.can_evaluate(right)
            }
            Expr::UnaryOp { op, expr } => {
                self.can_evaluate_unary_op(*op) && self.can_evaluate(expr)
            }
            Expr::IsNull { expr, .. } => self.can_evaluate(expr),
            Expr::Cast { expr, .. } => self.can_evaluate(expr),
            Expr::Case {
                operand,
                when_clauses,
                else_result,
            } => {
                operand.as_ref().is_none_or(|e| self.can_evaluate(e))
                    && when_clauses
                        .iter()
                        .all(|wc| self.can_evaluate(&wc.condition) && self.can_evaluate(&wc.result))
                    && else_result.as_ref().is_none_or(|e| self.can_evaluate(e))
            }
            Expr::ScalarFunction { args, .. } => args.iter().all(|a| self.can_evaluate(a)),
            Expr::Between {
                expr, low, high, ..
            } => self.can_evaluate(expr) && self.can_evaluate(low) && self.can_evaluate(high),
            Expr::Like { expr, pattern, .. } => {
                self.can_evaluate(expr) && self.can_evaluate(pattern)
            }
            Expr::InList { expr, list, .. } => {
                self.can_evaluate(expr) && list.iter().all(|e| self.can_evaluate(e))
            }
            Expr::InUnnest {
                expr, array_expr, ..
            } => self.can_evaluate(expr) && self.can_evaluate(array_expr),
            Expr::IsDistinctFrom { left, right, .. } => {
                self.can_evaluate(left) && self.can_evaluate(right)
            }
            Expr::Extract { expr, .. } => self.can_evaluate(expr),
            Expr::Substring {
                expr,
                start,
                length,
            } => {
                self.can_evaluate(expr)
                    && start.as_ref().is_none_or(|s| self.can_evaluate(s))
                    && length.as_ref().is_none_or(|l| self.can_evaluate(l))
            }
            Expr::Trim {
                expr, trim_what, ..
            } => self.can_evaluate(expr) && trim_what.as_ref().is_none_or(|t| self.can_evaluate(t)),
            Expr::Position { substr, string } => {
                self.can_evaluate(substr) && self.can_evaluate(string)
            }
            Expr::Overlay {
                expr,
                overlay_what,
                overlay_from,
                overlay_for,
            } => {
                self.can_evaluate(expr)
                    && self.can_evaluate(overlay_what)
                    && self.can_evaluate(overlay_from)
                    && overlay_for.as_ref().is_none_or(|f| self.can_evaluate(f))
            }
            Expr::Array { elements, .. } => elements.iter().all(|e| self.can_evaluate(e)),
            Expr::ArrayAccess { array, index } => {
                self.can_evaluate(array) && self.can_evaluate(index)
            }
            Expr::Struct { fields } => fields.iter().all(|(_, e)| self.can_evaluate(e)),
            Expr::StructAccess { expr, .. } => self.can_evaluate(expr),
            Expr::TypedString { .. } => true,
            Expr::Interval { value, .. } => self.can_evaluate(value),
            Expr::Alias { expr, .. } => self.can_evaluate(expr),
            Expr::Variable { .. } => true,
            Expr::AtTimeZone {
                timestamp,
                time_zone,
            } => self.can_evaluate(timestamp) && self.can_evaluate(time_zone),
            Expr::JsonAccess { expr, .. } => self.can_evaluate(expr),
            _ => false,
        }
    }

    fn can_evaluate_binary_op(&self, op: BinaryOp) -> bool {
        matches!(
            op,
            BinaryOp::Add
                | BinaryOp::Sub
                | BinaryOp::Mul
                | BinaryOp::Eq
                | BinaryOp::NotEq
                | BinaryOp::Lt
                | BinaryOp::LtEq
                | BinaryOp::Gt
                | BinaryOp::GtEq
                | BinaryOp::And
                | BinaryOp::Or
                | BinaryOp::Concat
                | BinaryOp::BitwiseAnd
                | BinaryOp::BitwiseOr
                | BinaryOp::BitwiseXor
                | BinaryOp::ShiftLeft
                | BinaryOp::ShiftRight
        )
    }

    fn can_evaluate_unary_op(&self, op: UnaryOp) -> bool {
        matches!(
            op,
            UnaryOp::Not | UnaryOp::Minus | UnaryOp::Plus | UnaryOp::BitwiseNot
        )
    }

    pub fn evaluate(&self, expr: &Expr, table: &Table) -> Result<Column> {
        match expr {
            Expr::Literal(lit) => self.eval_literal(lit, table.row_count()),
            Expr::Column {
                table: tbl,
                name,
                index,
            } => self.eval_column_ref(tbl.as_deref(), name, *index, table),
            Expr::BinaryOp { left, op, right } => self.eval_binary_op(left, *op, right, table),
            Expr::UnaryOp { op, expr } => self.eval_unary_op(*op, expr, table),
            Expr::IsNull { expr, negated } => self.eval_is_null(expr, *negated, table),
            Expr::Cast {
                expr,
                data_type,
                safe,
            } => self.eval_cast(expr, data_type, *safe, table),
            Expr::Case {
                operand,
                when_clauses,
                else_result,
            } => self.eval_case(
                operand.as_deref(),
                when_clauses,
                else_result.as_deref(),
                table,
            ),
            Expr::ScalarFunction { name, args } => eval_scalar_function(self, name, args, table),
            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => eval_between(self, expr, low, high, *negated, table),
            Expr::Like {
                expr,
                pattern,
                negated,
                case_insensitive,
            } => eval_like(self, expr, pattern, *negated, *case_insensitive, table),
            Expr::InList {
                expr,
                list,
                negated,
            } => eval_in_list(self, expr, list, *negated, table),
            Expr::InUnnest {
                expr,
                array_expr,
                negated,
            } => eval_in_unnest(self, expr, array_expr, *negated, table),
            Expr::IsDistinctFrom {
                left,
                right,
                negated,
            } => eval_is_distinct_from(self, left, right, *negated, table),
            Expr::Extract { field, expr } => eval_extract(self, *field, expr, table),
            Expr::Substring {
                expr,
                start,
                length,
            } => eval_substring(self, expr, start.as_deref(), length.as_deref(), table),
            Expr::Trim {
                expr,
                trim_what,
                trim_where,
            } => eval_trim(self, expr, trim_what.as_deref(), *trim_where, table),
            Expr::Position { substr, string } => eval_position(self, substr, string, table),
            Expr::Overlay {
                expr,
                overlay_what,
                overlay_from,
                overlay_for,
            } => eval_overlay(
                self,
                expr,
                overlay_what,
                overlay_from,
                overlay_for.as_deref(),
                table,
            ),
            Expr::Array { elements, .. } => eval_array(self, elements, table),
            Expr::ArrayAccess { array, index } => {
                eval_array_access(self, array, index, false, table)
            }
            Expr::Struct { fields } => eval_struct(self, fields, table),
            Expr::StructAccess { expr, field } => eval_struct_access(self, expr, field, table),
            Expr::TypedString { data_type, value } => {
                eval_typed_string(data_type, value, table.row_count())
            }
            Expr::Interval {
                value,
                leading_field,
            } => self.eval_interval_expr(value, leading_field.as_ref(), table),
            Expr::Alias { expr, .. } => eval_alias(self, expr, table),
            Expr::Variable { name } => eval_variable(
                self.variables,
                self.system_variables,
                name,
                table.row_count(),
            ),
            Expr::AtTimeZone {
                timestamp,
                time_zone,
            } => eval_at_time_zone(self, timestamp, time_zone, table),
            Expr::JsonAccess { expr, path } => eval_json_access(self, expr, path, table),
            _ => Err(Error::unsupported(format!(
                "ColumnarEvaluator does not support expression: {:?}",
                expr
            ))),
        }
    }

    fn eval_interval_expr(
        &self,
        value: &Expr,
        leading_field: Option<&yachtsql_ir::DateTimeField>,
        table: &Table,
    ) -> Result<Column> {
        let val_col = self.evaluate(value, table)?;
        let field = leading_field
            .copied()
            .unwrap_or(yachtsql_ir::DateTimeField::Second);
        let n = table.row_count();
        let mut results = Vec::with_capacity(n);
        for i in 0..n {
            let v = val_col.get_value(i);
            match v {
                Value::Int64(int_val) => {
                    let interval_col = eval_interval(int_val, field, 1)?;
                    results.push(interval_col.get_value(0));
                }
                Value::Null => results.push(Value::Null),
                _ => results.push(Value::Null),
            }
        }
        Ok(Column::from_values(&results))
    }

    fn eval_literal(&self, lit: &Literal, row_count: usize) -> Result<Column> {
        let value = match lit {
            Literal::Null => Value::Null,
            Literal::Bool(b) => Value::Bool(*b),
            Literal::Int64(i) => Value::Int64(*i),
            Literal::Float64(f) => Value::float64(f.0),
            Literal::Numeric(d) => Value::Numeric(*d),
            Literal::BigNumeric(d) => Value::BigNumeric(*d),
            Literal::String(s) => Value::String(s.clone()),
            Literal::Bytes(b) => Value::Bytes(b.clone()),
            Literal::Date(d) => {
                let date = chrono::NaiveDate::from_num_days_from_ce_opt(*d).ok_or_else(|| {
                    Error::InvalidQuery(format!("Invalid date literal: {} days from CE", d))
                })?;
                Value::Date(date)
            }
            Literal::Time(t) => {
                let nanos = *t;
                let secs = (nanos / 1_000_000_000) as u32;
                let nsecs = (nanos % 1_000_000_000) as u32;
                let time = chrono::NaiveTime::from_num_seconds_from_midnight_opt(secs, nsecs)
                    .ok_or_else(|| {
                        Error::InvalidQuery(format!("Invalid time literal: {} nanoseconds", t))
                    })?;
                Value::Time(time)
            }
            Literal::Timestamp(ts) => {
                let dt = chrono::DateTime::from_timestamp(
                    *ts / 1_000_000,
                    ((*ts % 1_000_000) * 1000) as u32,
                )
                .ok_or_else(|| {
                    Error::InvalidQuery(format!("Invalid timestamp literal: {} microseconds", ts))
                })?;
                Value::Timestamp(dt)
            }
            Literal::Datetime(dt) => {
                let ndt = chrono::DateTime::from_timestamp(
                    *dt / 1_000_000,
                    ((*dt % 1_000_000) * 1000) as u32,
                )
                .map(|d| d.naive_utc())
                .ok_or_else(|| {
                    Error::InvalidQuery(format!("Invalid datetime literal: {} microseconds", dt))
                })?;
                Value::DateTime(ndt)
            }
            Literal::Interval {
                months,
                days,
                nanos,
            } => Value::Interval(yachtsql_common::types::IntervalValue {
                months: *months,
                days: *days,
                nanos: *nanos,
            }),
            Literal::Array(elements) => {
                let vals: Vec<Value> = elements
                    .iter()
                    .map(|e| self.literal_to_value(e))
                    .collect::<Result<_>>()?;
                Value::Array(vals)
            }
            Literal::Struct(fields) => {
                let field_vals: Vec<(String, Value)> = fields
                    .iter()
                    .map(|(name, lit)| Ok((name.clone(), self.literal_to_value(lit)?)))
                    .collect::<Result<_>>()?;
                Value::Struct(field_vals)
            }
            Literal::Json(j) => Value::Json(j.clone()),
        };
        Ok(Column::broadcast(value, row_count))
    }

    fn literal_to_value(&self, lit: &Literal) -> Result<Value> {
        match lit {
            Literal::Null => Ok(Value::Null),
            Literal::Bool(b) => Ok(Value::Bool(*b)),
            Literal::Int64(i) => Ok(Value::Int64(*i)),
            Literal::Float64(f) => Ok(Value::float64(f.0)),
            Literal::Numeric(d) => Ok(Value::Numeric(*d)),
            Literal::BigNumeric(d) => Ok(Value::BigNumeric(*d)),
            Literal::String(s) => Ok(Value::String(s.clone())),
            Literal::Bytes(b) => Ok(Value::Bytes(b.clone())),
            Literal::Date(d) => {
                let date = chrono::NaiveDate::from_num_days_from_ce_opt(*d)
                    .unwrap_or_else(|| chrono::DateTime::UNIX_EPOCH.date_naive());
                Ok(Value::Date(date))
            }
            Literal::Time(t) => {
                let nanos = *t;
                let secs = (nanos / 1_000_000_000) as u32;
                let nsecs = (nanos % 1_000_000_000) as u32;
                let time = chrono::NaiveTime::from_num_seconds_from_midnight_opt(secs, nsecs)
                    .unwrap_or(chrono::NaiveTime::MIN);
                Ok(Value::Time(time))
            }
            Literal::Timestamp(ts) => {
                let dt = chrono::DateTime::from_timestamp(
                    *ts / 1_000_000,
                    ((*ts % 1_000_000) * 1000) as u32,
                )
                .unwrap_or(chrono::DateTime::UNIX_EPOCH);
                Ok(Value::Timestamp(dt))
            }
            Literal::Datetime(dt) => {
                let ndt = chrono::DateTime::from_timestamp(
                    *dt / 1_000_000,
                    ((*dt % 1_000_000) * 1000) as u32,
                )
                .map(|d| d.naive_utc())
                .unwrap_or(chrono::DateTime::UNIX_EPOCH.naive_utc());
                Ok(Value::DateTime(ndt))
            }
            Literal::Interval {
                months,
                days,
                nanos,
            } => Ok(Value::Interval(yachtsql_common::types::IntervalValue {
                months: *months,
                days: *days,
                nanos: *nanos,
            })),
            Literal::Array(elements) => {
                let vals: Vec<Value> = elements
                    .iter()
                    .map(|e| self.literal_to_value(e))
                    .collect::<Result<_>>()?;
                Ok(Value::Array(vals))
            }
            Literal::Struct(fields) => {
                let field_vals: Vec<(String, Value)> = fields
                    .iter()
                    .map(|(name, lit)| Ok((name.clone(), self.literal_to_value(lit)?)))
                    .collect::<Result<_>>()?;
                Ok(Value::Struct(field_vals))
            }
            Literal::Json(j) => Ok(Value::Json(j.clone())),
        }
    }

    fn eval_column_ref(
        &self,
        _table: Option<&str>,
        name: &str,
        index: Option<usize>,
        input: &Table,
    ) -> Result<Column> {
        if let Some(idx) = index
            && let Some(col) = input.column(idx)
        {
            return Ok(col.clone());
        }

        let upper_name = name.to_uppercase();
        for (col_name, col) in input.columns().iter() {
            if col_name.to_uppercase() == upper_name {
                return Ok(col.as_ref().clone());
            }
        }

        if let Some(vars) = self.variables
            && let Some(val) = vars.get(&upper_name)
        {
            return Ok(Column::broadcast(val.clone(), input.row_count()));
        }

        Err(Error::ColumnNotFound(name.to_string()))
    }

    fn eval_binary_op(
        &self,
        left: &Expr,
        op: BinaryOp,
        right: &Expr,
        table: &Table,
    ) -> Result<Column> {
        let left_col = self.evaluate(left, table)?;
        let right_col = self.evaluate(right, table)?;

        let (left_col, right_col) = self.coerce_columns_for_op(left_col, right_col, op)?;

        match op {
            BinaryOp::Add => left_col.binary_add(&right_col),
            BinaryOp::Sub => left_col.binary_sub(&right_col),
            BinaryOp::Mul => left_col.binary_mul(&right_col),
            BinaryOp::Div => left_col.binary_div(&right_col),
            BinaryOp::Mod => self.eval_binary_mod(&left_col, &right_col),
            BinaryOp::Eq => left_col.binary_eq(&right_col),
            BinaryOp::NotEq => left_col.binary_ne(&right_col),
            BinaryOp::Lt => left_col.binary_lt(&right_col),
            BinaryOp::LtEq => left_col.binary_le(&right_col),
            BinaryOp::Gt => left_col.binary_gt(&right_col),
            BinaryOp::GtEq => left_col.binary_ge(&right_col),
            BinaryOp::And => left_col.binary_and(&right_col),
            BinaryOp::Or => left_col.binary_or(&right_col),
            BinaryOp::Concat
            | BinaryOp::BitwiseAnd
            | BinaryOp::BitwiseOr
            | BinaryOp::BitwiseXor
            | BinaryOp::ShiftLeft
            | BinaryOp::ShiftRight => eval_binary_op_ext(self, left, op, right, table),
        }
    }

    fn coerce_columns_for_op(
        &self,
        left: Column,
        right: Column,
        _op: BinaryOp,
    ) -> Result<(Column, Column)> {
        use yachtsql_common::types::DataType;

        let left_type = left.data_type();
        let right_type = right.data_type();

        if left_type == right_type {
            return Ok((left, right));
        }

        match (&left_type, &right_type) {
            (DataType::Int64, DataType::Float64) => {
                let coerced_left = self.coerce_int_to_float(&left)?;
                Ok((coerced_left, right))
            }
            (DataType::Float64, DataType::Int64) => {
                let coerced_right = self.coerce_int_to_float(&right)?;
                Ok((left, coerced_right))
            }
            (DataType::Int64, DataType::Numeric(_)) => {
                let coerced_left = self.coerce_int_to_numeric(&left)?;
                Ok((coerced_left, right))
            }
            (DataType::Numeric(_), DataType::Int64) => {
                let coerced_right = self.coerce_int_to_numeric(&right)?;
                Ok((left, coerced_right))
            }
            (DataType::Numeric(_), DataType::Numeric(_)) => Ok((left, right)),
            _ => Ok((left, right)),
        }
    }

    fn coerce_int_to_numeric(&self, col: &Column) -> Result<Column> {
        match col {
            Column::Int64 { data, nulls } => {
                let mut new_data: Vec<rust_decimal::Decimal> = Vec::with_capacity(data.len());
                for &v in data.iter() {
                    new_data.push(rust_decimal::Decimal::from(v));
                }
                Ok(Column::Numeric {
                    data: new_data,
                    nulls: nulls.clone(),
                })
            }
            _ => {
                let mut values = Vec::with_capacity(col.len());
                for i in 0..col.len() {
                    let val = col.get_value(i);
                    match val {
                        Value::Int64(v) => {
                            values.push(Value::Numeric(rust_decimal::Decimal::from(v)));
                        }
                        Value::Null => values.push(Value::Null),
                        other => values.push(other),
                    }
                }
                Ok(Column::from_values(&values))
            }
        }
    }

    fn coerce_int_to_float(&self, col: &Column) -> Result<Column> {
        match col {
            Column::Int64 { data, nulls } => {
                let mut new_data = aligned_vec::AVec::new(64);
                for &v in data.iter() {
                    new_data.push(v as f64);
                }
                Ok(Column::Float64 {
                    data: new_data,
                    nulls: nulls.clone(),
                })
            }
            _ => {
                let mut values = Vec::with_capacity(col.len());
                for i in 0..col.len() {
                    let val = col.get_value(i);
                    match val {
                        Value::Int64(v) => values.push(Value::float64(v as f64)),
                        Value::Null => values.push(Value::Null),
                        other => values.push(other),
                    }
                }
                Ok(Column::from_values(&values))
            }
        }
    }

    fn eval_binary_mod(&self, left: &Column, right: &Column) -> Result<Column> {
        let len = left.len();
        let mut values = Vec::with_capacity(len);
        for i in 0..len {
            let l = left.get_value(i);
            let r = right.get_value(i);
            let result = match (l, r) {
                (Value::Int64(a), Value::Int64(b)) if b != 0 => Value::Int64(a % b),
                (Value::Float64(a), Value::Float64(b)) if b.0 != 0.0 => Value::float64(a.0 % b.0),
                _ => Value::Null,
            };
            values.push(result);
        }
        Ok(Column::from_values(&values))
    }

    fn eval_unary_op(&self, op: UnaryOp, expr: &Expr, table: &Table) -> Result<Column> {
        let col = self.evaluate(expr, table)?;
        match op {
            UnaryOp::Not => {
                if matches!(col, Column::Bool { .. }) {
                    col.unary_not()
                } else {
                    let n = col.len();
                    let mut results = Vec::with_capacity(n);
                    for i in 0..n {
                        let val = col.get_value(i);
                        match val {
                            Value::Null => results.push(Value::Null),
                            Value::Bool(b) => results.push(Value::Bool(!b)),
                            _ => {
                                return Err(Error::InvalidQuery(format!(
                                    "NOT requires boolean operand, got {:?}",
                                    val
                                )));
                            }
                        }
                    }
                    Ok(Column::from_values(&results))
                }
            }
            UnaryOp::Minus => col.unary_neg(),
            UnaryOp::Plus => Ok(col),
            UnaryOp::BitwiseNot => eval_unary_op_ext(self, op, expr, table),
        }
    }

    fn eval_is_null(&self, expr: &Expr, negated: bool, table: &Table) -> Result<Column> {
        let col = self.evaluate(expr, table)?;
        if negated {
            Ok(col.is_not_null_mask())
        } else {
            Ok(col.is_null_mask())
        }
    }

    fn eval_cast(
        &self,
        expr: &Expr,
        target_type: &yachtsql_common::types::DataType,
        safe: bool,
        table: &Table,
    ) -> Result<Column> {
        use crate::value_evaluator::cast_value;

        let col = self.evaluate(expr, table)?;
        let mut values = Vec::with_capacity(col.len());
        for i in 0..col.len() {
            let val = col.get_value(i);
            let casted = cast_value(val, target_type, safe)?;
            values.push(casted);
        }
        Ok(Column::from_values(&values))
    }

    fn eval_case(
        &self,
        operand: Option<&Expr>,
        when_clauses: &[yachtsql_ir::WhenClause],
        else_result: Option<&Expr>,
        table: &Table,
    ) -> Result<Column> {
        let row_count = table.row_count();
        let mut result_values = vec![Value::Null; row_count];
        let mut assigned = vec![false; row_count];

        let operand_col = operand.map(|e| self.evaluate(e, table)).transpose()?;

        for wc in when_clauses {
            let condition_col = self.evaluate(&wc.condition, table)?;
            let result_col = self.evaluate(&wc.result, table)?;

            let condition_col = match &operand_col {
                Some(op_col) => op_col.binary_eq(&condition_col)?,
                None => condition_col,
            };

            let Column::Bool {
                data: cond_data,
                nulls: cond_nulls,
            } = &condition_col
            else {
                return Err(Error::invalid_query("CASE condition must be boolean"));
            };

            for i in 0..row_count {
                if !assigned[i] && !cond_nulls.is_null(i) && cond_data[i] {
                    result_values[i] = result_col.get_value(i);
                    assigned[i] = true;
                }
            }
        }

        if let Some(else_expr) = else_result {
            let else_col = self.evaluate(else_expr, table)?;
            for i in 0..row_count {
                if !assigned[i] {
                    result_values[i] = else_col.get_value(i);
                }
            }
        }

        Ok(Column::from_values(&result_values))
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_common::types::DataType;
    use yachtsql_storage::{Field, FieldMode};

    use super::*;

    fn make_test_table() -> Table {
        let schema = Schema::from_fields(vec![
            Field::new("a".to_string(), DataType::Int64, FieldMode::Nullable),
            Field::new("b".to_string(), DataType::Int64, FieldMode::Nullable),
            Field::new("c".to_string(), DataType::String, FieldMode::Nullable),
        ]);
        let mut table = Table::new(schema);
        table
            .push_row(vec![
                Value::Int64(1),
                Value::Int64(10),
                Value::String("foo".into()),
            ])
            .unwrap();
        table
            .push_row(vec![
                Value::Int64(2),
                Value::Int64(20),
                Value::String("bar".into()),
            ])
            .unwrap();
        table
            .push_row(vec![
                Value::Int64(3),
                Value::Int64(30),
                Value::String("baz".into()),
            ])
            .unwrap();
        table
    }

    #[test]
    fn test_eval_literal() {
        let table = make_test_table();
        let eval = ColumnarEvaluator::new(table.schema());

        let expr = Expr::Literal(Literal::Int64(42));
        let result = eval.evaluate(&expr, &table).unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result.get_value(0), Value::Int64(42));
        assert_eq!(result.get_value(1), Value::Int64(42));
        assert_eq!(result.get_value(2), Value::Int64(42));
    }

    #[test]
    fn test_eval_column_ref() {
        let table = make_test_table();
        let eval = ColumnarEvaluator::new(table.schema());

        let expr = Expr::Column {
            table: None,
            name: "a".to_string(),
            index: Some(0),
        };
        let result = eval.evaluate(&expr, &table).unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result.get_value(0), Value::Int64(1));
        assert_eq!(result.get_value(1), Value::Int64(2));
        assert_eq!(result.get_value(2), Value::Int64(3));
    }

    #[test]
    fn test_eval_binary_add() {
        let table = make_test_table();
        let eval = ColumnarEvaluator::new(table.schema());

        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column {
                table: None,
                name: "a".to_string(),
                index: Some(0),
            }),
            op: BinaryOp::Add,
            right: Box::new(Expr::Column {
                table: None,
                name: "b".to_string(),
                index: Some(1),
            }),
        };
        let result = eval.evaluate(&expr, &table).unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result.get_value(0), Value::Int64(11));
        assert_eq!(result.get_value(1), Value::Int64(22));
        assert_eq!(result.get_value(2), Value::Int64(33));
    }

    #[test]
    fn test_eval_comparison() {
        let table = make_test_table();
        let eval = ColumnarEvaluator::new(table.schema());

        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column {
                table: None,
                name: "a".to_string(),
                index: Some(0),
            }),
            op: BinaryOp::Gt,
            right: Box::new(Expr::Literal(Literal::Int64(1))),
        };
        let result = eval.evaluate(&expr, &table).unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result.get_value(0), Value::Bool(false));
        assert_eq!(result.get_value(1), Value::Bool(true));
        assert_eq!(result.get_value(2), Value::Bool(true));
    }

    #[test]
    fn test_eval_is_null() {
        let schema = Schema::from_fields(vec![Field::new(
            "x".to_string(),
            DataType::Int64,
            FieldMode::Nullable,
        )]);
        let mut table = Table::new(schema);
        table.push_row(vec![Value::Int64(1)]).unwrap();
        table.push_row(vec![Value::Null]).unwrap();
        table.push_row(vec![Value::Int64(3)]).unwrap();

        let eval = ColumnarEvaluator::new(table.schema());

        let expr = Expr::IsNull {
            expr: Box::new(Expr::Column {
                table: None,
                name: "x".to_string(),
                index: Some(0),
            }),
            negated: false,
        };
        let result = eval.evaluate(&expr, &table).unwrap();

        assert_eq!(result.get_value(0), Value::Bool(false));
        assert_eq!(result.get_value(1), Value::Bool(true));
        assert_eq!(result.get_value(2), Value::Bool(false));
    }

    #[test]
    fn test_can_evaluate() {
        let table = make_test_table();
        let eval = ColumnarEvaluator::new(table.schema());

        assert!(eval.can_evaluate(&Expr::Literal(Literal::Int64(42))));
        assert!(eval.can_evaluate(&Expr::Column {
            table: None,
            name: "a".to_string(),
            index: None
        }));
        assert!(eval.can_evaluate(&Expr::BinaryOp {
            left: Box::new(Expr::Literal(Literal::Int64(1))),
            op: BinaryOp::Add,
            right: Box::new(Expr::Literal(Literal::Int64(2))),
        }));
        assert!(eval.can_evaluate(&Expr::ScalarFunction {
            name: yachtsql_ir::ScalarFunction::Upper,
            args: vec![Expr::Literal(Literal::String("test".into()))],
        }));
    }
}
