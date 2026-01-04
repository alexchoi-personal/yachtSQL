use std::borrow::Cow;
use std::cell::RefCell;
use std::num::NonZeroUsize;

use lru::LruCache;
use rustc_hash::FxHashMap;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::{DataType, Value};
use yachtsql_ir::{BinaryOp, Expr, FunctionBody, Literal, ScalarFunction, UnaryOp, WhenClause};

use crate::js_udf::evaluate_js_function;
use crate::py_udf::evaluate_py_function;

#[derive(Copy, Clone)]
enum ArrayAccessMode {
    Default,
    Offset,
    Ordinal,
    SafeOffset,
    SafeOrdinal,
}
use yachtsql_storage::{Record, Schema};

use crate::scalar_functions;

#[derive(Debug, Clone)]
pub struct UserFunctionDef {
    pub parameters: Vec<String>,
    pub body: FunctionBody,
}

pub struct ValueEvaluator<'a> {
    schema: &'a Schema,
    variables: Option<&'a FxHashMap<String, Value>>,
    system_variables: Option<&'a FxHashMap<String, Value>>,
    user_functions: Option<&'a FxHashMap<String, UserFunctionDef>>,
}

impl<'a> ValueEvaluator<'a> {
    pub fn new(schema: &'a Schema) -> Self {
        Self {
            schema,
            variables: None,
            system_variables: None,
            user_functions: None,
        }
    }

    pub fn with_variables(mut self, variables: &'a FxHashMap<String, Value>) -> Self {
        self.variables = Some(variables);
        self
    }

    pub fn with_system_variables(mut self, system_variables: &'a FxHashMap<String, Value>) -> Self {
        self.system_variables = Some(system_variables);
        self
    }

    pub fn with_user_functions(
        mut self,
        user_functions: &'a FxHashMap<String, UserFunctionDef>,
    ) -> Self {
        self.user_functions = Some(user_functions);
        self
    }

    fn get_collation_for_expr(&self, expr: &Expr) -> Option<String> {
        match expr {
            Expr::ScalarFunction { name, args } => {
                if let ScalarFunction::Custom(func_name) = name
                    && func_name.eq_ignore_ascii_case("COLLATE")
                    && args.len() == 2
                    && let Expr::Literal(Literal::String(collation)) = &args[1]
                {
                    return Some(collation.clone());
                }
                None
            }
            Expr::Column { name, .. } => self.schema.field(name).and_then(|f| f.collation.clone()),
            _ => None,
        }
    }

    pub fn evaluate(&self, expr: &Expr, record: &Record) -> Result<Value> {
        match expr {
            Expr::Literal(lit) => self.eval_literal(lit),
            Expr::Column { table, name, index } => {
                self.eval_column(table.as_deref(), name, *index, record)
            }
            Expr::BinaryOp { left, op, right } => self.eval_binary_op(left, *op, right, record),
            Expr::UnaryOp { op, expr } => self.eval_unary_op(*op, expr, record),
            Expr::ScalarFunction { name, args } => self.eval_scalar_function(name, args, record),
            Expr::IsNull { expr, negated } => {
                let val = self.evaluate(expr, record)?;
                let is_null = val.is_null();
                Ok(Value::Bool(if *negated { !is_null } else { is_null }))
            }
            Expr::Cast {
                expr,
                data_type,
                safe,
            } => {
                let val = self.evaluate(expr, record)?;
                cast_value(val, data_type, *safe)
            }
            Expr::Case {
                operand,
                when_clauses,
                else_result,
            } => self.eval_case(
                operand.as_deref(),
                when_clauses,
                else_result.as_deref(),
                record,
            ),
            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                let val = self.evaluate(expr, record)?;
                let low_val = self.evaluate(low, record)?;
                let high_val = self.evaluate(high, record)?;
                if val.is_null() || low_val.is_null() || high_val.is_null() {
                    return Ok(Value::Null);
                }
                let in_range = val >= low_val && val <= high_val;
                Ok(Value::Bool(if *negated { !in_range } else { in_range }))
            }
            Expr::Like {
                expr,
                pattern,
                negated,
                case_insensitive,
            } => {
                let val = self.evaluate(expr, record)?;
                let pat = self.evaluate(pattern, record)?;
                match (&val, &pat) {
                    (Value::String(s), Value::String(p)) => {
                        let matches = like_match(s, p, *case_insensitive)?;
                        Ok(Value::Bool(if *negated { !matches } else { matches }))
                    }
                    (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                    _ => Err(Error::invalid_query("LIKE requires string operands")),
                }
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let val = self.evaluate(expr, record)?;
                if val.is_null() {
                    return Ok(Value::Null);
                }
                let mut found = false;
                let mut has_null = false;
                for item in list {
                    let item_val = self.evaluate(item, record)?;
                    if item_val.is_null() {
                        has_null = true;
                    } else if val == item_val {
                        found = true;
                        break;
                    }
                }
                if found {
                    Ok(Value::Bool(!*negated))
                } else if has_null {
                    Ok(Value::Null)
                } else {
                    Ok(Value::Bool(*negated))
                }
            }
            Expr::InUnnest {
                expr,
                array_expr,
                negated,
            } => {
                let val = self.evaluate(expr, record)?;
                let arr = self.evaluate(array_expr, record)?;
                if val.is_null() {
                    return Ok(Value::Null);
                }
                match arr {
                    Value::Null => Ok(Value::Null),
                    Value::Array(elements) => {
                        let mut found = false;
                        let mut has_null = false;
                        for elem in &elements {
                            if elem.is_null() {
                                has_null = true;
                            } else if val == *elem {
                                found = true;
                                break;
                            }
                        }
                        if found {
                            Ok(Value::Bool(!*negated))
                        } else if has_null {
                            Ok(Value::Null)
                        } else {
                            Ok(Value::Bool(*negated))
                        }
                    }
                    _ => Ok(Value::Bool(*negated)),
                }
            }
            Expr::Extract { field, expr } => {
                let val = self.evaluate(expr, record)?;
                Ok(crate::columnar_evaluator::extract_field(&val, field))
            }
            Expr::Substring {
                expr,
                start,
                length,
            } => {
                let val = self.evaluate(expr, record)?;
                let start_val = start
                    .as_ref()
                    .map(|e| self.evaluate(e, record))
                    .transpose()?
                    .unwrap_or(Value::Int64(1));
                let len_val = length
                    .as_ref()
                    .map(|e| self.evaluate(e, record))
                    .transpose()?;
                let mut args = vec![val, start_val];
                if let Some(lv) = len_val {
                    args.push(lv);
                }
                scalar_functions::string::fn_substring(&args)
            }
            Expr::Array { elements, .. } => {
                let mut values = Vec::with_capacity(elements.len());
                for elem in elements {
                    values.push(self.evaluate(elem, record)?);
                }
                Ok(Value::Array(values))
            }
            Expr::Struct { fields } => {
                let mut struct_fields = Vec::with_capacity(fields.len());
                for (i, (name, expr)) in fields.iter().enumerate() {
                    let val = self.evaluate(expr, record)?;
                    let field_name = name.clone().unwrap_or_else(|| format!("_field{}", i));
                    struct_fields.push((field_name, val));
                }
                Ok(Value::Struct(struct_fields))
            }
            Expr::ArrayAccess { array, index } => {
                let arr = self.evaluate(array, record)?;

                let (idx, access_mode) = match index.as_ref() {
                    Expr::ScalarFunction { name, args } if args.len() == 1 => {
                        let idx = self.evaluate(&args[0], record)?;
                        let mode = match name {
                            ScalarFunction::ArrayOffset => ArrayAccessMode::Offset,
                            ScalarFunction::ArrayOrdinal => ArrayAccessMode::Ordinal,
                            ScalarFunction::SafeOffset => ArrayAccessMode::SafeOffset,
                            ScalarFunction::SafeOrdinal => ArrayAccessMode::SafeOrdinal,
                            _ => {
                                let idx = self.evaluate(index, record)?;
                                return self.eval_array_access_impl(
                                    &arr,
                                    &idx,
                                    ArrayAccessMode::Default,
                                );
                            }
                        };
                        (idx, mode)
                    }
                    _ => {
                        let idx = self.evaluate(index, record)?;
                        (idx, ArrayAccessMode::Default)
                    }
                };

                self.eval_array_access_impl(&arr, &idx, access_mode)
            }
            Expr::StructAccess { expr, field } => {
                let val = self.evaluate(expr, record)?;
                match val {
                    Value::Struct(fields) => {
                        for (name, v) in fields {
                            if name.eq_ignore_ascii_case(field) {
                                return Ok(v);
                            }
                        }
                        Ok(Value::Null)
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Ok(Value::Null),
                }
            }
            Expr::Alias { expr, .. } => self.evaluate(expr, record),
            Expr::TypedString { data_type, value } => {
                crate::columnar_evaluator::handlers::eval_typed_string(data_type, value, 1)
                    .map(|col| col.get_value(0))
            }
            Expr::Position { substr, string } => {
                let substr_val = self.evaluate(substr, record)?;
                let string_val = self.evaluate(string, record)?;
                match (&substr_val, &string_val) {
                    (Value::String(sub), Value::String(s)) => {
                        let pos = s.find(sub).map(|i| i as i64 + 1).unwrap_or(0);
                        Ok(Value::Int64(pos))
                    }
                    (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                    _ => Err(Error::invalid_query("POSITION requires string operands")),
                }
            }
            Expr::Overlay {
                expr,
                overlay_what,
                overlay_from,
                overlay_for,
            } => {
                let val = self.evaluate(expr, record)?;
                let what = self.evaluate(overlay_what, record)?;
                let from = self.evaluate(overlay_from, record)?;
                let for_len = overlay_for
                    .as_ref()
                    .map(|e| self.evaluate(e, record))
                    .transpose()?;

                match (&val, &what, &from) {
                    (Value::String(s), Value::String(replacement), Value::Int64(start)) => {
                        let start_idx = (*start as usize).saturating_sub(1);
                        let replace_len = match &for_len {
                            Some(Value::Int64(l)) => *l as usize,
                            _ => replacement.len(),
                        };
                        let mut result = s.clone();
                        if start_idx < s.len() {
                            let end_idx = (start_idx + replace_len).min(s.len());
                            result.replace_range(start_idx..end_idx, replacement);
                        }
                        Ok(Value::String(result))
                    }
                    (Value::Null, _, _) | (_, Value::Null, _) | (_, _, Value::Null) => {
                        Ok(Value::Null)
                    }
                    _ => Err(Error::invalid_query("OVERLAY requires string operands")),
                }
            }
            Expr::Trim {
                expr,
                trim_what,
                trim_where,
            } => {
                use yachtsql_ir::TrimWhere;
                let val = self.evaluate(expr, record)?;
                let trim_chars = trim_what
                    .as_ref()
                    .map(|e| self.evaluate(e, record))
                    .transpose()?;
                match val {
                    Value::String(s) => {
                        let chars_to_trim: Vec<char> = match &trim_chars {
                            Some(Value::String(c)) => c.chars().collect(),
                            _ => vec![' '],
                        };
                        let result = match trim_where {
                            TrimWhere::Both => s
                                .trim_start_matches(|c| chars_to_trim.contains(&c))
                                .trim_end_matches(|c| chars_to_trim.contains(&c))
                                .to_string(),
                            TrimWhere::Leading => s
                                .trim_start_matches(|c| chars_to_trim.contains(&c))
                                .to_string(),
                            TrimWhere::Trailing => s
                                .trim_end_matches(|c| chars_to_trim.contains(&c))
                                .to_string(),
                        };
                        Ok(Value::String(result))
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(Error::invalid_query("TRIM requires string operand")),
                }
            }
            Expr::Interval {
                value,
                leading_field,
            } => {
                let v = self.evaluate(value, record)?;
                match v {
                    Value::Int64(n) => {
                        let field = leading_field
                            .as_ref()
                            .copied()
                            .unwrap_or(yachtsql_ir::DateTimeField::Second);
                        let col = crate::columnar_evaluator::handlers::eval_interval(n, field, 1)?;
                        Ok(col.get_value(0))
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(Error::invalid_query("INTERVAL requires numeric value")),
                }
            }
            Expr::Variable { name } => {
                let upper = name.to_uppercase();
                if let Some(vars) = self.variables
                    && let Some(val) = vars.get(&upper)
                {
                    return Ok(val.clone());
                }
                if let Some(sys_vars) = self.system_variables
                    && let Some(val) = sys_vars.get(&upper)
                {
                    return Ok(val.clone());
                }
                Ok(Value::Null)
            }
            Expr::IsDistinctFrom {
                left,
                right,
                negated,
            } => {
                let l = self.evaluate(left, record)?;
                let r = self.evaluate(right, record)?;
                let distinct = match (&l, &r) {
                    (Value::Null, Value::Null) => false,
                    (Value::Null, _) | (_, Value::Null) => true,
                    _ => l != r,
                };
                Ok(Value::Bool(if *negated { !distinct } else { distinct }))
            }
            Expr::JsonAccess { expr, path } => {
                let val = self.evaluate(expr, record)?;
                let mut current = match val {
                    Value::Json(j) => j,
                    Value::Null => return Ok(Value::Null),
                    _ => return Ok(Value::Null),
                };
                for element in path {
                    current = match element {
                        yachtsql_ir::JsonPathElement::Key(key) => {
                            if let Some(obj) = current.as_object() {
                                obj.get(key).cloned().unwrap_or(serde_json::Value::Null)
                            } else {
                                return Ok(Value::Null);
                            }
                        }
                        yachtsql_ir::JsonPathElement::Index(idx) => {
                            if let Some(arr) = current.as_array() {
                                let idx = *idx as usize;
                                arr.get(idx).cloned().unwrap_or(serde_json::Value::Null)
                            } else {
                                return Ok(Value::Null);
                            }
                        }
                    };
                }
                Ok(Value::Json(current))
            }
            _ => Err(Error::unsupported(format!(
                "Expression type {:?} not yet supported in ValueEvaluator",
                std::mem::discriminant(expr)
            ))),
        }
    }

    fn eval_literal(&self, lit: &Literal) -> Result<Value> {
        Ok(match lit {
            Literal::Null => Value::Null,
            Literal::Bool(b) => Value::Bool(*b),
            Literal::Int64(n) => Value::Int64(*n),
            Literal::Float64(f) => Value::Float64(*f),
            Literal::String(s) => Value::String(s.clone()),
            Literal::Bytes(b) => Value::Bytes(b.clone()),
            Literal::Numeric(n) => Value::Numeric(*n),
            Literal::BigNumeric(n) => Value::BigNumeric(*n),
            Literal::Date(d) => {
                let epoch = chrono::DateTime::UNIX_EPOCH.date_naive();
                Value::Date(epoch + chrono::Duration::days(*d as i64))
            }
            Literal::Time(t) => {
                let secs = *t / 1_000_000_000;
                let nanos = (*t % 1_000_000_000) as u32;
                Value::Time(
                    chrono::NaiveTime::from_num_seconds_from_midnight_opt(secs as u32, nanos)
                        .unwrap_or_default(),
                )
            }
            Literal::Datetime(dt) => {
                let secs = *dt / 1_000_000;
                let micros = (*dt % 1_000_000) as u32;
                Value::DateTime(
                    chrono::DateTime::from_timestamp(secs, micros * 1000)
                        .map(|d| d.naive_utc())
                        .unwrap_or_default(),
                )
            }
            Literal::Timestamp(ts) => {
                let secs = *ts / 1_000_000;
                let micros = (*ts % 1_000_000) as u32;
                Value::Timestamp(
                    chrono::DateTime::from_timestamp(secs, micros * 1000)
                        .unwrap_or_default()
                        .with_timezone(&chrono::Utc),
                )
            }
            Literal::Interval {
                months,
                days,
                nanos,
            } => Value::Interval(yachtsql_common::types::Interval {
                months: *months,
                days: *days,
                nanos: *nanos,
            }),
            Literal::Array(items) => {
                let values: Result<Vec<Value>> =
                    items.iter().map(|i| self.eval_literal(i)).collect();
                Value::Array(values?)
            }
            Literal::Struct(fields) => {
                let values: Result<Vec<(String, Value)>> = fields
                    .iter()
                    .map(|(name, lit)| Ok((name.clone(), self.eval_literal(lit)?)))
                    .collect();
                Value::Struct(values?)
            }
            Literal::Json(j) => Value::Json(j.clone()),
        })
    }

    fn eval_column(
        &self,
        table: Option<&str>,
        name: &str,
        index: Option<usize>,
        record: &Record,
    ) -> Result<Value> {
        if let Some(idx) = index
            && idx < record.len()
        {
            return Ok(record.get(idx).cloned().unwrap_or(Value::Null));
        }

        for (i, field) in self.schema.fields().iter().enumerate() {
            if field.name.eq_ignore_ascii_case(name) {
                return Ok(record.get(i).cloned().unwrap_or(Value::Null));
            }
        }

        let upper_name = name.to_uppercase();
        if let Some(vars) = self.variables
            && let Some(val) = vars.get(&upper_name)
        {
            return Ok(val.clone());
        }

        if let Some(tbl) = table
            && let Some(vars) = self.variables
        {
            let tbl_upper = tbl.to_uppercase();
            if let Some(val) = vars.get(&tbl_upper)
                && let Value::Struct(fields) = val
            {
                for (field_name, field_val) in fields {
                    if field_name.eq_ignore_ascii_case(name) {
                        return Ok(field_val.clone());
                    }
                }
            }
        }

        Err(Error::ColumnNotFound(name.to_string()))
    }

    fn eval_binary_op(
        &self,
        left: &Expr,
        op: BinaryOp,
        right: &Expr,
        record: &Record,
    ) -> Result<Value> {
        let left_val = self.evaluate(left, record)?;
        let right_val = self.evaluate(right, record)?;

        let collation = self
            .get_collation_for_expr(left)
            .or_else(|| self.get_collation_for_expr(right));

        match op {
            BinaryOp::And => match (&left_val, &right_val) {
                (Value::Bool(false), _) | (_, Value::Bool(false)) => Ok(Value::Bool(false)),
                (Value::Bool(true), Value::Bool(true)) => Ok(Value::Bool(true)),
                (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                _ => Err(Error::invalid_query("AND requires boolean operands")),
            },
            BinaryOp::Or => match (&left_val, &right_val) {
                (Value::Bool(true), _) | (_, Value::Bool(true)) => Ok(Value::Bool(true)),
                (Value::Bool(false), Value::Bool(false)) => Ok(Value::Bool(false)),
                (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                _ => Err(Error::invalid_query("OR requires boolean operands")),
            },
            BinaryOp::Eq => {
                Ok(self.values_eq_with_collation(&left_val, &right_val, collation.as_deref()))
            }
            BinaryOp::NotEq => {
                match self.values_eq_with_collation(&left_val, &right_val, collation.as_deref()) {
                    Value::Bool(b) => Ok(Value::Bool(!b)),
                    other => Ok(other),
                }
            }
            BinaryOp::Lt => {
                Ok(self.values_lt_with_collation(&left_val, &right_val, collation.as_deref()))
            }
            BinaryOp::LtEq => {
                let lt = self.values_lt_with_collation(&left_val, &right_val, collation.as_deref());
                let eq = self.values_eq_with_collation(&left_val, &right_val, collation.as_deref());
                match (&lt, &eq) {
                    (Value::Bool(true), _) | (_, Value::Bool(true)) => Ok(Value::Bool(true)),
                    (Value::Bool(false), Value::Bool(false)) => Ok(Value::Bool(false)),
                    _ => Ok(Value::Null),
                }
            }
            BinaryOp::Gt => {
                Ok(self.values_lt_with_collation(&right_val, &left_val, collation.as_deref()))
            }
            BinaryOp::GtEq => {
                let gt = self.values_lt_with_collation(&right_val, &left_val, collation.as_deref());
                let eq = self.values_eq_with_collation(&left_val, &right_val, collation.as_deref());
                match (&gt, &eq) {
                    (Value::Bool(true), _) | (_, Value::Bool(true)) => Ok(Value::Bool(true)),
                    (Value::Bool(false), Value::Bool(false)) => Ok(Value::Bool(false)),
                    _ => Ok(Value::Null),
                }
            }
            BinaryOp::Add => scalar_functions::binary_ops::add_values(&left_val, &right_val),
            BinaryOp::Sub => scalar_functions::binary_ops::sub_values(&left_val, &right_val),
            BinaryOp::Mul => scalar_functions::binary_ops::mul_values(&left_val, &right_val),
            BinaryOp::Div => scalar_functions::binary_ops::div_values(&left_val, &right_val),
            BinaryOp::Mod => scalar_functions::binary_ops::mod_values(&left_val, &right_val),
            BinaryOp::Concat => match (&left_val, &right_val) {
                (Value::String(a), Value::String(b)) => Ok(Value::String(format!("{}{}", a, b))),
                (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                _ => Err(Error::invalid_query("CONCAT requires string operands")),
            },
            BinaryOp::BitwiseAnd => match (&left_val, &right_val) {
                (Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64(a & b)),
                (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                _ => Err(Error::invalid_query(
                    "Bitwise AND requires integer operands",
                )),
            },
            BinaryOp::BitwiseOr => match (&left_val, &right_val) {
                (Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64(a | b)),
                (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                _ => Err(Error::invalid_query("Bitwise OR requires integer operands")),
            },
            BinaryOp::BitwiseXor => match (&left_val, &right_val) {
                (Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64(a ^ b)),
                (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                _ => Err(Error::invalid_query(
                    "Bitwise XOR requires integer operands",
                )),
            },
            BinaryOp::ShiftLeft => match (&left_val, &right_val) {
                (Value::Int64(a), Value::Int64(b)) => {
                    if *b < 0 || *b >= 64 {
                        Ok(Value::Int64(0))
                    } else {
                        Ok(Value::Int64(a << b))
                    }
                }
                (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                _ => Err(Error::invalid_query("Shift left requires integer operands")),
            },
            BinaryOp::ShiftRight => match (&left_val, &right_val) {
                (Value::Int64(a), Value::Int64(b)) => {
                    if *b < 0 || *b >= 64 {
                        Ok(Value::Int64(if *a < 0 { -1 } else { 0 }))
                    } else {
                        Ok(Value::Int64(a >> b))
                    }
                }
                (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                _ => Err(Error::invalid_query(
                    "Shift right requires integer operands",
                )),
            },
        }
    }

    fn eval_unary_op(&self, op: UnaryOp, expr: &Expr, record: &Record) -> Result<Value> {
        let val = self.evaluate(expr, record)?;
        match op {
            UnaryOp::Not => match val {
                Value::Null => Ok(Value::Null),
                Value::Bool(b) => Ok(Value::Bool(!b)),
                _ => Err(Error::invalid_query("NOT requires boolean operand")),
            },
            UnaryOp::Minus => match val {
                Value::Null => Ok(Value::Null),
                Value::Int64(n) => Ok(Value::Int64(-n)),
                Value::Float64(f) => Ok(Value::Float64(ordered_float::OrderedFloat(-f.0))),
                Value::Numeric(d) => Ok(Value::Numeric(-d)),
                _ => Err(Error::invalid_query("Unary minus requires numeric operand")),
            },
            UnaryOp::Plus => Ok(val),
            UnaryOp::BitwiseNot => match val {
                Value::Null => Ok(Value::Null),
                Value::Int64(n) => Ok(Value::Int64(!n)),
                _ => Err(Error::invalid_query("Bitwise NOT requires integer operand")),
            },
        }
    }

    fn eval_scalar_function(
        &self,
        func: &ScalarFunction,
        args: &[Expr],
        record: &Record,
    ) -> Result<Value> {
        let arg_values: Vec<Value> = args
            .iter()
            .map(|a| self.evaluate(a, record))
            .collect::<Result<_>>()?;

        if let ScalarFunction::Custom(name) = func {
            return self.eval_custom_function(name, &arg_values);
        }

        scalar_functions::dispatch(func, &arg_values)
    }

    fn eval_custom_function(&self, name: &str, args: &[Value]) -> Result<Value> {
        if let Some(result) = self.try_eval_user_function(name, args)? {
            return Ok(result);
        }

        let upper = name.to_uppercase();
        match upper.as_str() {
            "COALESCE" => {
                for arg in args {
                    if !arg.is_null() {
                        return Ok(arg.clone());
                    }
                }
                Ok(Value::Null)
            }
            "IFNULL" => {
                if args.len() >= 2 {
                    if args[0].is_null() {
                        Ok(args[1].clone())
                    } else {
                        Ok(args[0].clone())
                    }
                } else {
                    Ok(Value::Null)
                }
            }
            "NULLIF" => {
                if args.len() >= 2 && args[0] == args[1] {
                    Ok(Value::Null)
                } else if !args.is_empty() {
                    Ok(args[0].clone())
                } else {
                    Ok(Value::Null)
                }
            }
            "RANGE" => scalar_functions::range::fn_range(args),
            "RANGE_CONTAINS" => scalar_functions::range::fn_range_contains(args),
            "RANGE_START" => scalar_functions::range::fn_range_start(args),
            "RANGE_END" => scalar_functions::range::fn_range_end(args),
            "RANGE_OVERLAPS" => scalar_functions::range::fn_range_overlaps(args),
            "RANGE_INTERSECT" => scalar_functions::range::fn_range_intersect(args),
            "RANGE_BUCKET" => scalar_functions::range::fn_range_bucket(args),
            "RANGE_IS_EMPTY" => scalar_functions::range::fn_range_is_empty(args),
            "GENERATE_RANGE_ARRAY" => scalar_functions::range::fn_generate_range_array(args),

            "JSON_EXTRACT_STRING_ARRAY" => {
                scalar_functions::json::fn_json_extract_string_array(args)
            }
            "JSON_QUERY_ARRAY" => scalar_functions::json::fn_json_query_array(args),
            "JSON_VALUE_ARRAY" => scalar_functions::json::fn_json_value_array(args),
            "JSON_KEYS" => scalar_functions::json::fn_json_keys(args),
            "JSON_TYPE" => scalar_functions::json::fn_json_type(args),
            "JSON_ARRAY" => scalar_functions::json::fn_json_array(args),
            "JSON_OBJECT" => scalar_functions::json::fn_json_object(args),
            "JSON_SET" => scalar_functions::json::fn_json_set(args),
            "JSON_REMOVE" => scalar_functions::json::fn_json_remove(args),
            "JSON_STRIP_NULLS" => scalar_functions::json::fn_json_strip_nulls(args),

            "LAX_BOOL" => scalar_functions::lax::fn_lax_bool(args),
            "LAX_INT64" => scalar_functions::lax::fn_lax_int64(args),
            "LAX_FLOAT64" => scalar_functions::lax::fn_lax_float64(args),
            "LAX_STRING" => scalar_functions::lax::fn_lax_string(args),

            "STRING" => scalar_functions::from_json::fn_string_from_json(args),

            "ARRAY_SLICE" => scalar_functions::array::fn_array_slice(args),
            "SAFE_OFFSET" => scalar_functions::array::fn_safe_offset(args),
            "SAFE_ORDINAL" => scalar_functions::array::fn_safe_ordinal(args),
            "ARRAY_FIRST" => scalar_functions::array::fn_array_first(args),
            "ARRAY_LAST" => scalar_functions::array::fn_array_last(args),
            "ARRAYENUMERATE" => scalar_functions::array::fn_array_enumerate(args),
            "MAP" => scalar_functions::map::fn_map(args),
            "MAP_KEYS" | "MAPKEYS" => scalar_functions::map::fn_map_keys(args),
            "MAP_VALUES" | "MAPVALUES" => scalar_functions::map::fn_map_values(args),
            "REGEXP_INSTR" => scalar_functions::string::fn_regexp_instr(args),
            "REGEXP_SUBSTR" => scalar_functions::string::fn_regexp_substr(args),

            "NULLIFZERO" => match args.first() {
                Some(Value::Int64(0)) => Ok(Value::Null),
                Some(Value::Float64(f)) if f.0 == 0.0 => Ok(Value::Null),
                Some(v) => Ok(v.clone()),
                None => Ok(Value::Null),
            },

            "CURRENT_USER" | "SESSION_USER" => Ok(Value::String("user".to_string())),

            "DATETIME_ADD" => scalar_functions::datetime::fn_date_add(args),
            "DATETIME_SUB" => scalar_functions::datetime::fn_date_sub(args),
            "DATETIME_DIFF" => scalar_functions::datetime::fn_date_diff(args),
            "TIMESTAMP_ADD" => scalar_functions::datetime::fn_date_add(args),
            "TIMESTAMP_SUB" => scalar_functions::datetime::fn_date_sub(args),
            "TIMESTAMP_DIFF" => scalar_functions::datetime::fn_date_diff(args),
            "TIME_ADD" => scalar_functions::datetime::fn_time_add(args),
            "TIME_SUB" => scalar_functions::datetime::fn_time_sub(args),
            "TIME_DIFF" => scalar_functions::datetime::fn_time_diff(args),

            "COLLATE" => {
                if args.len() != 2 {
                    return Err(Error::invalid_query("COLLATE requires exactly 2 arguments"));
                }
                Ok(args[0].clone())
            }

            "HLL_COUNT.EXTRACT" | "HLL_COUNT_EXTRACT" => {
                if args.is_empty() {
                    return Ok(Value::Null);
                }
                match &args[0] {
                    Value::Null => Ok(Value::Null),
                    Value::String(sketch) => {
                        if let Some(n_part) = sketch.split(':').find(|s| s.starts_with('n'))
                            && let Ok(count) = n_part[1..].parse::<i64>()
                        {
                            return Ok(Value::Int64(count));
                        }
                        Ok(Value::Int64(0))
                    }
                    _ => Ok(Value::Null),
                }
            }

            "KEYS.NEW_KEYSET" => scalar_functions::crypto::fn_keys_new_keyset(args),
            "AEAD.ENCRYPT" => scalar_functions::crypto::fn_aead_encrypt(args),
            "AEAD.DECRYPT_BYTES" => scalar_functions::crypto::fn_aead_decrypt_bytes(args),
            "AEAD.DECRYPT_STRING" => scalar_functions::crypto::fn_aead_decrypt_string(args),

            _ if upper.starts_with("ST_") => self.eval_geo_function(&upper, args),
            _ if upper.starts_with("NET.") => self.eval_net_function(&upper, args),
            _ => {
                if let Some(result) = self.try_eval_user_function(name, args)? {
                    return Ok(result);
                }
                Err(Error::unsupported(format!(
                    "Custom function '{}' not implemented in ValueEvaluator",
                    name
                )))
            }
        }
    }

    fn eval_geo_function(&self, name: &str, args: &[Value]) -> Result<Value> {
        match name {
            "ST_GEOGFROMTEXT" | "ST_GEOGRAPHYFROMTEXT" => {
                scalar_functions::geo::fn_st_geogfromtext(args)
            }
            "ST_GEOGPOINT" | "ST_GEOGRAPHYPOINT" => scalar_functions::geo::fn_st_geogpoint(args),
            "ST_ASTEXT" => scalar_functions::geo::fn_st_astext(args),
            "ST_DISTANCE" => scalar_functions::geo::fn_st_distance(args),
            "ST_AREA" => scalar_functions::geo::fn_st_area(args),
            "ST_LENGTH" => scalar_functions::geo::fn_st_length(args),
            "ST_PERIMETER" => scalar_functions::geo::fn_st_perimeter(args),
            "ST_CONTAINS" => scalar_functions::geo::fn_st_contains(args),
            "ST_INTERSECTS" => scalar_functions::geo::fn_st_intersects(args),
            "ST_WITHIN" => scalar_functions::geo::fn_st_within(args),
            "ST_X" => scalar_functions::geo::fn_st_x(args),
            "ST_Y" => scalar_functions::geo::fn_st_y(args),
            "ST_CENTROID" => scalar_functions::geo::fn_st_centroid(args),
            "ST_BUFFER" => scalar_functions::geo::fn_st_buffer(args),
            "ST_UNION" => scalar_functions::geo::fn_st_union(args),
            "ST_INTERSECTION" => scalar_functions::geo::fn_st_intersection(args),
            "ST_DIFFERENCE" => scalar_functions::geo::fn_st_difference(args),
            "ST_GEOGFROMGEOJSON" => scalar_functions::geo::fn_st_geogfromgeojson(args),
            "ST_ASGEOJSON" => scalar_functions::geo::fn_st_asgeojson(args),
            "ST_ASBINARY" => scalar_functions::geo::fn_st_asbinary(args),
            "ST_GEOGFROMWKB" => scalar_functions::geo::fn_st_geogfromwkb(args),
            "ST_GEOHASH" => scalar_functions::geo::fn_st_geohash(args),
            "ST_GEOGPOINTFROMGEOHASH" => scalar_functions::geo::fn_st_geogpointfromgeohash(args),
            "ST_MAKELINE" => scalar_functions::geo::fn_st_makeline(args),
            "ST_MAKEPOLYGON" => scalar_functions::geo::fn_st_makepolygon(args),
            "ST_TOUCHES" => scalar_functions::geo::fn_st_touches(args),
            "ST_DISJOINT" => scalar_functions::geo::fn_st_disjoint(args),
            "ST_EQUALS" => scalar_functions::geo::fn_st_equals(args),
            "ST_COVERS" => scalar_functions::geo::fn_st_covers(args),
            "ST_COVEREDBY" => scalar_functions::geo::fn_st_coveredby(args),
            "ST_DWITHIN" => scalar_functions::geo::fn_st_dwithin(args),
            "ST_CLOSESTPOINT" => scalar_functions::geo::fn_st_closestpoint(args),
            "ST_BOUNDINGBOX" => scalar_functions::geo::fn_st_boundingbox(args),
            "ST_CONVEXHULL" => scalar_functions::geo::fn_st_convexhull(args),
            "ST_SIMPLIFY" => scalar_functions::geo::fn_st_simplify(args),
            "ST_SNAPTOGRID" => scalar_functions::geo::fn_st_snaptogrid(args),
            "ST_NUMPOINTS" => scalar_functions::geo::fn_st_numpoints(args),
            "ST_STARTPOINT" => scalar_functions::geo::fn_st_startpoint(args),
            "ST_ENDPOINT" => scalar_functions::geo::fn_st_endpoint(args),
            "ST_POINTN" => scalar_functions::geo::fn_st_pointn(args),
            "ST_ISCLOSED" => scalar_functions::geo::fn_st_isclosed(args),
            "ST_ISRING" => scalar_functions::geo::fn_st_isring(args),
            "ST_ISEMPTY" => scalar_functions::geo::fn_st_isempty(args),
            "ST_DIMENSION" => scalar_functions::geo::fn_st_dimension(args),
            "ST_GEOMETRYTYPE" => scalar_functions::geo::fn_st_geometrytype(args),
            "ST_MAXDISTANCE" => scalar_functions::geo::fn_st_maxdistance(args),
            "ST_ISCOLLECTION" => scalar_functions::geo::fn_st_iscollection(args),
            "ST_BOUNDARY" => scalar_functions::geo::fn_st_boundary(args),
            "ST_BUFFERWITHTOLERANCE" => scalar_functions::geo::fn_st_bufferwithtolerance(args),
            _ => Err(Error::unsupported(format!(
                "Geography function '{}' not implemented",
                name
            ))),
        }
    }

    fn eval_net_function(&self, name: &str, args: &[Value]) -> Result<Value> {
        match name {
            "NET.IP_FROM_STRING" => scalar_functions::net::fn_net_ip_from_string(args),
            "NET.SAFE_IP_FROM_STRING" => scalar_functions::net::fn_net_safe_ip_from_string(args),
            "NET.IP_TO_STRING" => scalar_functions::net::fn_net_ip_to_string(args),
            "NET.HOST" => scalar_functions::net::fn_net_host(args),
            "NET.PUBLIC_SUFFIX" => scalar_functions::net::fn_net_public_suffix(args),
            "NET.REG_DOMAIN" => scalar_functions::net::fn_net_reg_domain(args),
            "NET.IP_NET_MASK" => scalar_functions::net::fn_net_ip_net_mask(args),
            "NET.IP_TRUNC" => scalar_functions::net::fn_net_ip_trunc(args),
            "NET.IP_IN_NET" => scalar_functions::net::fn_net_ip_in_net(args),
            "NET.MAKE_NET" => scalar_functions::net::fn_net_make_net(args),
            "NET.IP_IS_PRIVATE" => scalar_functions::net::fn_net_ip_is_private(args),
            "NET.IPV4_FROM_INT64" => scalar_functions::net::fn_net_ipv4_from_int64(args),
            "NET.IPV4_TO_INT64" => scalar_functions::net::fn_net_ipv4_to_int64(args),
            _ => Err(Error::unsupported(format!(
                "Net function '{}' not implemented",
                name
            ))),
        }
    }

    fn try_eval_user_function(&self, name: &str, args: &[Value]) -> Result<Option<Value>> {
        if let Some(funcs) = self.user_functions {
            let upper = name.to_uppercase();
            if let Some(func_def) = funcs.get(&upper) {
                match &func_def.body {
                    FunctionBody::Sql(expr) => {
                        let mut local_vars = FxHashMap::default();
                        for (i, param) in func_def.parameters.iter().enumerate() {
                            let val = args.get(i).cloned().unwrap_or(Value::Null);
                            local_vars.insert(param.to_uppercase(), val);
                        }
                        let func_evaluator = ValueEvaluator::new(self.schema)
                            .with_variables(&local_vars)
                            .with_user_functions(funcs);
                        let empty_record = Record::new();
                        return Ok(Some(func_evaluator.evaluate(expr.as_ref(), &empty_record)?));
                    }
                    FunctionBody::JavaScript(code) => {
                        let result = evaluate_js_function(code, &func_def.parameters, args)
                            .map_err(Error::Internal)?;
                        return Ok(Some(result));
                    }
                    FunctionBody::Language { name: lang, code } => {
                        let lang_upper = lang.to_uppercase();
                        if lang_upper == "JS" || lang_upper == "JAVASCRIPT" {
                            let result = evaluate_js_function(code, &func_def.parameters, args)
                                .map_err(Error::Internal)?;
                            return Ok(Some(result));
                        }
                        if lang_upper == "PYTHON" || lang_upper == "PY" {
                            let result = evaluate_py_function(code, &func_def.parameters, args)
                                .map_err(Error::Internal)?;
                            return Ok(Some(result));
                        }
                        return Err(Error::unsupported(format!(
                            "Language '{}' not supported for function: {}",
                            lang, name
                        )));
                    }
                    FunctionBody::SqlQuery(_) => {
                        return Err(Error::unsupported(format!(
                            "SQL query function body not yet supported: {}",
                            name
                        )));
                    }
                }
            }
        }
        Ok(None)
    }

    pub fn user_functions(&self) -> Option<&FxHashMap<String, UserFunctionDef>> {
        self.user_functions
    }

    pub fn eval_scalar_function_with_values(
        &self,
        func: &ScalarFunction,
        arg_values: &[Value],
    ) -> Result<Value> {
        if let ScalarFunction::Custom(name) = func {
            return self.eval_custom_function(name, arg_values);
        }
        scalar_functions::dispatch(func, arg_values)
    }

    pub fn eval_binary_op_with_values(
        &self,
        op: BinaryOp,
        left: Value,
        right: Value,
    ) -> Result<Value> {
        match op {
            BinaryOp::And => match (&left, &right) {
                (Value::Bool(false), _) | (_, Value::Bool(false)) => Ok(Value::Bool(false)),
                (Value::Bool(true), Value::Bool(true)) => Ok(Value::Bool(true)),
                (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                _ => Err(Error::invalid_query("AND requires boolean operands")),
            },
            BinaryOp::Or => match (&left, &right) {
                (Value::Bool(true), _) | (_, Value::Bool(true)) => Ok(Value::Bool(true)),
                (Value::Bool(false), Value::Bool(false)) => Ok(Value::Bool(false)),
                (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                _ => Err(Error::invalid_query("OR requires boolean operands")),
            },
            BinaryOp::Eq => Ok(self.values_eq(&left, &right)),
            BinaryOp::NotEq => match self.values_eq(&left, &right) {
                Value::Bool(b) => Ok(Value::Bool(!b)),
                other => Ok(other),
            },
            BinaryOp::Lt => Ok(self.values_lt(&left, &right)),
            BinaryOp::LtEq => {
                let lt = self.values_lt(&left, &right);
                let eq = self.values_eq(&left, &right);
                match (&lt, &eq) {
                    (Value::Bool(true), _) | (_, Value::Bool(true)) => Ok(Value::Bool(true)),
                    (Value::Bool(false), Value::Bool(false)) => Ok(Value::Bool(false)),
                    _ => Ok(Value::Null),
                }
            }
            BinaryOp::Gt => Ok(self.values_lt(&right, &left)),
            BinaryOp::GtEq => {
                let gt = self.values_lt(&right, &left);
                let eq = self.values_eq(&left, &right);
                match (&gt, &eq) {
                    (Value::Bool(true), _) | (_, Value::Bool(true)) => Ok(Value::Bool(true)),
                    (Value::Bool(false), Value::Bool(false)) => Ok(Value::Bool(false)),
                    _ => Ok(Value::Null),
                }
            }
            BinaryOp::Add => scalar_functions::binary_ops::add_values(&left, &right),
            BinaryOp::Sub => scalar_functions::binary_ops::sub_values(&left, &right),
            BinaryOp::Mul => scalar_functions::binary_ops::mul_values(&left, &right),
            BinaryOp::Div => scalar_functions::binary_ops::div_values(&left, &right),
            BinaryOp::Mod => scalar_functions::binary_ops::mod_values(&left, &right),
            BinaryOp::Concat => match (&left, &right) {
                (Value::String(a), Value::String(b)) => Ok(Value::String(format!("{}{}", a, b))),
                (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                _ => Err(Error::invalid_query("CONCAT requires string operands")),
            },
            BinaryOp::BitwiseAnd => match (&left, &right) {
                (Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64(a & b)),
                (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                _ => Err(Error::invalid_query(
                    "Bitwise AND requires integer operands",
                )),
            },
            BinaryOp::BitwiseOr => match (&left, &right) {
                (Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64(a | b)),
                (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                _ => Err(Error::invalid_query("Bitwise OR requires integer operands")),
            },
            BinaryOp::BitwiseXor => match (&left, &right) {
                (Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64(a ^ b)),
                (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                _ => Err(Error::invalid_query(
                    "Bitwise XOR requires integer operands",
                )),
            },
            BinaryOp::ShiftLeft => match (&left, &right) {
                (Value::Int64(a), Value::Int64(b)) => {
                    if *b < 0 || *b >= 64 {
                        Ok(Value::Int64(0))
                    } else {
                        Ok(Value::Int64(a << b))
                    }
                }
                (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                _ => Err(Error::invalid_query("Shift left requires integer operands")),
            },
            BinaryOp::ShiftRight => match (&left, &right) {
                (Value::Int64(a), Value::Int64(b)) => {
                    if *b < 0 || *b >= 64 {
                        Ok(Value::Int64(if *a < 0 { -1 } else { 0 }))
                    } else {
                        Ok(Value::Int64(a >> b))
                    }
                }
                (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                _ => Err(Error::invalid_query(
                    "Shift right requires integer operands",
                )),
            },
        }
    }

    fn eval_case(
        &self,
        operand: Option<&Expr>,
        when_clauses: &[WhenClause],
        else_result: Option<&Expr>,
        record: &Record,
    ) -> Result<Value> {
        match operand {
            Some(op_expr) => {
                let op_val = self.evaluate(op_expr, record)?;
                for clause in when_clauses {
                    let when_val = self.evaluate(&clause.condition, record)?;
                    if op_val == when_val {
                        return self.evaluate(&clause.result, record);
                    }
                }
            }
            None => {
                for clause in when_clauses {
                    let cond_val = self.evaluate(&clause.condition, record)?;
                    if cond_val.as_bool().unwrap_or(false) {
                        return self.evaluate(&clause.result, record);
                    }
                }
            }
        }

        match else_result {
            Some(e) => self.evaluate(e, record),
            None => Ok(Value::Null),
        }
    }

    fn values_eq(&self, a: &Value, b: &Value) -> Value {
        scalar_functions::comparison::eq_values(a, b).unwrap_or(Value::Null)
    }

    fn values_eq_with_collation(&self, a: &Value, b: &Value, collation: Option<&str>) -> Value {
        scalar_functions::comparison::eq_values_with_collation(a, b, collation)
            .unwrap_or(Value::Null)
    }

    fn values_lt(&self, a: &Value, b: &Value) -> Value {
        scalar_functions::comparison::compare_values(a, b, |ord| ord == std::cmp::Ordering::Less)
            .unwrap_or(Value::Null)
    }

    fn values_lt_with_collation(&self, a: &Value, b: &Value, collation: Option<&str>) -> Value {
        scalar_functions::comparison::compare_values_with_collation(
            a,
            b,
            |ord| ord == std::cmp::Ordering::Less,
            collation,
        )
        .unwrap_or(Value::Null)
    }

    fn numeric_op<F1, F2>(&self, a: &Value, b: &Value, int_op: F1, float_op: F2) -> Result<Value>
    where
        F1: Fn(i64, i64) -> i64,
        F2: Fn(f64, f64) -> f64,
    {
        match (a, b) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Int64(x), Value::Int64(y)) => Ok(Value::Int64(int_op(*x, *y))),
            (Value::Float64(x), Value::Float64(y)) => Ok(Value::Float64(
                ordered_float::OrderedFloat(float_op(x.0, y.0)),
            )),
            (Value::Int64(x), Value::Float64(y)) => Ok(Value::Float64(
                ordered_float::OrderedFloat(float_op(*x as f64, y.0)),
            )),
            (Value::Float64(x), Value::Int64(y)) => Ok(Value::Float64(
                ordered_float::OrderedFloat(float_op(x.0, *y as f64)),
            )),
            (Value::Numeric(x), Value::Numeric(y)) => {
                use rust_decimal::prelude::ToPrimitive;
                let x_f64 = x.to_f64().unwrap_or(0.0);
                let y_f64 = y.to_f64().unwrap_or(0.0);
                Ok(Value::Float64(ordered_float::OrderedFloat(float_op(
                    x_f64, y_f64,
                ))))
            }
            _ => Err(Error::invalid_query(
                "Numeric operation requires numeric operands",
            )),
        }
    }

    fn eval_array_access_impl(
        &self,
        arr: &Value,
        idx: &Value,
        mode: ArrayAccessMode,
    ) -> Result<Value> {
        match (arr, idx) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Array(elements), Value::Int64(idx)) => {
                let (actual_idx, safe) = match mode {
                    ArrayAccessMode::Default => ((*idx as usize).saturating_sub(1), true),
                    ArrayAccessMode::Offset => (*idx as usize, false),
                    ArrayAccessMode::SafeOffset => (*idx as usize, true),
                    ArrayAccessMode::Ordinal => ((*idx as usize).saturating_sub(1), false),
                    ArrayAccessMode::SafeOrdinal => ((*idx as usize).saturating_sub(1), true),
                };

                if *idx < 0 || actual_idx >= elements.len() {
                    if safe {
                        Ok(Value::Null)
                    } else {
                        Err(Error::invalid_query(format!(
                            "Array index {} out of bounds for array of length {}",
                            idx,
                            elements.len()
                        )))
                    }
                } else {
                    Ok(elements[actual_idx].clone())
                }
            }
            (Value::Json(json), Value::Int64(idx)) => {
                if let Some(arr) = json.as_array() {
                    let actual_idx = *idx as usize;
                    if *idx < 0 || actual_idx >= arr.len() {
                        Ok(Value::Null)
                    } else {
                        Ok(Value::Json(arr[actual_idx].clone()))
                    }
                } else {
                    Ok(Value::Null)
                }
            }
            (Value::Json(json), Value::String(key)) => {
                if let Some(obj) = json.as_object() {
                    Ok(obj
                        .get(key)
                        .map(|v| Value::Json(v.clone()))
                        .unwrap_or(Value::Null))
                } else {
                    Ok(Value::Null)
                }
            }
            _ => Ok(Value::Null),
        }
    }
}

pub fn cast_value(val: Value, target_type: &DataType, safe: bool) -> Result<Value> {
    if val.is_null() {
        return Ok(Value::Null);
    }

    let result = match target_type {
        DataType::Bool => match &val {
            Value::Bool(_) => Ok(val),
            Value::Int64(n) => Ok(Value::Bool(*n != 0)),
            Value::String(s) => match s.to_lowercase().as_str() {
                "true" | "1" => Ok(Value::Bool(true)),
                "false" | "0" => Ok(Value::Bool(false)),
                _ => Err(Error::invalid_query(format!("Cannot cast '{}' to BOOL", s))),
            },
            _ => Err(Error::invalid_query(format!(
                "Cannot cast {:?} to BOOL",
                val
            ))),
        },
        DataType::Int64 => match &val {
            Value::Int64(_) => Ok(val),
            Value::Float64(f) => {
                let float_val = f.0;
                if float_val > i64::MAX as f64 || float_val < i64::MIN as f64 {
                    Err(Error::invalid_query(format!(
                        "Cannot cast {} to INT64: value out of range",
                        float_val
                    )))
                } else {
                    Ok(Value::Int64(float_val as i64))
                }
            }
            Value::String(s) => s
                .parse::<i64>()
                .map(Value::Int64)
                .map_err(|_| Error::invalid_query(format!("Cannot cast '{}' to INT64", s))),
            Value::Bool(b) => Ok(Value::Int64(if *b { 1 } else { 0 })),
            Value::Numeric(d) => {
                use rust_decimal::prelude::ToPrimitive;
                d.to_i64().map(Value::Int64).ok_or_else(|| {
                    Error::invalid_query(format!("Cannot cast {} to INT64: value out of range", d))
                })
            }
            _ => Err(Error::invalid_query(format!(
                "Cannot cast {:?} to INT64",
                val
            ))),
        },
        DataType::Float64 => match &val {
            Value::Float64(_) => Ok(val),
            Value::Int64(n) => Ok(Value::Float64(ordered_float::OrderedFloat(*n as f64))),
            Value::String(s) => s
                .parse::<f64>()
                .map(|f| Value::Float64(ordered_float::OrderedFloat(f)))
                .map_err(|_| Error::invalid_query(format!("Cannot cast '{}' to FLOAT64", s))),
            Value::Numeric(d) => {
                use rust_decimal::prelude::ToPrimitive;
                d.to_f64()
                    .map(|f| Value::Float64(ordered_float::OrderedFloat(f)))
                    .ok_or_else(|| {
                        Error::invalid_query(format!("Cannot cast NUMERIC {} to FLOAT64", d))
                    })
            }
            _ => Err(Error::invalid_query(format!(
                "Cannot cast {:?} to FLOAT64",
                val
            ))),
        },
        DataType::String => match &val {
            Value::String(_) => Ok(val),
            Value::Bytes(b) => String::from_utf8(b.clone())
                .map(Value::String)
                .map_err(|_| Error::invalid_query("Cannot cast BYTES to STRING: invalid UTF-8")),
            _ => Ok(Value::String(val.to_string())),
        },
        DataType::Bytes => match &val {
            Value::Bytes(_) => Ok(val),
            Value::String(s) => Ok(Value::Bytes(s.as_bytes().to_vec())),
            _ => Err(Error::invalid_query(format!(
                "Cannot cast {:?} to BYTES",
                val
            ))),
        },
        DataType::Date => match &val {
            Value::Date(_) => Ok(val),
            Value::String(s) => chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                .map(Value::Date)
                .map_err(|_| Error::invalid_query(format!("Cannot cast '{}' to DATE", s))),
            Value::Timestamp(ts) => Ok(Value::Date(ts.date_naive())),
            Value::DateTime(dt) => Ok(Value::Date(dt.date())),
            _ => Err(Error::invalid_query(format!(
                "Cannot cast {:?} to DATE",
                val
            ))),
        },
        DataType::Timestamp => match &val {
            Value::Timestamp(_) => Ok(val),
            Value::String(s) => parse_timestamp(s),
            Value::Date(d) => Ok(Value::Timestamp(
                d.and_hms_opt(0, 0, 0)
                    .unwrap_or_else(|| d.and_time(chrono::NaiveTime::default()))
                    .and_utc(),
            )),
            Value::DateTime(dt) => Ok(Value::Timestamp(dt.and_utc())),
            _ => Err(Error::invalid_query(format!(
                "Cannot cast {:?} to TIMESTAMP",
                val
            ))),
        },
        DataType::Numeric(_) => match &val {
            Value::Numeric(_) => Ok(val),
            Value::Int64(n) => Ok(Value::Numeric(rust_decimal::Decimal::from(*n))),
            Value::Float64(f) => rust_decimal::Decimal::try_from(f.0)
                .map(Value::Numeric)
                .map_err(|_| Error::invalid_query(format!("Cannot cast FLOAT64 {} to NUMERIC", f))),
            Value::String(s) => s
                .parse::<rust_decimal::Decimal>()
                .map(Value::Numeric)
                .map_err(|_| Error::invalid_query(format!("Cannot cast '{}' to NUMERIC", s))),
            _ => Err(Error::invalid_query(format!(
                "Cannot cast {:?} to NUMERIC",
                val
            ))),
        },
        DataType::BigNumeric => match &val {
            Value::BigNumeric(_) => Ok(val),
            Value::Numeric(d) => Ok(Value::BigNumeric(*d)),
            Value::Int64(n) => Ok(Value::BigNumeric(rust_decimal::Decimal::from(*n))),
            Value::Float64(f) => rust_decimal::Decimal::try_from(f.0)
                .map(Value::BigNumeric)
                .map_err(|_| {
                    Error::invalid_query(format!("Cannot cast FLOAT64 {} to BIGNUMERIC", f))
                }),
            Value::String(s) => s
                .parse::<rust_decimal::Decimal>()
                .map(Value::BigNumeric)
                .map_err(|_| Error::invalid_query(format!("Cannot cast '{}' to BIGNUMERIC", s))),
            _ => Err(Error::invalid_query(format!(
                "Cannot cast {:?} to BIGNUMERIC",
                val
            ))),
        },
        DataType::Array(inner_type) => match &val {
            Value::Array(elements) => {
                let mut casted = Vec::with_capacity(elements.len());
                for elem in elements {
                    casted.push(cast_value(elem.clone(), inner_type, safe)?);
                }
                Ok(Value::Array(casted))
            }
            _ => Err(Error::invalid_query(format!(
                "Cannot cast {:?} to ARRAY",
                val
            ))),
        },
        _ => Ok(val),
    };

    if safe {
        result.or(Ok(Value::Null))
    } else {
        result
    }
}

thread_local! {
    static LIKE_REGEX_CACHE: RefCell<LruCache<(String, bool), regex::Regex>> =
        RefCell::new(LruCache::new(NonZeroUsize::new(256).unwrap()));
}

fn like_match(s: &str, pattern: &str, case_insensitive: bool) -> Result<bool> {
    let s_cow: Cow<str> = if case_insensitive {
        Cow::Owned(s.to_lowercase())
    } else {
        Cow::Borrowed(s)
    };

    let pattern_key = if case_insensitive {
        pattern.to_lowercase()
    } else {
        pattern.to_string()
    };

    LIKE_REGEX_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if let Some(re) = cache.get(&(pattern_key.clone(), case_insensitive)) {
            return Ok(re.is_match(&s_cow));
        }
        let regex_pattern = pattern_key.replace('%', ".*").replace('_', ".");
        let re =
            regex::Regex::new(&format!("^{}$", regex_pattern)).map_err(|e| Error::RegexError {
                pattern: pattern.to_string(),
                reason: e.to_string(),
            })?;
        let result = re.is_match(&s_cow);
        cache.put((pattern_key, case_insensitive), re);
        Ok(result)
    })
}

fn parse_timestamp(s: &str) -> Result<Value> {
    let formats = [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
    ];

    for fmt in &formats {
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, fmt) {
            return Ok(Value::Timestamp(dt.and_utc()));
        }
    }

    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return Ok(Value::Timestamp(dt.with_timezone(&chrono::Utc)));
    }

    Err(Error::invalid_query(format!(
        "Cannot parse '{}' as TIMESTAMP",
        s
    )))
}
