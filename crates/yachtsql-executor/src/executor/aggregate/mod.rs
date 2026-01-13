#![coverage(off)]

mod accumulator;

use accumulator::Accumulator;
use rustc_hash::{FxHashMap, FxHashSet};

type GroupMap<V> = FxHashMap<Vec<Value>, V>;
use ordered_float::OrderedFloat;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;
use yachtsql_ir::{AggregateFunction, Expr, PlanSchema};
use yachtsql_storage::{Column, Record, Table};

use super::plan_schema_to_schema;
use crate::value_evaluator::ValueEvaluator;

pub(crate) fn compute_aggregate(
    input_table: &Table,
    group_by: &[Expr],
    aggregates: &[Expr],
    schema: &PlanSchema,
    grouping_sets: Option<&Vec<Vec<usize>>>,
    variables: &FxHashMap<String, Value>,
    user_function_defs: &FxHashMap<String, crate::value_evaluator::UserFunctionDef>,
    parallel: bool,
    threshold: usize,
) -> Result<Table> {
    if can_use_columnar_aggregate(aggregates, group_by, grouping_sets) {
        return execute_columnar_aggregate(input_table, aggregates, schema);
    }

    let input_schema = input_table.schema().clone();
    let evaluator = ValueEvaluator::new(&input_schema)
        .with_variables(variables)
        .with_user_functions(user_function_defs);

    let result_schema = plan_schema_to_schema(schema);
    let mut result = Table::empty(result_schema);

    let n = input_table.row_count();
    let columns: Vec<&Column> = input_table
        .columns()
        .iter()
        .map(|(_, c)| c.as_ref())
        .collect();

    if group_by.is_empty() {
        let mut accumulators: Vec<Accumulator> =
            aggregates.iter().map(Accumulator::from_expr).collect();

        let mut record = Record::with_capacity(columns.len());
        for i in 0..n {
            fill_record_from_columns(&mut record, &columns, i);
            for (acc, agg_expr) in accumulators.iter_mut().zip(aggregates.iter()) {
                if matches!(
                    acc,
                    Accumulator::SumIf(_)
                        | Accumulator::AvgIf { .. }
                        | Accumulator::MinIf(_)
                        | Accumulator::MaxIf(_)
                ) {
                    let (value, condition) =
                        extract_conditional_agg_args(&evaluator, agg_expr, &record)?;
                    acc.accumulate_conditional(&value, condition)?;
                } else if matches!(acc, Accumulator::ArrayAgg { .. }) {
                    let arg_val = extract_agg_arg(&evaluator, agg_expr, &record)?;
                    let sort_keys = extract_order_by_keys(&evaluator, agg_expr, &record)?;
                    acc.accumulate_array_agg(&arg_val, sort_keys)?;
                } else if matches!(acc, Accumulator::Covariance { .. }) {
                    let (x, y) = extract_bivariate_args(&evaluator, agg_expr, &record)?;
                    acc.accumulate_bivariate(&x, &y)?;
                } else if matches!(acc, Accumulator::ApproxTopSum { .. }) {
                    let (value, weight) = extract_bivariate_args(&evaluator, agg_expr, &record)?;
                    acc.accumulate_approx_top_sum(&value, &weight)?;
                } else {
                    let arg_val = extract_agg_arg(&evaluator, agg_expr, &record)?;
                    acc.accumulate(&arg_val)?;
                }
            }
        }

        let row: Vec<Value> = accumulators.iter().map(|a| a.finalize()).collect();
        result.push_row(row)?;
    } else if let Some(sets) = grouping_sets {
        for grouping_set in sets {
            let active_indices_vec: Vec<usize> = grouping_set.clone();
            let active_indices_set: FxHashSet<usize> = active_indices_vec.iter().copied().collect();
            let mut group_map: GroupMap<(Vec<Accumulator>, Vec<usize>)> = FxHashMap::default();

            let mut record = Record::with_capacity(columns.len());
            for i in 0..n {
                fill_record_from_columns(&mut record, &columns, i);
                let mut group_key_values = Vec::new();
                for (idx, group_expr) in group_by.iter().enumerate() {
                    if active_indices_set.contains(&idx) {
                        let val = evaluator.evaluate(group_expr, &record)?;
                        group_key_values.push(val);
                    } else {
                        group_key_values.push(Value::Null);
                    }
                }

                let entry = group_map
                    .entry(group_key_values)
                    .or_insert_with_key(|_key| {
                        let mut accs: Vec<Accumulator> =
                            aggregates.iter().map(Accumulator::from_expr).collect();
                        for (acc, agg_expr) in accs.iter_mut().zip(aggregates.iter()) {
                            match acc {
                                Accumulator::Grouping { .. } => {
                                    let col_idx = get_grouping_column_index(agg_expr, group_by);
                                    let is_active = col_idx
                                        .map(|idx| active_indices_set.contains(&idx))
                                        .unwrap_or(true);
                                    acc.set_grouping_value(if is_active { 0 } else { 1 });
                                }
                                Accumulator::GroupingId { .. } => {
                                    let gid = compute_grouping_id(
                                        agg_expr,
                                        group_by,
                                        &active_indices_set,
                                    );
                                    acc.set_grouping_value(gid);
                                }
                                _ => {}
                            }
                        }
                        (accs, active_indices_vec.clone())
                    });

                for (acc, agg_expr) in entry.0.iter_mut().zip(aggregates.iter()) {
                    if matches!(
                        acc,
                        Accumulator::Grouping { .. } | Accumulator::GroupingId { .. }
                    ) {
                        continue;
                    }
                    if matches!(
                        acc,
                        Accumulator::SumIf(_)
                            | Accumulator::AvgIf { .. }
                            | Accumulator::MinIf(_)
                            | Accumulator::MaxIf(_)
                    ) {
                        let (value, condition) =
                            extract_conditional_agg_args(&evaluator, agg_expr, &record)?;
                        acc.accumulate_conditional(&value, condition)?;
                    } else if matches!(acc, Accumulator::ArrayAgg { .. }) {
                        let arg_val = extract_agg_arg(&evaluator, agg_expr, &record)?;
                        let sort_keys = extract_order_by_keys(&evaluator, agg_expr, &record)?;
                        acc.accumulate_array_agg(&arg_val, sort_keys)?;
                    } else if matches!(acc, Accumulator::Covariance { .. }) {
                        let (x, y) = extract_bivariate_args(&evaluator, agg_expr, &record)?;
                        acc.accumulate_bivariate(&x, &y)?;
                    } else if matches!(acc, Accumulator::ApproxTopSum { .. }) {
                        let (value, weight) =
                            extract_bivariate_args(&evaluator, agg_expr, &record)?;
                        acc.accumulate_approx_top_sum(&value, &weight)?;
                    } else {
                        let arg_val = extract_agg_arg(&evaluator, agg_expr, &record)?;
                        acc.accumulate(&arg_val)?;
                    }
                }
            }

            for (group_key, (accumulators, _active)) in group_map {
                let mut row = group_key;
                row.extend(accumulators.iter().map(|a| a.finalize()));
                result.push_row(row)?;
            }
        }
    } else {
        let sample_accs: Vec<Accumulator> = aggregates.iter().map(Accumulator::from_expr).collect();
        let can_merge = sample_accs.iter().all(|a| a.is_mergeable());

        if parallel && n >= threshold && can_merge {
            let num_threads = std::thread::available_parallelism().map_or(4, |n| n.get());
            let chunk_size = n.div_ceil(num_threads);

            let input_schema_ref = &input_schema;
            let columns_ref = &columns;
            let local_results: Vec<Result<GroupMap<Vec<Accumulator>>>> = std::thread::scope(|s| {
                let handles: Vec<_> = (0..n)
                    .collect::<Vec<_>>()
                    .chunks(chunk_size)
                    .map(|chunk| {
                        let chunk_indices: Vec<usize> = chunk.to_vec();
                        s.spawn(move || {
                            let evaluator = ValueEvaluator::new(input_schema_ref)
                                .with_variables(variables)
                                .with_user_functions(user_function_defs);
                            let mut local_groups: GroupMap<Vec<Accumulator>> = FxHashMap::default();
                            let mut record = Record::with_capacity(columns_ref.len());

                            for &idx in &chunk_indices {
                                fill_record_from_columns(&mut record, columns_ref, idx);
                                let group_key_values: Vec<Value> = group_by
                                    .iter()
                                    .map(|e| evaluator.evaluate(e, &record))
                                    .collect::<Result<_>>()?;

                                let accumulators =
                                    local_groups.entry(group_key_values).or_insert_with(|| {
                                        aggregates.iter().map(Accumulator::from_expr).collect()
                                    });

                                for (acc, agg_expr) in
                                    accumulators.iter_mut().zip(aggregates.iter())
                                {
                                    if matches!(
                                        acc,
                                        Accumulator::SumIf(_)
                                            | Accumulator::AvgIf { .. }
                                            | Accumulator::MinIf(_)
                                            | Accumulator::MaxIf(_)
                                    ) {
                                        let (value, condition) = extract_conditional_agg_args(
                                            &evaluator, agg_expr, &record,
                                        )?;
                                        acc.accumulate_conditional(&value, condition)?;
                                    } else {
                                        let arg_val =
                                            extract_agg_arg(&evaluator, agg_expr, &record)?;
                                        acc.accumulate(&arg_val)?;
                                    }
                                }
                            }
                            Ok(local_groups)
                        })
                    })
                    .collect();
                handles
                    .into_iter()
                    .map(|h| {
                        h.join()
                            .map_err(|_| Error::internal("Thread join failed"))?
                    })
                    .collect()
            });

            let mut merged_groups: GroupMap<Vec<Accumulator>> = FxHashMap::default();

            for local_result in local_results {
                let local_groups = local_result?;
                for (key, local_accs) in local_groups {
                    merged_groups
                        .entry(key)
                        .and_modify(|existing| {
                            for (e, l) in existing.iter_mut().zip(local_accs.iter()) {
                                e.merge(l);
                            }
                        })
                        .or_insert(local_accs);
                }
            }

            for (group_key, accumulators) in merged_groups {
                let mut row = group_key;
                row.extend(accumulators.iter().map(|a| a.finalize()));
                result.push_row(row)?;
            }
        } else {
            let mut groups: GroupMap<Vec<Accumulator>> = FxHashMap::default();
            let mut record = Record::with_capacity(columns.len());

            for i in 0..n {
                fill_record_from_columns(&mut record, &columns, i);
                let group_key_values: Vec<Value> = group_by
                    .iter()
                    .map(|e| evaluator.evaluate(e, &record))
                    .collect::<Result<_>>()?;

                let accumulators = groups
                    .entry(group_key_values)
                    .or_insert_with(|| aggregates.iter().map(Accumulator::from_expr).collect());

                for (acc, agg_expr) in accumulators.iter_mut().zip(aggregates.iter()) {
                    if matches!(
                        acc,
                        Accumulator::SumIf(_)
                            | Accumulator::AvgIf { .. }
                            | Accumulator::MinIf(_)
                            | Accumulator::MaxIf(_)
                    ) {
                        let (value, condition) =
                            extract_conditional_agg_args(&evaluator, agg_expr, &record)?;
                        acc.accumulate_conditional(&value, condition)?;
                    } else if matches!(acc, Accumulator::ArrayAgg { .. }) {
                        let arg_val = extract_agg_arg(&evaluator, agg_expr, &record)?;
                        let sort_keys = extract_order_by_keys(&evaluator, agg_expr, &record)?;
                        acc.accumulate_array_agg(&arg_val, sort_keys)?;
                    } else if matches!(acc, Accumulator::Covariance { .. }) {
                        let (x, y) = extract_bivariate_args(&evaluator, agg_expr, &record)?;
                        acc.accumulate_bivariate(&x, &y)?;
                    } else if matches!(acc, Accumulator::ApproxTopSum { .. }) {
                        let (value, weight) =
                            extract_bivariate_args(&evaluator, agg_expr, &record)?;
                        acc.accumulate_approx_top_sum(&value, &weight)?;
                    } else {
                        let arg_val = extract_agg_arg(&evaluator, agg_expr, &record)?;
                        acc.accumulate(&arg_val)?;
                    }
                }
            }

            for (group_key, accumulators) in groups {
                let mut row = group_key;
                row.extend(accumulators.iter().map(|a| a.finalize()));
                result.push_row(row)?;
            }
        }
    }

    Ok(result)
}

#[allow(clippy::wildcard_enum_match_arm)]
fn get_simple_column_index(expr: &Expr) -> Option<usize> {
    match expr {
        Expr::Column { index, .. } => *index,
        Expr::Alias { expr, .. } => get_simple_column_index(expr),
        Expr::Aggregate { args, .. } if args.len() == 1 => get_simple_column_index(&args[0]),
        _ => None,
    }
}

#[allow(clippy::wildcard_enum_match_arm)]
fn can_use_columnar_aggregate(
    aggregates: &[Expr],
    group_by: &[Expr],
    grouping_sets: Option<&Vec<Vec<usize>>>,
) -> bool {
    if !group_by.is_empty() || grouping_sets.is_some() {
        return false;
    }

    aggregates.iter().all(|expr| {
        let (func, distinct, filter, args) = match expr {
            Expr::Aggregate {
                func,
                args,
                distinct,
                filter,
                ..
            } => (func, *distinct, filter.is_some(), args),
            Expr::Alias { expr, .. } => match expr.as_ref() {
                Expr::Aggregate {
                    func,
                    args,
                    distinct,
                    filter,
                    ..
                } => (func, *distinct, filter.is_some(), args),
                _ => return false,
            },
            _ => return false,
        };

        if distinct || filter {
            return false;
        }

        let is_simple_column = args.len() == 1 && get_simple_column_index(&args[0]).is_some();
        let is_count_star = args.is_empty() || matches!(args.first(), Some(Expr::Wildcard { .. }));

        match func {
            AggregateFunction::Count => is_count_star || is_simple_column,
            AggregateFunction::Sum
            | AggregateFunction::Avg
            | AggregateFunction::Min
            | AggregateFunction::Max => is_simple_column,
            _ => false,
        }
    })
}

fn execute_columnar_aggregate(
    input_table: &Table,
    aggregates: &[Expr],
    schema: &PlanSchema,
) -> Result<Table> {
    let result_schema = plan_schema_to_schema(schema);
    let mut result = Table::empty(result_schema);
    let mut row: Vec<Value> = Vec::with_capacity(aggregates.len());

    for expr in aggregates {
        let (func, args) = match expr {
            Expr::Aggregate { func, args, .. } => (func, args),
            Expr::Alias { expr, .. } => match expr.as_ref() {
                Expr::Aggregate { func, args, .. } => (func, args),
                _ => {
                    return Err(Error::internal(
                        "Expected Aggregate inside Alias in columnar aggregate",
                    ));
                }
            },
            _ => {
                return Err(Error::internal(
                    "Expected Aggregate expression in columnar aggregate",
                ));
            }
        };

        let value = match func {
            AggregateFunction::Count => {
                if args.is_empty() || matches!(args.first(), Some(Expr::Wildcard { .. })) {
                    Value::Int64(input_table.row_count() as i64)
                } else {
                    let col_idx = get_simple_column_index(&args[0])
                        .ok_or_else(|| Error::internal("Missing column index for COUNT"))?;
                    let column = input_table
                        .column(col_idx)
                        .ok_or_else(|| Error::internal("Column not found for COUNT"))?;
                    Value::Int64(column.count_valid() as i64)
                }
            }
            AggregateFunction::Sum => {
                let col_idx = get_simple_column_index(&args[0])
                    .ok_or_else(|| Error::internal("Missing column index for SUM"))?;
                let column = input_table
                    .column(col_idx)
                    .ok_or_else(|| Error::internal("Column not found for SUM"))?;
                column.sum().map(Value::float64).unwrap_or(Value::Null)
            }
            AggregateFunction::Avg => {
                let col_idx = get_simple_column_index(&args[0])
                    .ok_or_else(|| Error::internal("Missing column index for AVG"))?;
                let column = input_table
                    .column(col_idx)
                    .ok_or_else(|| Error::internal("Column not found for AVG"))?;
                let count = column.count_valid();
                if count == 0 {
                    Value::Null
                } else {
                    column
                        .sum()
                        .map(|s| Value::float64(s / count as f64))
                        .unwrap_or(Value::Null)
                }
            }
            AggregateFunction::Min => {
                let col_idx = get_simple_column_index(&args[0])
                    .ok_or_else(|| Error::internal("Missing column index for MIN"))?;
                let column = input_table
                    .column(col_idx)
                    .ok_or_else(|| Error::internal("Column not found for MIN"))?;
                column.min().unwrap_or(Value::Null)
            }
            AggregateFunction::Max => {
                let col_idx = get_simple_column_index(&args[0])
                    .ok_or_else(|| Error::internal("Missing column index for MAX"))?;
                let column = input_table
                    .column(col_idx)
                    .ok_or_else(|| Error::internal("Column not found for MAX"))?;
                column.max().unwrap_or(Value::Null)
            }
            _ => {
                return Err(Error::internal(format!(
                    "Unsupported aggregate function in columnar aggregate: {:?}",
                    func
                )));
            }
        };
        row.push(value);
    }

    result.push_row(row)?;
    Ok(result)
}

fn get_record_from_columns(columns: &[&Column], idx: usize) -> Record {
    let values: Vec<Value> = columns.iter().map(|c| c.get_value(idx)).collect();
    Record::from_values(values)
}

fn fill_record_from_columns(record: &mut Record, columns: &[&Column], idx: usize) {
    record.set_from_columns(columns, idx);
}

fn extract_agg_arg(
    evaluator: &ValueEvaluator,
    agg_expr: &Expr,
    record: &yachtsql_storage::Record,
) -> Result<Value> {
    match agg_expr {
        Expr::Aggregate { args, .. } => {
            if args.is_empty() {
                Ok(Value::Null)
            } else if matches!(&args[0], Expr::Wildcard { .. }) {
                Ok(Value::Int64(1))
            } else {
                evaluator.evaluate(&args[0], record)
            }
        }
        Expr::UserDefinedAggregate { args, .. } => {
            if args.is_empty() {
                Ok(Value::Null)
            } else {
                evaluator.evaluate(&args[0], record)
            }
        }
        Expr::Alias { expr, .. } => extract_agg_arg(evaluator, expr, record),
        Expr::Literal(_)
        | Expr::Column { .. }
        | Expr::BinaryOp { .. }
        | Expr::UnaryOp { .. }
        | Expr::ScalarFunction { .. }
        | Expr::Window { .. }
        | Expr::AggregateWindow { .. }
        | Expr::Case { .. }
        | Expr::Cast { .. }
        | Expr::IsNull { .. }
        | Expr::IsDistinctFrom { .. }
        | Expr::InList { .. }
        | Expr::InSubquery { .. }
        | Expr::InUnnest { .. }
        | Expr::Exists { .. }
        | Expr::Between { .. }
        | Expr::Like { .. }
        | Expr::Extract { .. }
        | Expr::Substring { .. }
        | Expr::Trim { .. }
        | Expr::Position { .. }
        | Expr::Overlay { .. }
        | Expr::Array { .. }
        | Expr::ArrayAccess { .. }
        | Expr::Struct { .. }
        | Expr::StructAccess { .. }
        | Expr::TypedString { .. }
        | Expr::Interval { .. }
        | Expr::Wildcard { .. }
        | Expr::Subquery(_)
        | Expr::ScalarSubquery(_)
        | Expr::ArraySubquery(_)
        | Expr::Parameter { .. }
        | Expr::Variable { .. }
        | Expr::Placeholder { .. }
        | Expr::Lambda { .. }
        | Expr::AtTimeZone { .. }
        | Expr::JsonAccess { .. }
        | Expr::Default => evaluator.evaluate(agg_expr, record),
    }
}

fn extract_conditional_agg_args(
    evaluator: &ValueEvaluator,
    agg_expr: &Expr,
    record: &yachtsql_storage::Record,
) -> Result<(Value, bool)> {
    match agg_expr {
        Expr::Aggregate { args, .. } => {
            if args.len() >= 2 {
                let value = evaluator.evaluate(&args[0], record)?;
                let condition_val = evaluator.evaluate(&args[1], record)?;
                let condition = condition_val.as_bool().unwrap_or(false);
                Ok((value, condition))
            } else if args.len() == 1 {
                let value = evaluator.evaluate(&args[0], record)?;
                Ok((value, true))
            } else {
                Ok((Value::Null, false))
            }
        }
        Expr::UserDefinedAggregate { args, .. } => {
            if args.len() >= 2 {
                let value = evaluator.evaluate(&args[0], record)?;
                let condition_val = evaluator.evaluate(&args[1], record)?;
                let condition = condition_val.as_bool().unwrap_or(false);
                Ok((value, condition))
            } else if args.len() == 1 {
                let value = evaluator.evaluate(&args[0], record)?;
                Ok((value, true))
            } else {
                Ok((Value::Null, false))
            }
        }
        Expr::Alias { expr, .. } => extract_conditional_agg_args(evaluator, expr, record),
        Expr::Literal(_)
        | Expr::Column { .. }
        | Expr::BinaryOp { .. }
        | Expr::UnaryOp { .. }
        | Expr::ScalarFunction { .. }
        | Expr::Window { .. }
        | Expr::AggregateWindow { .. }
        | Expr::Case { .. }
        | Expr::Cast { .. }
        | Expr::IsNull { .. }
        | Expr::IsDistinctFrom { .. }
        | Expr::InList { .. }
        | Expr::InSubquery { .. }
        | Expr::InUnnest { .. }
        | Expr::Exists { .. }
        | Expr::Between { .. }
        | Expr::Like { .. }
        | Expr::Extract { .. }
        | Expr::Substring { .. }
        | Expr::Trim { .. }
        | Expr::Position { .. }
        | Expr::Overlay { .. }
        | Expr::Array { .. }
        | Expr::ArrayAccess { .. }
        | Expr::Struct { .. }
        | Expr::StructAccess { .. }
        | Expr::TypedString { .. }
        | Expr::Interval { .. }
        | Expr::Wildcard { .. }
        | Expr::Subquery(_)
        | Expr::ScalarSubquery(_)
        | Expr::ArraySubquery(_)
        | Expr::Parameter { .. }
        | Expr::Variable { .. }
        | Expr::Placeholder { .. }
        | Expr::Lambda { .. }
        | Expr::AtTimeZone { .. }
        | Expr::JsonAccess { .. }
        | Expr::Default => Ok((Value::Null, false)),
    }
}

fn extract_bivariate_args(
    evaluator: &ValueEvaluator,
    agg_expr: &Expr,
    record: &yachtsql_storage::Record,
) -> Result<(Value, Value)> {
    match agg_expr {
        Expr::Aggregate { args, .. } | Expr::UserDefinedAggregate { args, .. } => {
            if args.len() >= 2 {
                let x = evaluator.evaluate(&args[0], record)?;
                let y = evaluator.evaluate(&args[1], record)?;
                Ok((x, y))
            } else {
                Ok((Value::Null, Value::Null))
            }
        }
        Expr::Alias { expr, .. } => extract_bivariate_args(evaluator, expr, record),
        Expr::Literal(_)
        | Expr::Column { .. }
        | Expr::BinaryOp { .. }
        | Expr::UnaryOp { .. }
        | Expr::ScalarFunction { .. }
        | Expr::Window { .. }
        | Expr::AggregateWindow { .. }
        | Expr::Case { .. }
        | Expr::Cast { .. }
        | Expr::IsNull { .. }
        | Expr::IsDistinctFrom { .. }
        | Expr::InList { .. }
        | Expr::InSubquery { .. }
        | Expr::InUnnest { .. }
        | Expr::Exists { .. }
        | Expr::Between { .. }
        | Expr::Like { .. }
        | Expr::Extract { .. }
        | Expr::Substring { .. }
        | Expr::Trim { .. }
        | Expr::Position { .. }
        | Expr::Overlay { .. }
        | Expr::Array { .. }
        | Expr::ArrayAccess { .. }
        | Expr::Struct { .. }
        | Expr::StructAccess { .. }
        | Expr::TypedString { .. }
        | Expr::Interval { .. }
        | Expr::Wildcard { .. }
        | Expr::Subquery(_)
        | Expr::ScalarSubquery(_)
        | Expr::ArraySubquery(_)
        | Expr::Parameter { .. }
        | Expr::Variable { .. }
        | Expr::Placeholder { .. }
        | Expr::Lambda { .. }
        | Expr::AtTimeZone { .. }
        | Expr::JsonAccess { .. }
        | Expr::Default => Ok((Value::Null, Value::Null)),
    }
}

fn extract_order_by_keys(
    evaluator: &ValueEvaluator,
    agg_expr: &Expr,
    record: &yachtsql_storage::Record,
) -> Result<Vec<(Value, bool)>> {
    match agg_expr {
        Expr::Aggregate { order_by, .. } => {
            let mut keys = Vec::new();
            for sort_expr in order_by {
                let val = evaluator.evaluate(&sort_expr.expr, record)?;
                keys.push((val, sort_expr.asc));
            }
            Ok(keys)
        }
        Expr::UserDefinedAggregate { .. } => Ok(Vec::new()),
        Expr::Alias { expr, .. } => extract_order_by_keys(evaluator, expr, record),
        Expr::Literal(_)
        | Expr::Column { .. }
        | Expr::BinaryOp { .. }
        | Expr::UnaryOp { .. }
        | Expr::ScalarFunction { .. }
        | Expr::Window { .. }
        | Expr::AggregateWindow { .. }
        | Expr::Case { .. }
        | Expr::Cast { .. }
        | Expr::IsNull { .. }
        | Expr::IsDistinctFrom { .. }
        | Expr::InList { .. }
        | Expr::InSubquery { .. }
        | Expr::InUnnest { .. }
        | Expr::Exists { .. }
        | Expr::Between { .. }
        | Expr::Like { .. }
        | Expr::Extract { .. }
        | Expr::Substring { .. }
        | Expr::Trim { .. }
        | Expr::Position { .. }
        | Expr::Overlay { .. }
        | Expr::Array { .. }
        | Expr::ArrayAccess { .. }
        | Expr::Struct { .. }
        | Expr::StructAccess { .. }
        | Expr::TypedString { .. }
        | Expr::Interval { .. }
        | Expr::Wildcard { .. }
        | Expr::Subquery(_)
        | Expr::ScalarSubquery(_)
        | Expr::ArraySubquery(_)
        | Expr::Parameter { .. }
        | Expr::Variable { .. }
        | Expr::Placeholder { .. }
        | Expr::Lambda { .. }
        | Expr::AtTimeZone { .. }
        | Expr::JsonAccess { .. }
        | Expr::Default => Ok(Vec::new()),
    }
}

fn has_order_by(agg_expr: &Expr) -> bool {
    match agg_expr {
        Expr::Aggregate { order_by, .. } => !order_by.is_empty(),
        Expr::UserDefinedAggregate { .. } => false,
        Expr::Alias { expr, .. } => has_order_by(expr),
        Expr::Literal(_)
        | Expr::Column { .. }
        | Expr::BinaryOp { .. }
        | Expr::UnaryOp { .. }
        | Expr::ScalarFunction { .. }
        | Expr::Window { .. }
        | Expr::AggregateWindow { .. }
        | Expr::Case { .. }
        | Expr::Cast { .. }
        | Expr::IsNull { .. }
        | Expr::IsDistinctFrom { .. }
        | Expr::InList { .. }
        | Expr::InSubquery { .. }
        | Expr::InUnnest { .. }
        | Expr::Exists { .. }
        | Expr::Between { .. }
        | Expr::Like { .. }
        | Expr::Extract { .. }
        | Expr::Substring { .. }
        | Expr::Trim { .. }
        | Expr::Position { .. }
        | Expr::Overlay { .. }
        | Expr::Array { .. }
        | Expr::ArrayAccess { .. }
        | Expr::Struct { .. }
        | Expr::StructAccess { .. }
        | Expr::TypedString { .. }
        | Expr::Interval { .. }
        | Expr::Wildcard { .. }
        | Expr::Subquery(_)
        | Expr::ScalarSubquery(_)
        | Expr::ArraySubquery(_)
        | Expr::Parameter { .. }
        | Expr::Variable { .. }
        | Expr::Placeholder { .. }
        | Expr::Lambda { .. }
        | Expr::AtTimeZone { .. }
        | Expr::JsonAccess { .. }
        | Expr::Default => false,
    }
}

fn get_grouping_column_index(agg_expr: &Expr, group_by: &[Expr]) -> Option<usize> {
    let arg = match agg_expr {
        Expr::Aggregate { args, .. } => args.first(),
        Expr::Alias { expr, .. } => {
            return get_grouping_column_index(expr, group_by);
        }
        _ => None,
    }?;

    for (i, group_expr) in group_by.iter().enumerate() {
        if exprs_match(arg, group_expr) {
            return Some(i);
        }
    }
    None
}

fn exprs_match(a: &Expr, b: &Expr) -> bool {
    match (a, b) {
        (
            Expr::Column {
                name: n1,
                table: t1,
                ..
            },
            Expr::Column {
                name: n2,
                table: t2,
                ..
            },
        ) => {
            n1.eq_ignore_ascii_case(n2)
                && match (t1, t2) {
                    (Some(t1), Some(t2)) => t1.eq_ignore_ascii_case(t2),
                    (None, None) => true,
                    _ => true,
                }
        }
        (Expr::Alias { expr: e1, .. }, e2) => exprs_match(e1, e2),
        (e1, Expr::Alias { expr: e2, .. }) => exprs_match(e1, e2),
        _ => a == b,
    }
}

fn compute_grouping_id(
    agg_expr: &Expr,
    group_by: &[Expr],
    active_indices: &FxHashSet<usize>,
) -> i64 {
    let args = match agg_expr {
        Expr::Aggregate { args, .. } => args,
        Expr::Alias { expr, .. } => {
            return compute_grouping_id(expr, group_by, active_indices);
        }
        _ => return 0,
    };

    let mut result: i64 = 0;
    let n = args.len();
    for (arg_pos, arg) in args.iter().enumerate() {
        let mut is_active = true;
        for (i, group_expr) in group_by.iter().enumerate() {
            if exprs_match(arg, group_expr) {
                is_active = active_indices.contains(&i);
                break;
            }
        }
        if !is_active {
            result |= 1 << (n - 1 - arg_pos);
        }
    }
    result
}
