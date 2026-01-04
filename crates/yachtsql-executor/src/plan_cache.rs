#![coverage(off)]

use std::num::NonZeroUsize;

use lru::LruCache;
use rustc_hash::{FxHashMap, FxHashSet};
use xxhash_rust::xxh3::xxh3_64;
use yachtsql_ir::{Expr, LogicalPlan};

type SqlHash = u64;

fn hash_sql(sql: &str) -> SqlHash {
    xxh3_64(sql.as_bytes())
}

pub struct PlanCache {
    plans: LruCache<SqlHash, LogicalPlan>,
    object_to_hashes: FxHashMap<String, FxHashSet<SqlHash>>,
}

impl PlanCache {
    pub fn new(capacity: NonZeroUsize) -> Self {
        Self {
            plans: LruCache::new(capacity),
            object_to_hashes: FxHashMap::default(),
        }
    }

    pub fn get(&mut self, sql: &str) -> Option<LogicalPlan> {
        self.plans.get(&hash_sql(sql)).cloned()
    }

    pub fn insert(&mut self, sql: &str, plan: LogicalPlan) {
        let hash = hash_sql(sql);
        let objects = extract_referenced_objects(&plan);
        for obj in objects {
            self.object_to_hashes.entry(obj).or_default().insert(hash);
        }
        self.plans.put(hash, plan);
    }

    pub fn invalidate_objects(&mut self, objects: &[String]) {
        for obj in objects {
            if let Some(hashes) = self.object_to_hashes.remove(obj) {
                for hash in hashes {
                    self.plans.pop(&hash);
                }
            }
        }
    }

    pub fn clear(&mut self) {
        self.plans.clear();
        self.object_to_hashes.clear();
    }
}

fn extract_referenced_objects(plan: &LogicalPlan) -> FxHashSet<String> {
    let mut objects = FxHashSet::default();
    collect_objects_from_plan(plan, &mut objects);
    objects
}

fn collect_objects_from_plan(plan: &LogicalPlan, objects: &mut FxHashSet<String>) {
    match plan {
        LogicalPlan::Scan { table_name, .. } => {
            objects.insert(table_name.clone());
        }
        LogicalPlan::Sample { input, .. } => collect_objects_from_plan(input, objects),
        LogicalPlan::Filter {
            input, predicate, ..
        } => {
            collect_objects_from_plan(input, objects);
            collect_objects_from_expr(predicate, objects);
        }
        LogicalPlan::Project {
            input, expressions, ..
        } => {
            collect_objects_from_plan(input, objects);
            for expr in expressions {
                collect_objects_from_expr(expr, objects);
            }
        }
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggregates,
            ..
        } => {
            collect_objects_from_plan(input, objects);
            for expr in group_by {
                collect_objects_from_expr(expr, objects);
            }
            for expr in aggregates {
                collect_objects_from_expr(expr, objects);
            }
        }
        LogicalPlan::Join {
            left,
            right,
            condition,
            ..
        } => {
            collect_objects_from_plan(left, objects);
            collect_objects_from_plan(right, objects);
            if let Some(cond) = condition {
                collect_objects_from_expr(cond, objects);
            }
        }
        LogicalPlan::Sort { input, .. } => collect_objects_from_plan(input, objects),
        LogicalPlan::Limit { input, .. } => collect_objects_from_plan(input, objects),
        LogicalPlan::Distinct { input, .. } => collect_objects_from_plan(input, objects),
        LogicalPlan::Values { values, .. } => {
            for row in values {
                for expr in row {
                    collect_objects_from_expr(expr, objects);
                }
            }
        }
        LogicalPlan::Empty { .. } => {}
        LogicalPlan::SetOperation { left, right, .. } => {
            collect_objects_from_plan(left, objects);
            collect_objects_from_plan(right, objects);
        }
        LogicalPlan::Window {
            input,
            window_exprs,
            ..
        } => {
            collect_objects_from_plan(input, objects);
            for expr in window_exprs {
                collect_objects_from_expr(expr, objects);
            }
        }
        LogicalPlan::WithCte { ctes, body } => {
            for cte in ctes {
                collect_objects_from_plan(&cte.query, objects);
            }
            collect_objects_from_plan(body, objects);
        }
        LogicalPlan::Unnest { input, .. } => collect_objects_from_plan(input, objects),
        LogicalPlan::Qualify {
            input, predicate, ..
        } => {
            collect_objects_from_plan(input, objects);
            collect_objects_from_expr(predicate, objects);
        }
        LogicalPlan::Insert {
            table_name, source, ..
        } => {
            objects.insert(table_name.clone());
            collect_objects_from_plan(source, objects);
        }
        LogicalPlan::Update {
            table_name,
            from,
            filter,
            assignments,
            ..
        } => {
            objects.insert(table_name.clone());
            if let Some(from) = from {
                collect_objects_from_plan(from, objects);
            }
            if let Some(filter) = filter {
                collect_objects_from_expr(filter, objects);
            }
            for assignment in assignments {
                collect_objects_from_expr(&assignment.value, objects);
            }
        }
        LogicalPlan::Delete {
            table_name, filter, ..
        } => {
            objects.insert(table_name.clone());
            if let Some(filter) = filter {
                collect_objects_from_expr(filter, objects);
            }
        }
        LogicalPlan::Merge {
            target_table,
            source,
            on,
            ..
        } => {
            objects.insert(target_table.clone());
            collect_objects_from_plan(source, objects);
            collect_objects_from_expr(on, objects);
        }
        LogicalPlan::CreateTable {
            table_name, query, ..
        } => {
            objects.insert(table_name.clone());
            if let Some(query) = query {
                collect_objects_from_plan(query, objects);
            }
        }
        LogicalPlan::DropTable { table_names, .. } => {
            objects.extend(table_names.iter().cloned());
        }
        LogicalPlan::AlterTable { table_name, .. } => {
            objects.insert(table_name.clone());
        }
        LogicalPlan::Truncate { table_name } => {
            objects.insert(table_name.clone());
        }
        LogicalPlan::CreateView { name, query, .. } => {
            objects.insert(name.clone());
            collect_objects_from_plan(query, objects);
        }
        LogicalPlan::DropView { name, .. } => {
            objects.insert(name.clone());
        }
        LogicalPlan::CreateSchema { .. } => {}
        LogicalPlan::DropSchema { .. } => {}
        LogicalPlan::UndropSchema { .. } => {}
        LogicalPlan::AlterSchema { .. } => {}
        LogicalPlan::CreateFunction { name, .. } => {
            objects.insert(name.clone());
        }
        LogicalPlan::DropFunction { name, .. } => {
            objects.insert(name.clone());
        }
        LogicalPlan::CreateProcedure { name, .. } => {
            objects.insert(name.clone());
        }
        LogicalPlan::DropProcedure { name, .. } => {
            objects.insert(name.clone());
        }
        LogicalPlan::Call {
            procedure_name,
            args,
            ..
        } => {
            objects.insert(procedure_name.clone());
            for arg in args {
                collect_objects_from_expr(arg, objects);
            }
        }
        LogicalPlan::ExportData { query, .. } => collect_objects_from_plan(query, objects),
        LogicalPlan::LoadData { table_name, .. } => {
            objects.insert(table_name.clone());
        }
        LogicalPlan::Declare { default, .. } => {
            if let Some(default) = default {
                collect_objects_from_expr(default, objects);
            }
        }
        LogicalPlan::SetVariable { value, .. } => {
            collect_objects_from_expr(value, objects);
        }
        LogicalPlan::SetMultipleVariables { value, .. } => {
            collect_objects_from_expr(value, objects);
        }
        LogicalPlan::If {
            condition,
            then_branch,
            else_branch,
            ..
        } => {
            collect_objects_from_expr(condition, objects);
            for plan in then_branch {
                collect_objects_from_plan(plan, objects);
            }
            if let Some(else_branch) = else_branch {
                for plan in else_branch {
                    collect_objects_from_plan(plan, objects);
                }
            }
        }
        LogicalPlan::While {
            condition, body, ..
        } => {
            collect_objects_from_expr(condition, objects);
            for plan in body {
                collect_objects_from_plan(plan, objects);
            }
        }
        LogicalPlan::Loop { body, .. } => {
            for plan in body {
                collect_objects_from_plan(plan, objects);
            }
        }
        LogicalPlan::Block { body, .. } => {
            for plan in body {
                collect_objects_from_plan(plan, objects);
            }
        }
        LogicalPlan::Repeat {
            body,
            until_condition,
            ..
        } => {
            for plan in body {
                collect_objects_from_plan(plan, objects);
            }
            collect_objects_from_expr(until_condition, objects);
        }
        LogicalPlan::For { query, body, .. } => {
            collect_objects_from_plan(query, objects);
            for plan in body {
                collect_objects_from_plan(plan, objects);
            }
        }
        LogicalPlan::Return { value, .. } => {
            if let Some(value) = value {
                collect_objects_from_expr(value, objects);
            }
        }
        LogicalPlan::Raise { message, .. } => {
            if let Some(message) = message {
                collect_objects_from_expr(message, objects);
            }
        }
        LogicalPlan::ExecuteImmediate {
            sql_expr,
            using_params,
            ..
        } => {
            collect_objects_from_expr(sql_expr, objects);
            for (expr, _) in using_params {
                collect_objects_from_expr(expr, objects);
            }
        }
        LogicalPlan::Break { .. } => {}
        LogicalPlan::Continue { .. } => {}
        LogicalPlan::CreateSnapshot {
            snapshot_name,
            source_name,
            ..
        } => {
            objects.insert(snapshot_name.clone());
            objects.insert(source_name.clone());
        }
        LogicalPlan::DropSnapshot { snapshot_name, .. } => {
            objects.insert(snapshot_name.clone());
        }
        LogicalPlan::Assert {
            condition, message, ..
        } => {
            collect_objects_from_expr(condition, objects);
            if let Some(message) = message {
                collect_objects_from_expr(message, objects);
            }
        }
        LogicalPlan::Grant { .. } => {}
        LogicalPlan::Revoke { .. } => {}
        LogicalPlan::BeginTransaction => {}
        LogicalPlan::Commit => {}
        LogicalPlan::Rollback => {}
        LogicalPlan::TryCatch {
            try_block,
            catch_block,
        } => {
            for (plan, _) in try_block {
                collect_objects_from_plan(plan, objects);
            }
            for plan in catch_block {
                collect_objects_from_plan(plan, objects);
            }
        }
        LogicalPlan::GapFill {
            input,
            bucket_width,
            origin,
            ..
        } => {
            collect_objects_from_plan(input, objects);
            collect_objects_from_expr(bucket_width, objects);
            if let Some(origin) = origin {
                collect_objects_from_expr(origin, objects);
            }
        }
        LogicalPlan::Explain { input, .. } => collect_objects_from_plan(input, objects),
    }
}

fn collect_objects_from_expr(expr: &Expr, objects: &mut FxHashSet<String>) {
    match expr {
        Expr::ScalarFunction { args, .. } => {
            for arg in args {
                collect_objects_from_expr(arg, objects);
            }
        }
        Expr::Aggregate { args, filter, .. } => {
            for arg in args {
                collect_objects_from_expr(arg, objects);
            }
            if let Some(filter) = filter {
                collect_objects_from_expr(filter, objects);
            }
        }
        Expr::UserDefinedAggregate {
            name, args, filter, ..
        } => {
            objects.insert(name.clone());
            for arg in args {
                collect_objects_from_expr(arg, objects);
            }
            if let Some(filter) = filter {
                collect_objects_from_expr(filter, objects);
            }
        }
        Expr::Window {
            args,
            partition_by,
            order_by,
            ..
        } => {
            for arg in args {
                collect_objects_from_expr(arg, objects);
            }
            for e in partition_by {
                collect_objects_from_expr(e, objects);
            }
            for sort_expr in order_by {
                collect_objects_from_expr(&sort_expr.expr, objects);
            }
        }
        Expr::AggregateWindow {
            args,
            partition_by,
            order_by,
            ..
        } => {
            for arg in args {
                collect_objects_from_expr(arg, objects);
            }
            for e in partition_by {
                collect_objects_from_expr(e, objects);
            }
            for sort_expr in order_by {
                collect_objects_from_expr(&sort_expr.expr, objects);
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_objects_from_expr(left, objects);
            collect_objects_from_expr(right, objects);
        }
        Expr::UnaryOp { expr, .. } => {
            collect_objects_from_expr(expr, objects);
        }
        Expr::Case {
            operand,
            when_clauses,
            else_result,
        } => {
            if let Some(operand) = operand {
                collect_objects_from_expr(operand, objects);
            }
            for clause in when_clauses {
                collect_objects_from_expr(&clause.condition, objects);
                collect_objects_from_expr(&clause.result, objects);
            }
            if let Some(else_result) = else_result {
                collect_objects_from_expr(else_result, objects);
            }
        }
        Expr::Cast { expr, .. } => {
            collect_objects_from_expr(expr, objects);
        }
        Expr::InList { expr, list, .. } => {
            collect_objects_from_expr(expr, objects);
            for item in list {
                collect_objects_from_expr(item, objects);
            }
        }
        Expr::InSubquery { expr, subquery, .. } => {
            collect_objects_from_expr(expr, objects);
            collect_objects_from_plan(subquery, objects);
        }
        Expr::InUnnest {
            expr, array_expr, ..
        } => {
            collect_objects_from_expr(expr, objects);
            collect_objects_from_expr(array_expr, objects);
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_objects_from_expr(expr, objects);
            collect_objects_from_expr(low, objects);
            collect_objects_from_expr(high, objects);
        }
        Expr::Subquery(subquery) => {
            collect_objects_from_plan(subquery, objects);
        }
        Expr::ScalarSubquery(subquery) => {
            collect_objects_from_plan(subquery, objects);
        }
        Expr::ArraySubquery(subquery) => {
            collect_objects_from_plan(subquery, objects);
        }
        Expr::Exists { subquery, .. } => {
            collect_objects_from_plan(subquery, objects);
        }
        Expr::Array { elements, .. } => {
            for elem in elements {
                collect_objects_from_expr(elem, objects);
            }
        }
        Expr::Struct { fields } => {
            for (_, e) in fields {
                collect_objects_from_expr(e, objects);
            }
        }
        Expr::ArrayAccess { array, index } => {
            collect_objects_from_expr(array, objects);
            collect_objects_from_expr(index, objects);
        }
        Expr::StructAccess { expr, .. } => {
            collect_objects_from_expr(expr, objects);
        }
        Expr::Like { expr, pattern, .. } => {
            collect_objects_from_expr(expr, objects);
            collect_objects_from_expr(pattern, objects);
        }
        Expr::IsNull { expr, .. } => {
            collect_objects_from_expr(expr, objects);
        }
        Expr::IsDistinctFrom { left, right, .. } => {
            collect_objects_from_expr(left, objects);
            collect_objects_from_expr(right, objects);
        }
        Expr::Lambda { body, .. } => {
            collect_objects_from_expr(body, objects);
        }
        Expr::Alias { expr, .. } => {
            collect_objects_from_expr(expr, objects);
        }
        Expr::Extract { expr, .. } => {
            collect_objects_from_expr(expr, objects);
        }
        Expr::Substring {
            expr,
            start,
            length,
        } => {
            collect_objects_from_expr(expr, objects);
            if let Some(start) = start {
                collect_objects_from_expr(start, objects);
            }
            if let Some(length) = length {
                collect_objects_from_expr(length, objects);
            }
        }
        Expr::Trim {
            expr, trim_what, ..
        } => {
            collect_objects_from_expr(expr, objects);
            if let Some(trim_what) = trim_what {
                collect_objects_from_expr(trim_what, objects);
            }
        }
        Expr::Position { substr, string } => {
            collect_objects_from_expr(substr, objects);
            collect_objects_from_expr(string, objects);
        }
        Expr::Overlay {
            expr,
            overlay_what,
            overlay_from,
            overlay_for,
        } => {
            collect_objects_from_expr(expr, objects);
            collect_objects_from_expr(overlay_what, objects);
            collect_objects_from_expr(overlay_from, objects);
            if let Some(overlay_for) = overlay_for {
                collect_objects_from_expr(overlay_for, objects);
            }
        }
        Expr::TypedString { .. } => {}
        Expr::Interval { value, .. } => {
            collect_objects_from_expr(value, objects);
        }
        Expr::AtTimeZone {
            timestamp,
            time_zone,
        } => {
            collect_objects_from_expr(timestamp, objects);
            collect_objects_from_expr(time_zone, objects);
        }
        Expr::JsonAccess { expr, .. } => {
            collect_objects_from_expr(expr, objects);
        }
        Expr::Column { .. }
        | Expr::Literal(_)
        | Expr::Wildcard { .. }
        | Expr::Placeholder { .. }
        | Expr::Variable { .. }
        | Expr::Parameter { .. }
        | Expr::Default => {}
    }
}

pub enum CacheInvalidation {
    Objects(Vec<String>),
    All,
}

pub fn get_cache_invalidation(plan: &LogicalPlan) -> CacheInvalidation {
    match plan {
        LogicalPlan::CreateTable { table_name, .. } => {
            CacheInvalidation::Objects(vec![table_name.clone()])
        }
        LogicalPlan::DropTable { table_names, .. } => {
            CacheInvalidation::Objects(table_names.clone())
        }
        LogicalPlan::AlterTable { table_name, .. } => {
            CacheInvalidation::Objects(vec![table_name.clone()])
        }
        LogicalPlan::Truncate { table_name } => {
            CacheInvalidation::Objects(vec![table_name.clone()])
        }
        LogicalPlan::CreateSnapshot { snapshot_name, .. } => {
            CacheInvalidation::Objects(vec![snapshot_name.clone()])
        }
        LogicalPlan::DropSnapshot { snapshot_name, .. } => {
            CacheInvalidation::Objects(vec![snapshot_name.clone()])
        }
        LogicalPlan::CreateView { .. }
        | LogicalPlan::DropView { .. }
        | LogicalPlan::CreateFunction { .. }
        | LogicalPlan::DropFunction { .. }
        | LogicalPlan::CreateProcedure { .. }
        | LogicalPlan::DropProcedure { .. } => CacheInvalidation::All,
        _ => CacheInvalidation::Objects(vec![]),
    }
}
