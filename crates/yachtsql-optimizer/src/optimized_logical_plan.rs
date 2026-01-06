#![coverage(off)]

use serde::{Deserialize, Serialize};
use yachtsql_common::types::DataType;
pub use yachtsql_ir::SampleType;
use yachtsql_ir::{
    AlterTableOp, Assignment, ColumnDef, CteDefinition, DclResourceType, ExportOptions, Expr,
    FunctionArg, FunctionBody, GapFillColumn, JoinType, LoadOptions, MergeClause, PlanSchema,
    ProcedureArg, RaiseLevel, ScalarFunction, SortExpr, UnnestColumn,
};

pub const PARALLEL_ROW_THRESHOLD: u64 = 1000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BoundType {
    Compute,
    Memory,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionHints {
    pub parallel: bool,
    pub bound_type: BoundType,
    pub estimated_rows: u64,
}

impl Default for ExecutionHints {
    fn default() -> Self {
        Self {
            parallel: false,
            bound_type: BoundType::Compute,
            estimated_rows: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PhysicalPlan {
    TableScan {
        table_name: String,
        schema: PlanSchema,
        projection: Option<Vec<usize>>,
        #[serde(default)]
        row_count: Option<u64>,
    },

    Sample {
        input: Box<PhysicalPlan>,
        sample_type: SampleType,
        sample_value: i64,
    },

    Filter {
        input: Box<PhysicalPlan>,
        predicate: Expr,
    },

    Project {
        input: Box<PhysicalPlan>,
        expressions: Vec<Expr>,
        schema: PlanSchema,
    },

    NestedLoopJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        join_type: JoinType,
        condition: Option<Expr>,
        schema: PlanSchema,
        #[serde(default)]
        parallel: bool,
        #[serde(default)]
        hints: ExecutionHints,
    },

    CrossJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        schema: PlanSchema,
        #[serde(default)]
        parallel: bool,
        #[serde(default)]
        hints: ExecutionHints,
    },

    HashJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        join_type: JoinType,
        left_keys: Vec<Expr>,
        right_keys: Vec<Expr>,
        schema: PlanSchema,
        #[serde(default)]
        parallel: bool,
        #[serde(default)]
        hints: ExecutionHints,
    },

    HashAggregate {
        input: Box<PhysicalPlan>,
        group_by: Vec<Expr>,
        aggregates: Vec<Expr>,
        schema: PlanSchema,
        grouping_sets: Option<Vec<Vec<usize>>>,
        #[serde(default)]
        hints: ExecutionHints,
    },

    Sort {
        input: Box<PhysicalPlan>,
        sort_exprs: Vec<SortExpr>,
        #[serde(default)]
        hints: ExecutionHints,
    },

    Limit {
        input: Box<PhysicalPlan>,
        limit: Option<usize>,
        offset: Option<usize>,
    },

    TopN {
        input: Box<PhysicalPlan>,
        sort_exprs: Vec<SortExpr>,
        limit: usize,
    },

    Distinct {
        input: Box<PhysicalPlan>,
    },

    Union {
        inputs: Vec<PhysicalPlan>,
        all: bool,
        schema: PlanSchema,
        #[serde(default)]
        parallel: bool,
        #[serde(default)]
        hints: ExecutionHints,
    },

    Intersect {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        all: bool,
        schema: PlanSchema,
        #[serde(default)]
        parallel: bool,
        #[serde(default)]
        hints: ExecutionHints,
    },

    Except {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        all: bool,
        schema: PlanSchema,
        #[serde(default)]
        parallel: bool,
        #[serde(default)]
        hints: ExecutionHints,
    },

    Window {
        input: Box<PhysicalPlan>,
        window_exprs: Vec<Expr>,
        schema: PlanSchema,
        #[serde(default)]
        hints: ExecutionHints,
    },

    Unnest {
        input: Box<PhysicalPlan>,
        columns: Vec<UnnestColumn>,
        schema: PlanSchema,
    },

    Qualify {
        input: Box<PhysicalPlan>,
        predicate: Expr,
    },

    WithCte {
        ctes: Vec<CteDefinition>,
        body: Box<PhysicalPlan>,
        #[serde(default)]
        parallel_ctes: Vec<usize>,
        #[serde(default)]
        hints: ExecutionHints,
    },

    Values {
        values: Vec<Vec<Expr>>,
        schema: PlanSchema,
    },

    Empty {
        schema: PlanSchema,
    },

    Insert {
        table_name: String,
        columns: Vec<String>,
        source: Box<PhysicalPlan>,
    },

    Update {
        table_name: String,
        alias: Option<String>,
        assignments: Vec<Assignment>,
        from: Option<Box<PhysicalPlan>>,
        filter: Option<Expr>,
    },

    Delete {
        table_name: String,
        alias: Option<String>,
        filter: Option<Expr>,
    },

    Merge {
        target_table: String,
        source: Box<PhysicalPlan>,
        on: Expr,
        clauses: Vec<MergeClause>,
    },

    CreateTable {
        table_name: String,
        columns: Vec<ColumnDef>,
        if_not_exists: bool,
        or_replace: bool,
        query: Option<Box<PhysicalPlan>>,
    },

    DropTable {
        table_names: Vec<String>,
        if_exists: bool,
    },

    AlterTable {
        table_name: String,
        operation: AlterTableOp,
        if_exists: bool,
    },

    Truncate {
        table_name: String,
    },

    CreateView {
        name: String,
        query: Box<PhysicalPlan>,
        query_sql: String,
        column_aliases: Vec<String>,
        or_replace: bool,
        if_not_exists: bool,
    },

    DropView {
        name: String,
        if_exists: bool,
    },

    CreateSchema {
        name: String,
        if_not_exists: bool,
        or_replace: bool,
    },

    DropSchema {
        name: String,
        if_exists: bool,
        cascade: bool,
    },

    UndropSchema {
        name: String,
        if_not_exists: bool,
    },

    AlterSchema {
        name: String,
        options: Vec<(String, String)>,
    },

    CreateFunction {
        name: String,
        args: Vec<FunctionArg>,
        return_type: DataType,
        body: FunctionBody,
        or_replace: bool,
        if_not_exists: bool,
        is_temp: bool,
        is_aggregate: bool,
    },

    DropFunction {
        name: String,
        if_exists: bool,
    },

    CreateProcedure {
        name: String,
        args: Vec<ProcedureArg>,
        body: Vec<PhysicalPlan>,
        or_replace: bool,
        if_not_exists: bool,
    },

    DropProcedure {
        name: String,
        if_exists: bool,
    },

    Call {
        procedure_name: String,
        args: Vec<Expr>,
    },

    ExportData {
        options: ExportOptions,
        query: Box<PhysicalPlan>,
    },

    LoadData {
        table_name: String,
        options: LoadOptions,
        temp_table: bool,
        temp_schema: Option<Vec<ColumnDef>>,
    },

    Declare {
        name: String,
        data_type: DataType,
        default: Option<Expr>,
    },

    SetVariable {
        name: String,
        value: Expr,
    },

    SetMultipleVariables {
        names: Vec<String>,
        value: Expr,
    },

    If {
        condition: Expr,
        then_branch: Vec<PhysicalPlan>,
        else_branch: Option<Vec<PhysicalPlan>>,
    },

    While {
        condition: Expr,
        body: Vec<PhysicalPlan>,
        label: Option<String>,
    },

    Loop {
        body: Vec<PhysicalPlan>,
        label: Option<String>,
    },

    Block {
        body: Vec<PhysicalPlan>,
        label: Option<String>,
    },

    Repeat {
        body: Vec<PhysicalPlan>,
        until_condition: Expr,
    },

    For {
        variable: String,
        query: Box<PhysicalPlan>,
        body: Vec<PhysicalPlan>,
    },

    Return {
        value: Option<Expr>,
    },

    Raise {
        message: Option<Expr>,
        level: RaiseLevel,
    },

    ExecuteImmediate {
        sql_expr: Expr,
        into_variables: Vec<String>,
        using_params: Vec<(Expr, Option<String>)>,
    },

    Break {
        label: Option<String>,
    },

    Continue {
        label: Option<String>,
    },

    CreateSnapshot {
        snapshot_name: String,
        source_name: String,
        if_not_exists: bool,
    },

    DropSnapshot {
        snapshot_name: String,
        if_exists: bool,
    },

    Assert {
        condition: Expr,
        message: Option<Expr>,
    },

    Grant {
        roles: Vec<String>,
        resource_type: DclResourceType,
        resource_name: String,
        grantees: Vec<String>,
    },

    Revoke {
        roles: Vec<String>,
        resource_type: DclResourceType,
        resource_name: String,
        grantees: Vec<String>,
    },

    BeginTransaction,

    Commit,

    Rollback,

    TryCatch {
        try_block: Vec<(PhysicalPlan, Option<String>)>,
        catch_block: Vec<PhysicalPlan>,
    },

    GapFill {
        input: Box<PhysicalPlan>,
        ts_column: String,
        bucket_width: Expr,
        value_columns: Vec<GapFillColumn>,
        partitioning_columns: Vec<String>,
        origin: Option<Expr>,
        input_schema: PlanSchema,
        schema: PlanSchema,
    },

    Explain {
        input: Box<PhysicalPlan>,
        analyze: bool,
        logical_plan_text: String,
        #[serde(default)]
        physical_plan_text: String,
    },
}

impl PhysicalPlan {
    pub fn schema(&self) -> &PlanSchema {
        use yachtsql_ir::EMPTY_SCHEMA;
        match self {
            PhysicalPlan::TableScan { schema, .. } => schema,
            PhysicalPlan::Sample { input, .. } => input.schema(),
            PhysicalPlan::Filter { input, .. } => input.schema(),
            PhysicalPlan::Project { schema, .. } => schema,
            PhysicalPlan::NestedLoopJoin { schema, .. } => schema,
            PhysicalPlan::CrossJoin { schema, .. } => schema,
            PhysicalPlan::HashJoin { schema, .. } => schema,
            PhysicalPlan::HashAggregate { schema, .. } => schema,
            PhysicalPlan::Sort { input, .. } => input.schema(),
            PhysicalPlan::Limit { input, .. } => input.schema(),
            PhysicalPlan::TopN { input, .. } => input.schema(),
            PhysicalPlan::Distinct { input } => input.schema(),
            PhysicalPlan::Union { schema, .. } => schema,
            PhysicalPlan::Intersect { schema, .. } => schema,
            PhysicalPlan::Except { schema, .. } => schema,
            PhysicalPlan::Window { schema, .. } => schema,
            PhysicalPlan::Unnest { schema, .. } => schema,
            PhysicalPlan::Qualify { input, .. } => input.schema(),
            PhysicalPlan::WithCte { body, .. } => body.schema(),
            PhysicalPlan::Values { schema, .. } => schema,
            PhysicalPlan::Empty { schema } => schema,
            PhysicalPlan::Insert { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::Update { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::Delete { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::Merge { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::CreateTable { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::DropTable { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::AlterTable { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::Truncate { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::CreateView { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::DropView { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::CreateSchema { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::DropSchema { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::UndropSchema { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::AlterSchema { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::CreateFunction { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::DropFunction { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::CreateProcedure { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::DropProcedure { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::Call { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::ExportData { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::LoadData { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::Declare { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::SetVariable { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::SetMultipleVariables { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::If { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::While { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::Loop { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::Block { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::Repeat { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::For { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::Return { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::Raise { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::ExecuteImmediate { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::Break { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::Continue { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::CreateSnapshot { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::DropSnapshot { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::Assert { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::Grant { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::Revoke { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::BeginTransaction => &EMPTY_SCHEMA,
            PhysicalPlan::Commit => &EMPTY_SCHEMA,
            PhysicalPlan::Rollback => &EMPTY_SCHEMA,
            PhysicalPlan::TryCatch { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::GapFill { schema, .. } => schema,
            PhysicalPlan::Explain { .. } => &EMPTY_SCHEMA,
        }
    }

    pub fn estimate_rows(&self) -> u64 {
        match self {
            PhysicalPlan::TableScan { row_count, .. } => row_count.unwrap_or(1000),
            PhysicalPlan::Values { values, .. } => values.len() as u64,
            PhysicalPlan::Empty { .. } => 0,
            PhysicalPlan::Filter { input, .. } => {
                let input_rows = input.estimate_rows();
                std::cmp::max(1, (input_rows as f64 * 0.33) as u64)
            }
            PhysicalPlan::Project { input, .. } => input.estimate_rows(),
            PhysicalPlan::Sample { sample_value, .. } => *sample_value as u64,
            PhysicalPlan::NestedLoopJoin { left, right, .. } => {
                left.estimate_rows().saturating_mul(right.estimate_rows())
            }
            PhysicalPlan::HashJoin { left, right, .. } => {
                let left_rows = left.estimate_rows();
                let right_rows = right.estimate_rows();
                let max_rows = std::cmp::max(left_rows, right_rows);
                if max_rows == 0 {
                    0
                } else {
                    left_rows.saturating_mul(right_rows) / max_rows
                }
            }
            PhysicalPlan::CrossJoin { left, right, .. } => {
                left.estimate_rows().saturating_mul(right.estimate_rows())
            }
            PhysicalPlan::HashAggregate {
                input, group_by, ..
            } => {
                if group_by.is_empty() {
                    1
                } else {
                    std::cmp::max(1, input.estimate_rows() / 10)
                }
            }
            PhysicalPlan::Sort { input, .. } => input.estimate_rows(),
            PhysicalPlan::Limit { limit, input, .. } => {
                std::cmp::min(limit.unwrap_or(usize::MAX) as u64, input.estimate_rows())
            }
            PhysicalPlan::TopN { limit, input, .. } => {
                std::cmp::min(*limit as u64, input.estimate_rows())
            }
            PhysicalPlan::Distinct { input } => std::cmp::max(1, input.estimate_rows() / 2),
            PhysicalPlan::Union { inputs, .. } => inputs.iter().map(|p| p.estimate_rows()).sum(),
            PhysicalPlan::Intersect { left, right, .. } => {
                std::cmp::min(left.estimate_rows(), right.estimate_rows())
            }
            PhysicalPlan::Except { left, .. } => left.estimate_rows(),
            PhysicalPlan::Window { input, .. } => input.estimate_rows(),
            PhysicalPlan::Unnest { input, .. } => input.estimate_rows().saturating_mul(10),
            PhysicalPlan::Qualify { input, .. } => {
                std::cmp::max(1, (input.estimate_rows() as f64 * 0.33) as u64)
            }
            PhysicalPlan::WithCte { body, .. } => body.estimate_rows(),
            PhysicalPlan::GapFill { input, .. } => input.estimate_rows().saturating_mul(2),
            _ => 1,
        }
    }

    pub fn bound_type(&self) -> BoundType {
        match self {
            PhysicalPlan::TableScan { .. }
            | PhysicalPlan::Values { .. }
            | PhysicalPlan::Empty { .. } => BoundType::Memory,

            PhysicalPlan::Limit { input, .. } | PhysicalPlan::Sample { input, .. } => {
                input.bound_type()
            }

            PhysicalPlan::Filter { input, predicate } => {
                if input.bound_type() == BoundType::Memory && !Self::is_expensive_expr(predicate) {
                    BoundType::Memory
                } else {
                    BoundType::Compute
                }
            }

            PhysicalPlan::Project {
                input, expressions, ..
            } => {
                if input.bound_type() == BoundType::Memory
                    && expressions.iter().all(|e| !Self::is_expensive_expr(e))
                {
                    BoundType::Memory
                } else {
                    BoundType::Compute
                }
            }

            PhysicalPlan::Distinct { input } => input.bound_type(),

            PhysicalPlan::Sort { .. }
            | PhysicalPlan::TopN { .. }
            | PhysicalPlan::HashAggregate { .. }
            | PhysicalPlan::Window { .. }
            | PhysicalPlan::NestedLoopJoin { .. }
            | PhysicalPlan::HashJoin { .. }
            | PhysicalPlan::CrossJoin { .. }
            | PhysicalPlan::Union { .. }
            | PhysicalPlan::Intersect { .. }
            | PhysicalPlan::Except { .. }
            | PhysicalPlan::Unnest { .. }
            | PhysicalPlan::Qualify { .. }
            | PhysicalPlan::GapFill { .. }
            | PhysicalPlan::Merge { .. } => BoundType::Compute,

            _ => BoundType::Compute,
        }
    }

    pub fn should_parallelize(left: &Self, right: &Self) -> bool {
        left.estimate_rows() >= PARALLEL_ROW_THRESHOLD
            && right.estimate_rows() >= PARALLEL_ROW_THRESHOLD
    }

    pub fn should_parallelize_union(inputs: &[Self]) -> bool {
        inputs.len() >= 2
            && inputs
                .iter()
                .filter(|p| p.estimate_rows() >= PARALLEL_ROW_THRESHOLD)
                .count()
                >= 2
    }

    fn is_expensive_expr(expr: &Expr) -> bool {
        match expr {
            Expr::ScalarFunction { name, args } => {
                let expensive = matches!(
                    name,
                    ScalarFunction::RegexpContains
                        | ScalarFunction::RegexpExtract
                        | ScalarFunction::RegexpExtractAll
                        | ScalarFunction::RegexpInstr
                        | ScalarFunction::RegexpReplace
                        | ScalarFunction::RegexpSubstr
                        | ScalarFunction::JsonExtract
                        | ScalarFunction::JsonExtractScalar
                        | ScalarFunction::JsonExtractArray
                        | ScalarFunction::JsonValue
                        | ScalarFunction::JsonQuery
                        | ScalarFunction::ParseJson
                        | ScalarFunction::ToJson
                        | ScalarFunction::ToJsonString
                        | ScalarFunction::Sqrt
                        | ScalarFunction::Power
                        | ScalarFunction::Pow
                        | ScalarFunction::Log
                        | ScalarFunction::Log10
                        | ScalarFunction::Exp
                        | ScalarFunction::Sin
                        | ScalarFunction::Cos
                        | ScalarFunction::Tan
                        | ScalarFunction::Asin
                        | ScalarFunction::Acos
                        | ScalarFunction::Atan
                        | ScalarFunction::Atan2
                        | ScalarFunction::Sinh
                        | ScalarFunction::Cosh
                        | ScalarFunction::Tanh
                        | ScalarFunction::Md5
                        | ScalarFunction::Sha1
                        | ScalarFunction::Sha256
                        | ScalarFunction::Sha512
                );
                expensive || args.iter().any(Self::is_expensive_expr)
            }
            Expr::Case {
                operand,
                when_clauses,
                else_result,
            } => {
                operand.as_ref().is_some_and(|e| Self::is_expensive_expr(e))
                    || when_clauses.iter().any(|wc| {
                        Self::is_expensive_expr(&wc.condition)
                            || Self::is_expensive_expr(&wc.result)
                    })
                    || else_result
                        .as_ref()
                        .is_some_and(|e| Self::is_expensive_expr(e))
            }
            Expr::Subquery(_) | Expr::ScalarSubquery(_) | Expr::ArraySubquery(_) => true,
            Expr::BinaryOp { left, right, .. } => {
                Self::is_expensive_expr(left) || Self::is_expensive_expr(right)
            }
            Expr::UnaryOp { expr, .. } => Self::is_expensive_expr(expr),
            Expr::Like { expr, pattern, .. } => {
                Self::is_expensive_expr(expr) || Self::is_expensive_expr(pattern)
            }
            Expr::InList { expr, list, .. } => {
                Self::is_expensive_expr(expr) || list.iter().any(Self::is_expensive_expr)
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                Self::is_expensive_expr(expr)
                    || Self::is_expensive_expr(low)
                    || Self::is_expensive_expr(high)
            }
            Expr::Cast { expr, .. } => Self::is_expensive_expr(expr),
            _ => false,
        }
    }
}
