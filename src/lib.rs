#![feature(coverage_attribute)]
#![coverage(off)]

//! YachtSQL - A SQL database engine (BigQuery dialect).
//!
//! YachtSQL provides an in-memory SQL database with BigQuery dialect support,
//! featuring columnar storage and comprehensive SQL functionality.
//!
//! # Architecture
//!
//! The query processing pipeline is:
//! ```text
//! SQL String → Parser → LogicalPlan → Optimizer → PhysicalPlan → Executor → Result
//! ```
//!
//! The `YachtSQLEngine` creates isolated sessions with their own catalog and state.
//!
//! # Example
//!
//! ```rust,ignore
//! use yachtsql::YachtSQLEngine;
//!
//! #[tokio::main]
//! async fn main() {
//!     let engine = YachtSQLEngine::new();
//!     let session = engine.create_session();
//!
//!     // Create a table
//!     session
//!         .execute_sql("CREATE TABLE users (id INT64, name STRING)")
//!         .await
//!         .unwrap();
//!
//!     // Insert data
//!     session
//!         .execute_sql("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
//!         .await
//!         .unwrap();
//!
//!     // Query data
//!     let result = session
//!         .execute_sql("SELECT * FROM users WHERE id = 1")
//!         .await
//!         .unwrap();
//! }
//! ```

pub use yachtsql_common::error::{Error, Result};
pub use yachtsql_common::result::{ColumnInfo, QueryResult, Row};
pub use yachtsql_common::types::{DataType, Value};
pub use yachtsql_executor::{
    AsyncQueryExecutor, ConcurrentCatalog, ConcurrentSession, Record, Table,
};
pub use yachtsql_ir::LogicalPlan;
pub use yachtsql_optimizer::OptimizedLogicalPlan;
pub use yachtsql_parser::{CatalogProvider, Planner, parse_and_plan, parse_sql};
pub use yachtsql_storage::{Field, FieldMode, Schema};

/// Factory for creating isolated SQL sessions.
///
/// `YachtSQLEngine` is lightweight and can be shared across threads. Each session
/// created from an engine has its own isolated catalog (tables, views, functions)
/// but shares the global query plan cache for better performance.
///
/// # Example
///
/// ```rust,ignore
/// let engine = YachtSQLEngine::new();
/// let session = engine.create_session();
/// ```
pub struct YachtSQLEngine;

impl YachtSQLEngine {
    /// Creates a new engine instance.
    pub fn new() -> Self {
        Self
    }

    /// Creates a new isolated session.
    ///
    /// Each session has its own catalog, meaning tables created in one session
    /// are not visible to other sessions. This is useful for parallel test execution.
    pub fn create_session(&self) -> YachtSQLSession {
        YachtSQLSession {
            executor: AsyncQueryExecutor::new(),
        }
    }
}

impl Default for YachtSQLEngine {
    fn default() -> Self {
        Self::new()
    }
}

/// An isolated SQL execution session.
///
/// A session provides a sandboxed environment for SQL execution with its own:
/// - **Catalog**: Tables, views, and user-defined functions
/// - **Session state**: Variables, current schema, and transaction context
///
/// Sessions are not `Send` or `Sync`. Create one session per task/thread.
///
/// # Query Methods
///
/// The session provides three methods for executing SQL, each returning different result types:
///
/// | Method | Returns | Use Case |
/// |--------|---------|----------|
/// | [`execute_sql`](Self::execute_sql) | `Table` | Low-level access to columnar data |
/// | [`query`](Self::query) | `QueryResult` | Row-based results, easy to serialize |
/// | [`run`](Self::run) | `u64` | DDL/DML statements where you only need the row count |
///
/// # Example
///
/// ```rust,ignore
/// let session = engine.create_session();
///
/// // Create and populate a table
/// session.execute_sql("CREATE TABLE users (id INT64, name STRING)").await?;
/// session.execute_sql("INSERT INTO users VALUES (1, 'Alice')").await?;
///
/// // Query with row-based results
/// let result = session.query("SELECT * FROM users").await?;
/// for row in &result.rows {
///     println!("{:?}", row);
/// }
/// ```
pub struct YachtSQLSession {
    executor: AsyncQueryExecutor,
}

impl YachtSQLSession {
    /// Creates a new session with an empty catalog.
    pub fn new() -> Self {
        Self {
            executor: AsyncQueryExecutor::new(),
        }
    }

    /// Executes SQL and returns the result as a [`Table`].
    ///
    /// This is the lowest-level query method, returning data in columnar format.
    /// Use this when you need direct access to columns or are building data pipelines.
    ///
    /// For row-based access, prefer [`query`](Self::query) instead.
    pub async fn execute_sql(&self, sql: &str) -> Result<Table> {
        self.executor.execute_sql(sql).await
    }

    /// Executes SQL and returns the result as a [`QueryResult`].
    ///
    /// Returns data in row-based format with schema information, suitable for
    /// serialization (e.g., to JSON) or iteration over rows.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let result = session.query("SELECT id, name FROM users").await?;
    /// for row in &result.rows {
    ///     let id = row.get(0);
    ///     let name = row.get(1);
    /// }
    /// ```
    pub async fn query(&self, sql: &str) -> Result<QueryResult> {
        let table = self.executor.execute_sql(sql).await?;
        table.to_query_result()
    }

    /// Executes SQL and returns the number of affected rows.
    ///
    /// Use this for DDL statements (`CREATE`, `DROP`) or DML statements
    /// (`INSERT`, `UPDATE`, `DELETE`) where you only need the row count.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let rows_inserted = session.run("INSERT INTO users VALUES (1, 'Alice')").await?;
    /// assert_eq!(rows_inserted, 1);
    /// ```
    pub async fn run(&self, sql: &str) -> Result<u64> {
        let table = self.executor.execute_sql(sql).await?;
        Ok(table.row_count() as u64)
    }

    /// Returns a reference to the session state.
    ///
    /// Use this to access or modify session variables and settings.
    pub fn session(&self) -> &ConcurrentSession {
        self.executor.session()
    }

    /// Returns a reference to the catalog.
    ///
    /// Use this for direct catalog operations like checking if a table exists,
    /// listing tables, or managing schemas programmatically.
    pub fn catalog(&self) -> &ConcurrentCatalog {
        self.executor.catalog()
    }

    /// Sets the default project for unqualified table references.
    ///
    /// In BigQuery SQL, tables can be referenced as `project.dataset.table`.
    /// Setting a default project allows you to omit the project prefix.
    pub fn set_default_project(&self, project: Option<String>) {
        self.executor.catalog().set_default_project(project);
    }

    /// Returns the current default project, if set.
    pub fn get_default_project(&self) -> Option<String> {
        self.executor.catalog().get_default_project()
    }

    /// Returns a list of all projects in the catalog.
    pub fn get_projects(&self) -> Vec<String> {
        self.executor.catalog().get_projects()
    }

    /// Returns a list of all datasets in the specified project.
    pub fn get_datasets(&self, project: &str) -> Vec<String> {
        self.executor.catalog().get_datasets(project)
    }

    /// Returns a list of all tables in the specified dataset.
    pub fn get_tables_in_dataset(&self, project: &str, dataset: &str) -> Vec<String> {
        self.executor
            .catalog()
            .get_tables_in_dataset(project, dataset)
    }
}

impl Default for YachtSQLSession {
    fn default() -> Self {
        Self::new()
    }
}
