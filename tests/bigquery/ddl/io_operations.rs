use std::fs;
use std::io::Write;
use std::sync::Arc;

use arrow::array::{
    BooleanBuilder, Date32Builder, Float64Builder, Int64Builder, StringBuilder,
    TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_writer::ArrowWriter;
use tempfile::{NamedTempFile, TempDir};
use yachtsql::RecordBatchVecExt;
use yachtsql_test_utils::{get_f64, get_i64, get_string, is_null};

use crate::assert_table_eq;
use crate::common::create_session;

fn create_test_parquet() -> NamedTempFile {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int64, false),
        ArrowField::new("name", ArrowDataType::Utf8, true),
        ArrowField::new("value", ArrowDataType::Float64, true),
    ]));

    let mut id_builder = Int64Builder::new();
    let mut name_builder = StringBuilder::new();
    let mut value_builder = Float64Builder::new();

    id_builder.append_value(1);
    name_builder.append_value("Alice");
    value_builder.append_value(100.5);

    id_builder.append_value(2);
    name_builder.append_value("Bob");
    value_builder.append_value(200.75);

    id_builder.append_value(3);
    name_builder.append_null();
    value_builder.append_null();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(id_builder.finish()),
            Arc::new(name_builder.finish()),
            Arc::new(value_builder.finish()),
        ],
    )
    .unwrap();

    let temp_file = NamedTempFile::new().unwrap();
    {
        let file = temp_file.reopen().unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }
    temp_file
}

fn create_parquet_with_all_types() -> NamedTempFile {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int64, false),
        ArrowField::new("active", ArrowDataType::Boolean, true),
        ArrowField::new("score", ArrowDataType::Float64, true),
        ArrowField::new("name", ArrowDataType::Utf8, true),
        ArrowField::new("created_date", ArrowDataType::Date32, true),
        ArrowField::new(
            "updated_at",
            ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
            true,
        ),
    ]));

    let mut id_builder = Int64Builder::new();
    let mut active_builder = BooleanBuilder::new();
    let mut score_builder = Float64Builder::new();
    let mut name_builder = StringBuilder::new();
    let mut date_builder = Date32Builder::new();
    let mut ts_builder = TimestampMicrosecondBuilder::new();

    id_builder.append_value(1);
    active_builder.append_value(true);
    score_builder.append_value(95.5);
    name_builder.append_value("Test User");
    date_builder.append_value(19724);
    ts_builder.append_value(1704067200000000);

    id_builder.append_value(2);
    active_builder.append_value(false);
    score_builder.append_null();
    name_builder.append_null();
    date_builder.append_null();
    ts_builder.append_value(1704153600000000);

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(id_builder.finish()),
            Arc::new(active_builder.finish()),
            Arc::new(score_builder.finish()),
            Arc::new(name_builder.finish()),
            Arc::new(date_builder.finish()),
            Arc::new(ts_builder.finish()),
        ],
    )
    .unwrap();

    let temp_file = NamedTempFile::new().unwrap();
    {
        let file = temp_file.reopen().unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }
    temp_file
}

fn create_test_json_file() -> NamedTempFile {
    let temp_file = NamedTempFile::new().unwrap();
    {
        let mut file = temp_file.reopen().unwrap();
        writeln!(file, r#"{{"id": 1, "name": "Alice", "value": 100.5}}"#).unwrap();
        writeln!(file, r#"{{"id": 2, "name": "Bob", "value": 200.75}}"#).unwrap();
        writeln!(file, r#"{{"id": 3, "name": null, "value": null}}"#).unwrap();
    }
    temp_file
}

fn create_test_csv_file() -> NamedTempFile {
    let temp_file = NamedTempFile::new().unwrap();
    {
        let mut file = temp_file.reopen().unwrap();
        writeln!(file, "1,Alice,100.5").unwrap();
        writeln!(file, "2,Bob,200.75").unwrap();
        writeln!(file, "3,,").unwrap();
    }
    temp_file
}

fn create_csv_with_header() -> NamedTempFile {
    let temp_file = NamedTempFile::new().unwrap();
    {
        let mut file = temp_file.reopen().unwrap();
        writeln!(file, "id,name,value").unwrap();
        writeln!(file, "1,Alice,100.5").unwrap();
        writeln!(file, "2,Bob,200.75").unwrap();
    }
    temp_file
}

fn create_csv_with_delimiter() -> NamedTempFile {
    let temp_file = NamedTempFile::new().unwrap();
    {
        let mut file = temp_file.reopen().unwrap();
        writeln!(file, "1|Alice|100.5").unwrap();
        writeln!(file, "2|Bob|200.75").unwrap();
    }
    temp_file
}

fn create_csv_with_tab_delimiter() -> NamedTempFile {
    let temp_file = NamedTempFile::new().unwrap();
    {
        let mut file = temp_file.reopen().unwrap();
        writeln!(file, "1\tAlice\t100.5").unwrap();
        writeln!(file, "2\tBob\t200.75").unwrap();
    }
    temp_file
}

fn create_csv_with_quoted_fields() -> NamedTempFile {
    let temp_file = NamedTempFile::new().unwrap();
    {
        let mut file = temp_file.reopen().unwrap();
        writeln!(file, r#"1,"Alice, Jr.",100.5"#).unwrap();
        writeln!(file, r#"2,"Bob ""The Builder""",200.75"#).unwrap();
    }
    temp_file
}

fn create_csv_with_null_marker() -> NamedTempFile {
    let temp_file = NamedTempFile::new().unwrap();
    {
        let mut file = temp_file.reopen().unwrap();
        writeln!(file, "1,Alice,100.5").unwrap();
        writeln!(file, "2,NULL,200.75").unwrap();
        writeln!(file, "3,Charlie,NULL").unwrap();
    }
    temp_file
}

#[tokio::test(flavor = "current_thread")]
async fn test_export_data_to_local_parquet() {
    let session = create_session();
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("output.parquet");

    session
        .execute_sql("CREATE TABLE export_local (id INT64, name STRING, value FLOAT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO export_local VALUES (1, 'Alice', 100.5), (2, 'Bob', 200.75)")
        .await
        .unwrap();

    let export_sql = format!(
        "EXPORT DATA OPTIONS(uri='file://{}', format='PARQUET') AS SELECT * FROM export_local",
        output_path.display()
    );
    session.execute_sql(&export_sql).await.unwrap();

    assert!(output_path.exists());
}

#[tokio::test(flavor = "current_thread")]
async fn test_export_data_to_local_json() {
    let session = create_session();
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("output.json");

    session
        .execute_sql("CREATE TABLE export_json_local (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO export_json_local VALUES (1, 'Alice'), (2, 'Bob')")
        .await
        .unwrap();

    let export_sql = format!(
        "EXPORT DATA OPTIONS(uri='file://{}', format='JSON') AS SELECT * FROM export_json_local",
        output_path.display()
    );
    session.execute_sql(&export_sql).await.unwrap();

    assert!(output_path.exists());
    let contents = fs::read_to_string(&output_path).unwrap();
    assert!(contents.contains("Alice"));
    assert!(contents.contains("Bob"));
}

#[tokio::test(flavor = "current_thread")]
async fn test_export_data_to_local_csv() {
    let session = create_session();
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("output.csv");

    session
        .execute_sql("CREATE TABLE export_csv_local (id INT64, name STRING, value FLOAT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO export_csv_local VALUES (1, 'Alice', 100.5), (2, 'Bob', 200.75)")
        .await
        .unwrap();

    let export_sql = format!(
        "EXPORT DATA OPTIONS(uri='file://{}', format='CSV') AS SELECT * FROM export_csv_local",
        output_path.display()
    );
    session.execute_sql(&export_sql).await.unwrap();

    assert!(output_path.exists());
}

#[tokio::test(flavor = "current_thread")]
async fn test_export_data_csv_with_header() {
    let session = create_session();
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("output_header.csv");

    session
        .execute_sql("CREATE TABLE export_csv_header (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO export_csv_header VALUES (1, 'Alice'), (2, 'Bob')")
        .await
        .unwrap();

    let export_sql = format!(
        "EXPORT DATA OPTIONS(uri='file://{}', format='CSV', header=true) AS SELECT * FROM export_csv_header",
        output_path.display()
    );
    session.execute_sql(&export_sql).await.unwrap();

    assert!(output_path.exists());
    let contents = fs::read_to_string(&output_path).unwrap();
    assert!(contents.lines().next().unwrap().contains("id"));
}

#[tokio::test(flavor = "current_thread")]
async fn test_export_data_csv_with_delimiter() {
    let session = create_session();
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("output_delim.csv");

    session
        .execute_sql("CREATE TABLE export_csv_delim (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO export_csv_delim VALUES (1, 'Alice'), (2, 'Bob')")
        .await
        .unwrap();

    let export_sql = format!(
        "EXPORT DATA OPTIONS(uri='file://{}', format='CSV', field_delimiter='|') AS SELECT * FROM export_csv_delim",
        output_path.display()
    );
    session.execute_sql(&export_sql).await.unwrap();

    assert!(output_path.exists());
    let contents = fs::read_to_string(&output_path).unwrap();
    assert!(contents.contains("|"));
}

#[tokio::test(flavor = "current_thread")]
async fn test_export_data_with_null_values() {
    let session = create_session();
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("output_nulls.json");

    session
        .execute_sql("CREATE TABLE export_nulls (id INT64, name STRING, value FLOAT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO export_nulls VALUES (1, 'Alice', 100.5), (2, NULL, NULL)")
        .await
        .unwrap();

    let export_sql = format!(
        "EXPORT DATA OPTIONS(uri='file://{}', format='JSON') AS SELECT * FROM export_nulls",
        output_path.display()
    );
    session.execute_sql(&export_sql).await.unwrap();

    assert!(output_path.exists());
    let contents = fs::read_to_string(&output_path).unwrap();
    assert!(contents.contains("null"));
}

#[tokio::test(flavor = "current_thread")]
async fn test_export_data_with_all_types() {
    let session = create_session();
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("all_types.parquet");

    session
        .execute_sql(
            "CREATE TABLE export_all_types (
                col_bool BOOL,
                col_int64 INT64,
                col_float64 FLOAT64,
                col_string STRING,
                col_date DATE,
                col_datetime DATETIME
            )",
        )
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO export_all_types VALUES (
                true, 42, 3.14, 'hello',
                DATE '2024-01-15',
                DATETIME '2024-01-15 10:30:00'
            )",
        )
        .await
        .unwrap();

    let export_sql = format!(
        "EXPORT DATA OPTIONS(uri='file://{}', format='PARQUET') AS SELECT * FROM export_all_types",
        output_path.display()
    );
    session.execute_sql(&export_sql).await.unwrap();

    assert!(output_path.exists());
}

#[tokio::test(flavor = "current_thread")]
async fn test_export_data_to_cloud_uri_no_local_file() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE export_cloud (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO export_cloud VALUES (1)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "EXPORT DATA OPTIONS(uri='gs://bucket/export/*.parquet', format='PARQUET') AS SELECT * FROM export_cloud",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_export_data_avro_unsupported() {
    let session = create_session();
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("output.avro");

    session
        .execute_sql("CREATE TABLE export_avro (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO export_avro VALUES (1)")
        .await
        .unwrap();

    let export_sql = format!(
        "EXPORT DATA OPTIONS(uri='file://{}', format='AVRO') AS SELECT * FROM export_avro",
        output_path.display()
    );
    let result = session.execute_sql(&export_sql).await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_load_data_from_parquet() {
    let session = create_session();
    let temp_file = create_test_parquet();
    let path = temp_file.path().to_str().unwrap();

    session
        .execute_sql("CREATE TABLE load_parquet (id INT64, name STRING, value FLOAT64)")
        .await
        .unwrap();

    let load_sql = format!(
        "LOAD DATA INTO load_parquet FROM FILES (FORMAT='PARQUET', URIS=['{}'])",
        path
    );
    session.execute_sql(&load_sql).await.unwrap();

    let result = session
        .execute_sql("SELECT * FROM load_parquet ORDER BY id")
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 3);
    assert_eq!(get_i64(&result, 0, 0), 1);
    assert_eq!(get_string(&result, 1, 0), "Alice");
    assert_eq!(get_f64(&result, 2, 0), 100.5);
}

#[tokio::test(flavor = "current_thread")]
async fn test_load_data_from_parquet_with_file_uri() {
    let session = create_session();
    let temp_file = create_test_parquet();
    let path = temp_file.path().to_str().unwrap();

    session
        .execute_sql("CREATE TABLE load_parquet_uri (id INT64, name STRING, value FLOAT64)")
        .await
        .unwrap();

    let load_sql = format!(
        "LOAD DATA INTO load_parquet_uri FROM FILES (FORMAT='PARQUET', URIS=['file://{}'])",
        path
    );
    session.execute_sql(&load_sql).await.unwrap();

    let result = session
        .execute_sql("SELECT COUNT(*) FROM load_parquet_uri")
        .await
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_load_data_from_parquet_all_types() {
    let session = create_session();
    let temp_file = create_parquet_with_all_types();
    let path = temp_file.path().to_str().unwrap();

    session
        .execute_sql(
            "CREATE TABLE load_all_types (
                id INT64,
                active BOOL,
                score FLOAT64,
                name STRING,
                created_date DATE,
                updated_at DATETIME
            )",
        )
        .await
        .unwrap();

    let load_sql = format!(
        "LOAD DATA INTO load_all_types FROM FILES (FORMAT='PARQUET', URIS=['{}'])",
        path
    );
    session.execute_sql(&load_sql).await.unwrap();

    let result = session
        .execute_sql("SELECT * FROM load_all_types ORDER BY id")
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 2);
}

#[tokio::test(flavor = "current_thread")]
async fn test_load_data_from_json() {
    let session = create_session();
    let temp_file = create_test_json_file();
    let path = temp_file.path().to_str().unwrap();

    session
        .execute_sql("CREATE TABLE load_json (id INT64, name STRING, value FLOAT64)")
        .await
        .unwrap();

    let load_sql = format!(
        "LOAD DATA INTO load_json FROM FILES (FORMAT='JSON', URIS=['{}'])",
        path
    );
    session.execute_sql(&load_sql).await.unwrap();

    let result = session
        .execute_sql("SELECT * FROM load_json ORDER BY id")
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 3);
    assert_eq!(get_i64(&result, 0, 0), 1);
    assert_eq!(get_string(&result, 1, 0), "Alice");
}

#[tokio::test(flavor = "current_thread")]
async fn test_load_data_from_csv() {
    let session = create_session();
    let temp_file = create_test_csv_file();
    let path = temp_file.path().to_str().unwrap();

    session
        .execute_sql("CREATE TABLE load_csv (id INT64, name STRING, value FLOAT64)")
        .await
        .unwrap();

    let load_sql = format!(
        "LOAD DATA INTO load_csv FROM FILES (FORMAT='CSV', URIS=['{}'])",
        path
    );
    session.execute_sql(&load_sql).await.unwrap();

    let result = session
        .execute_sql("SELECT * FROM load_csv ORDER BY id")
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 3);
    assert_eq!(get_i64(&result, 0, 0), 1);
    assert_eq!(get_string(&result, 1, 0), "Alice");
}

#[tokio::test(flavor = "current_thread")]
async fn test_load_data_csv_with_skip_rows() {
    let session = create_session();
    let temp_file = create_csv_with_header();
    let path = temp_file.path().to_str().unwrap();

    session
        .execute_sql("CREATE TABLE load_csv_skip (id INT64, name STRING, value FLOAT64)")
        .await
        .unwrap();

    let load_sql = format!(
        "LOAD DATA INTO load_csv_skip FROM FILES (FORMAT='CSV', URIS=['{}'], skip_leading_rows=1)",
        path
    );
    session.execute_sql(&load_sql).await.unwrap();

    let result = session
        .execute_sql("SELECT * FROM load_csv_skip ORDER BY id")
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 2);
}

#[tokio::test(flavor = "current_thread")]
async fn test_load_data_csv_with_delimiter() {
    let session = create_session();
    let temp_file = create_csv_with_delimiter();
    let path = temp_file.path().to_str().unwrap();

    session
        .execute_sql("CREATE TABLE load_csv_delim (id INT64, name STRING, value FLOAT64)")
        .await
        .unwrap();

    let load_sql = format!(
        "LOAD DATA INTO load_csv_delim FROM FILES (FORMAT='CSV', URIS=['{}'], field_delimiter='|')",
        path
    );
    session.execute_sql(&load_sql).await.unwrap();

    let result = session
        .execute_sql("SELECT * FROM load_csv_delim ORDER BY id")
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 2);
    assert_eq!(get_string(&result, 1, 0), "Alice");
}

#[tokio::test(flavor = "current_thread")]
async fn test_load_data_csv_with_tab_delimiter() {
    let session = create_session();
    let temp_file = create_csv_with_tab_delimiter();
    let path = temp_file.path().to_str().unwrap();

    session
        .execute_sql("CREATE TABLE load_csv_tab (id INT64, name STRING, value FLOAT64)")
        .await
        .unwrap();

    let load_sql = format!(
        r#"LOAD DATA INTO load_csv_tab FROM FILES (FORMAT='CSV', URIS=['{}'], field_delimiter='\t')"#,
        path
    );
    session.execute_sql(&load_sql).await.unwrap();

    let result = session
        .execute_sql("SELECT * FROM load_csv_tab ORDER BY id")
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 2);
}

#[tokio::test(flavor = "current_thread")]
async fn test_load_data_csv_with_quoted_fields() {
    let session = create_session();
    let temp_file = create_csv_with_quoted_fields();
    let path = temp_file.path().to_str().unwrap();

    session
        .execute_sql("CREATE TABLE load_csv_quoted (id INT64, name STRING, value FLOAT64)")
        .await
        .unwrap();

    let load_sql = format!(
        "LOAD DATA INTO load_csv_quoted FROM FILES (FORMAT='CSV', URIS=['{}'])",
        path
    );
    session.execute_sql(&load_sql).await.unwrap();

    let result = session
        .execute_sql("SELECT * FROM load_csv_quoted ORDER BY id")
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 2);
    assert_eq!(get_string(&result, 1, 0), "Alice, Jr.");
    assert!(get_string(&result, 1, 1).contains("Bob"));
}

#[tokio::test(flavor = "current_thread")]
async fn test_load_data_csv_with_null_marker() {
    let session = create_session();
    let temp_file = create_csv_with_null_marker();
    let path = temp_file.path().to_str().unwrap();

    session
        .execute_sql("CREATE TABLE load_csv_nulls (id INT64, name STRING, value FLOAT64)")
        .await
        .unwrap();

    let load_sql = format!(
        "LOAD DATA INTO load_csv_nulls FROM FILES (FORMAT='CSV', URIS=['{}'], null_marker='NULL')",
        path
    );
    session.execute_sql(&load_sql).await.unwrap();

    let result = session
        .execute_sql("SELECT * FROM load_csv_nulls ORDER BY id")
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 3);
    assert!(is_null(&result, 1, 1));
    assert!(is_null(&result, 2, 2));
}

#[tokio::test(flavor = "current_thread")]
async fn test_load_data_overwrite() {
    let session = create_session();
    let temp_file = create_test_parquet();
    let path = temp_file.path().to_str().unwrap();

    session
        .execute_sql("CREATE TABLE load_overwrite (id INT64, name STRING, value FLOAT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO load_overwrite VALUES (100, 'Existing', 999.99)")
        .await
        .unwrap();

    let load_sql = format!(
        "LOAD DATA OVERWRITE load_overwrite FROM FILES (FORMAT='PARQUET', URIS=['{}'])",
        path
    );
    session.execute_sql(&load_sql).await.unwrap();

    let result = session
        .execute_sql("SELECT * FROM load_overwrite ORDER BY id")
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 3);
    assert_eq!(get_i64(&result, 0, 0), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn test_load_data_into_temp_table() {
    let session = create_session();
    let temp_file = create_test_parquet();
    let path = temp_file.path().to_str().unwrap();

    let load_sql = format!(
        "LOAD DATA INTO TEMP TABLE temp_load (id INT64, name STRING, value FLOAT64) FROM FILES (FORMAT='PARQUET', URIS=['{}'])",
        path
    );
    session.execute_sql(&load_sql).await.unwrap();

    let result = session
        .execute_sql("SELECT COUNT(*) FROM temp_load")
        .await
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_load_data_cloud_uri_nonexistent() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE load_cloud (id INT64)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "LOAD DATA INTO load_cloud FROM FILES (FORMAT='CSV', URIS=['gs://nonexistent/data.csv'])",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_load_data_avro_unsupported() {
    let session = create_session();
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();

    session
        .execute_sql("CREATE TABLE load_avro (id INT64)")
        .await
        .unwrap();

    let load_sql = format!(
        "LOAD DATA INTO load_avro FROM FILES (FORMAT='AVRO', URIS=['{}'])",
        path
    );
    let result = session.execute_sql(&load_sql).await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_export_data_with_query_aggregation() {
    let session = create_session();
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("agg.json");

    session
        .execute_sql("CREATE TABLE sales (id INT64, product STRING, amount FLOAT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO sales VALUES (1, 'A', 100), (2, 'B', 200), (3, 'A', 150)")
        .await
        .unwrap();

    let export_sql = format!(
        "EXPORT DATA OPTIONS(uri='file://{}', format='JSON') AS SELECT product, SUM(amount) AS total FROM sales GROUP BY product",
        output_path.display()
    );
    session.execute_sql(&export_sql).await.unwrap();

    assert!(output_path.exists());
    let contents = fs::read_to_string(&output_path).unwrap();
    assert!(contents.contains("total"));
}

#[tokio::test(flavor = "current_thread")]
async fn test_export_csv_special_characters() {
    let session = create_session();
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("special.csv");

    session
        .execute_sql("CREATE TABLE special_chars (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql(
            r#"INSERT INTO special_chars VALUES (1, 'Hello, World'), (2, 'Test "quotes"'), (3, 'New
line')"#,
        )
        .await
        .unwrap();

    let export_sql = format!(
        "EXPORT DATA OPTIONS(uri='file://{}', format='CSV') AS SELECT * FROM special_chars",
        output_path.display()
    );
    session.execute_sql(&export_sql).await.unwrap();

    assert!(output_path.exists());
}

#[tokio::test(flavor = "current_thread")]
async fn test_export_json_complex_types() {
    let session = create_session();
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("complex.json");

    session
        .execute_sql(
            "CREATE TABLE complex_export (id INT64, tags ARRAY<STRING>, info STRUCT<name STRING, age INT64>)",
        )
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO complex_export VALUES (1, ['rust', 'sql'], STRUCT('Alice', 30))")
        .await
        .unwrap();

    let export_sql = format!(
        "EXPORT DATA OPTIONS(uri='file://{}', format='JSON') AS SELECT * FROM complex_export",
        output_path.display()
    );
    session.execute_sql(&export_sql).await.unwrap();

    assert!(output_path.exists());
}

#[tokio::test(flavor = "current_thread")]
async fn test_export_then_load_roundtrip_parquet() {
    let session = create_session();
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("roundtrip.parquet");

    session
        .execute_sql("CREATE TABLE roundtrip_src (id INT64, name STRING, value FLOAT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO roundtrip_src VALUES (1, 'Alice', 100.5), (2, 'Bob', 200.75)")
        .await
        .unwrap();

    let export_sql = format!(
        "EXPORT DATA OPTIONS(uri='file://{}', format='PARQUET') AS SELECT * FROM roundtrip_src",
        output_path.display()
    );
    session.execute_sql(&export_sql).await.unwrap();

    session
        .execute_sql("CREATE TABLE roundtrip_dest (id INT64, name STRING, value FLOAT64)")
        .await
        .unwrap();

    let load_sql = format!(
        "LOAD DATA INTO roundtrip_dest FROM FILES (FORMAT='PARQUET', URIS=['{}'])",
        output_path.display()
    );
    session.execute_sql(&load_sql).await.unwrap();

    let result = session
        .execute_sql("SELECT * FROM roundtrip_dest ORDER BY id")
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 2);
    assert_eq!(get_i64(&result, 0, 0), 1);
    assert_eq!(get_string(&result, 1, 0), "Alice");
}

#[tokio::test(flavor = "current_thread")]
async fn test_export_then_load_roundtrip_json() {
    let session = create_session();
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("roundtrip.json");

    session
        .execute_sql("CREATE TABLE json_roundtrip_src (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO json_roundtrip_src VALUES (1, 'Alice'), (2, 'Bob')")
        .await
        .unwrap();

    let export_sql = format!(
        "EXPORT DATA OPTIONS(uri='file://{}', format='JSON') AS SELECT * FROM json_roundtrip_src",
        output_path.display()
    );
    session.execute_sql(&export_sql).await.unwrap();

    session
        .execute_sql("CREATE TABLE json_roundtrip_dest (id INT64, name STRING)")
        .await
        .unwrap();

    let load_sql = format!(
        "LOAD DATA INTO json_roundtrip_dest FROM FILES (FORMAT='JSON', URIS=['{}'])",
        output_path.display()
    );
    session.execute_sql(&load_sql).await.unwrap();

    let result = session
        .execute_sql("SELECT * FROM json_roundtrip_dest ORDER BY id")
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 2);
}

#[tokio::test(flavor = "current_thread")]
async fn test_export_empty_table() {
    let session = create_session();
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("empty.json");

    session
        .execute_sql("CREATE TABLE empty_export (id INT64, name STRING)")
        .await
        .unwrap();

    let export_sql = format!(
        "EXPORT DATA OPTIONS(uri='file://{}', format='JSON') AS SELECT * FROM empty_export",
        output_path.display()
    );
    session.execute_sql(&export_sql).await.unwrap();

    assert!(output_path.exists());
    let contents = fs::read_to_string(&output_path).unwrap();
    assert!(contents.is_empty() || contents.trim().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn test_export_json_value_types() {
    let session = create_session();
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("types.json");

    session
        .execute_sql(
            "CREATE TABLE json_types (
                col_bool BOOL,
                col_int INT64,
                col_float FLOAT64,
                col_string STRING,
                col_date DATE,
                col_time TIME,
                col_numeric NUMERIC,
                col_json JSON
            )",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            r#"INSERT INTO json_types VALUES (
                true, 42, 3.14, 'hello',
                DATE '2024-01-15', TIME '10:30:00',
                NUMERIC '123.45',
                JSON '{"key": "value"}'
            )"#,
        )
        .await
        .unwrap();

    let export_sql = format!(
        "EXPORT DATA OPTIONS(uri='file://{}', format='JSON') AS SELECT * FROM json_types",
        output_path.display()
    );
    session.execute_sql(&export_sql).await.unwrap();

    assert!(output_path.exists());
}

#[tokio::test(flavor = "current_thread")]
async fn test_load_json_with_arrays() {
    let session = create_session();
    let temp_file = NamedTempFile::new().unwrap();
    {
        let mut file = temp_file.reopen().unwrap();
        writeln!(file, r#"{{"id": 1, "tags": ["a", "b", "c"]}}"#).unwrap();
        writeln!(file, r#"{{"id": 2, "tags": ["x", "y"]}}"#).unwrap();
    }
    let path = temp_file.path().to_str().unwrap();

    session
        .execute_sql("CREATE TABLE load_json_array (id INT64, tags ARRAY<STRING>)")
        .await
        .unwrap();

    let load_sql = format!(
        "LOAD DATA INTO load_json_array FROM FILES (FORMAT='JSON', URIS=['{}'])",
        path
    );
    session.execute_sql(&load_sql).await.unwrap();

    let result = session
        .execute_sql("SELECT id, ARRAY_LENGTH(tags) FROM load_json_array ORDER BY id")
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 2);
}

#[tokio::test(flavor = "current_thread")]
async fn test_load_json_with_nested_object() {
    let session = create_session();
    let temp_file = NamedTempFile::new().unwrap();
    {
        let mut file = temp_file.reopen().unwrap();
        writeln!(
            file,
            r#"{{"id": 1, "info": {{"name": "Alice", "age": 30}}}}"#
        )
        .unwrap();
        writeln!(file, r#"{{"id": 2, "info": {{"name": "Bob", "age": 25}}}}"#).unwrap();
    }
    let path = temp_file.path().to_str().unwrap();

    session
        .execute_sql(
            "CREATE TABLE load_json_nested (id INT64, info STRUCT<name STRING, age INT64>)",
        )
        .await
        .unwrap();

    let load_sql = format!(
        "LOAD DATA INTO load_json_nested FROM FILES (FORMAT='JSON', URIS=['{}'])",
        path
    );
    session.execute_sql(&load_sql).await.unwrap();

    let result = session
        .execute_sql("SELECT id FROM load_json_nested ORDER BY id")
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 2);
}

#[tokio::test(flavor = "current_thread")]
async fn test_load_json_with_dates() {
    let session = create_session();
    let temp_file = NamedTempFile::new().unwrap();
    {
        let mut file = temp_file.reopen().unwrap();
        writeln!(file, r#"{{"id": 1, "created": "2024-01-15"}}"#).unwrap();
        writeln!(file, r#"{{"id": 2, "created": "2024-06-20"}}"#).unwrap();
    }
    let path = temp_file.path().to_str().unwrap();

    session
        .execute_sql("CREATE TABLE load_json_dates (id INT64, created DATE)")
        .await
        .unwrap();

    let load_sql = format!(
        "LOAD DATA INTO load_json_dates FROM FILES (FORMAT='JSON', URIS=['{}'])",
        path
    );
    session.execute_sql(&load_sql).await.unwrap();

    let result = session
        .execute_sql("SELECT * FROM load_json_dates ORDER BY id")
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 2);
}

#[tokio::test(flavor = "current_thread")]
async fn test_load_csv_type_coercion() {
    let session = create_session();
    let temp_file = NamedTempFile::new().unwrap();
    {
        let mut file = temp_file.reopen().unwrap();
        writeln!(file, "1,true,100.5,2024-01-15").unwrap();
        writeln!(file, "2,false,200.75,2024-06-20").unwrap();
    }
    let path = temp_file.path().to_str().unwrap();

    session
        .execute_sql(
            "CREATE TABLE load_csv_types (id INT64, active BOOL, value FLOAT64, created DATE)",
        )
        .await
        .unwrap();

    let load_sql = format!(
        "LOAD DATA INTO load_csv_types FROM FILES (FORMAT='CSV', URIS=['{}'])",
        path
    );
    session.execute_sql(&load_sql).await.unwrap();

    let result = session
        .execute_sql("SELECT * FROM load_csv_types ORDER BY id")
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 2);
}

#[tokio::test(flavor = "current_thread")]
async fn test_export_uri_with_wildcard() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE wildcard_export (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO wildcard_export VALUES (1)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "EXPORT DATA OPTIONS(uri='gs://bucket/export/*.csv', format='CSV') AS SELECT * FROM wildcard_export",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_export_to_bigtable_uri() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE bigtable_export (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO bigtable_export VALUES (1)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "EXPORT DATA OPTIONS(uri='bigtable://project/instance/table', format='JSON') AS SELECT * FROM bigtable_export",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_export_to_pubsub_uri() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE pubsub_export (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO pubsub_export VALUES (1)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "EXPORT DATA OPTIONS(uri='pubsub://projects/project/topics/topic', format='JSON') AS SELECT * FROM pubsub_export",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_export_to_spanner_uri() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE spanner_export (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO spanner_export VALUES (1)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "EXPORT DATA OPTIONS(uri='spanner://projects/project/instances/instance/databases/db', format='JSON') AS SELECT * FROM spanner_export",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_export_to_s3_uri() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE s3_export (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO s3_export VALUES (1)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "EXPORT DATA OPTIONS(uri='s3://bucket/data.csv', format='CSV') AS SELECT * FROM s3_export",
        )
        .await;
    assert!(result.is_ok());
}
