use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use comfy_table::Table;
use comfy_table::presets::UTF8_FULL;
use yachtsql::arrow::array::Array;
use yachtsql::{RecordBatch, YachtSQLSession};

#[derive(Parser)]
#[command(name = "yachtsql")]
#[command(about = "YachtSQL - Lightweight in-memory SQL database", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Query { sql: String },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Query { sql } => {
            execute_query(&sql).await?;
        }
    }

    Ok(())
}

async fn execute_query(sql: &str) -> Result<()> {
    let session = YachtSQLSession::new();
    let batches = session
        .execute_sql(sql)
        .await
        .context("Failed to execute SQL query")?;
    print_record_batches(&batches)?;
    Ok(())
}

fn print_record_batches(batches: &[RecordBatch]) -> Result<()> {
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    if total_rows == 0 {
        println!("(0 rows)");
        return Ok(());
    }

    let mut table = Table::new();
    table.load_preset(UTF8_FULL);

    if let Some(first_batch) = batches.first() {
        let schema = first_batch.schema();
        let headers: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        table.set_header(headers);
    }

    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let mut row_values = Vec::new();
            for col_idx in 0..batch.num_columns() {
                let array = batch.column(col_idx);
                let value = format_array_value(array, row_idx);
                row_values.push(value);
            }
            table.add_row(row_values);
        }
    }

    println!("{table}");
    println!("({} rows)", total_rows);

    Ok(())
}

fn format_array_value(array: &dyn Array, row: usize) -> String {
    use yachtsql::arrow::array::*;
    use yachtsql::arrow::datatypes::DataType;

    if array.is_null(row) {
        return "NULL".to_string();
    }

    match array.data_type() {
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            arr.value(row).to_string()
        }
        DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
            arr.value(row).to_string()
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
            arr.value(row).to_string()
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            arr.value(row).to_string()
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            arr.value(row).to_string()
        }
        DataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            arr.value(row).to_string()
        }
        DataType::UInt16 => {
            let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            arr.value(row).to_string()
        }
        DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            arr.value(row).to_string()
        }
        DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            arr.value(row).to_string()
        }
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            arr.value(row).to_string()
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            arr.value(row).to_string()
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            arr.value(row).to_string()
        }
        DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            arr.value(row).to_string()
        }
        DataType::Date32 => {
            let arr = array.as_any().downcast_ref::<Date32Array>().unwrap();
            arr.value_as_date(row)
                .map(|d| d.to_string())
                .unwrap_or_else(|| "NULL".to_string())
        }
        DataType::Date64 => {
            let arr = array.as_any().downcast_ref::<Date64Array>().unwrap();
            arr.value_as_date(row)
                .map(|d| d.to_string())
                .unwrap_or_else(|| "NULL".to_string())
        }
        DataType::Timestamp(_, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            arr.value_as_datetime(row)
                .map(|dt| dt.to_string())
                .unwrap_or_else(|| "NULL".to_string())
        }
        _ => format!("<{:?}>", array.data_type()),
    }
}
