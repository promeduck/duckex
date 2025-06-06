use base64::{Engine as _, engine::general_purpose};
use duckdb::Connection;
use duckdb::Error;
use duckdb::Result;
use duckdb::params_from_iter;
use duckdb::types::{Null, ToSql, ToSqlOutput, Value};
use serde::{Deserialize, Serialize};
use std::io::{self, Read, Write};

#[derive(Debug, Serialize, Deserialize)]
struct Command {
    command: String, // "execute" | "query"
    sql: Option<String>,
    values: Vec<ErlangValue>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Response {
    status: String,
    message: String,
    columns: Option<Vec<String>>,
    rows: Option<Vec<Vec<serde_json::Value>>>,
}

impl Response {
    fn error(message: String) -> Self {
        Response {
            status: "error".to_string(),
            message,
            columns: None,
            rows: None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum ErlangValue {
    Integer(i64),
    Float(f64),
    Text(String),
    Boolean(bool),
    Null,
    Blob(String), // base64 encoded
}

// fn duckdb_value_to_erlang(value: &Value) -> ErlangValue {
//     match value {
//         Value::Boolean(b) => ErlangValue::Boolean(*b),
//         Value::Int(i) => ErlangValue::Integer(*i as i64),
//         Value::BigInt(i) => ErlangValue::Integer(*i),
//         Value::Float(f) => ErlangValue::Float(*f as f64),
//         Value::Double(f) => ErlangValue::Float(*f),
//         Value::Text(s) => ErlangValue::Text(s.clone()),
//         Value::Null => ErlangValue::Null,
//         Value::Blob(b) => ErlangValue::Blob(general_purpose::STANDARD.encode(b)),
//         _ => ErlangValue::Text(format!("{:?}", value)),
//     }
// }

impl From<&ErlangValue> for ToSqlOutput<'_> {
    fn from(value: &ErlangValue) -> Self {
        match value {
            ErlangValue::Integer(i) => ToSqlOutput::from(*i),
            ErlangValue::Float(f) => ToSqlOutput::from(*f),
            ErlangValue::Text(s) => ToSqlOutput::from(s.clone()),
            ErlangValue::Boolean(b) => ToSqlOutput::from(*b),
            ErlangValue::Null => ToSqlOutput::from(Null),
            ErlangValue::Blob(b) => {
                let decoded_blob = general_purpose::STANDARD.decode(b).unwrap_or_default();
                ToSqlOutput::from(decoded_blob)
            }
        }
    }
}

impl ToSql for ErlangValue {
    fn to_sql(&self) -> Result<ToSqlOutput, Error> {
        Ok(self.into())
    }
}

fn duckdb_value_to_json(value: &Value) -> serde_json::Value {
    match value {
        Value::Boolean(b) => serde_json::Value::Bool(*b),
        Value::Int(i) => serde_json::Value::Number((*i as i64).into()),
        Value::BigInt(i) => serde_json::Value::Number((*i).into()),
        Value::Float(f) => serde_json::Number::from_f64(*f as f64)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::Double(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::Text(s) => serde_json::Value::String(s.clone()),
        Value::Null => serde_json::Value::Null,
        Value::Blob(b) => serde_json::Value::String(general_purpose::STANDARD.encode(b)),
        _ => serde_json::Value::String(format!("{:?}", value)),
    }
}

fn setup_connection() -> Result<Connection, String> {
    let conn = Connection::open_in_memory()
        .map_err(|e| format!("Failed to create DuckDB connection: {}", e))?;

    Ok(conn)
}

fn execute_statement(conn: &Connection, sql: &str, values: &[ErlangValue]) -> Response {
    match conn.prepare(sql) {
        Ok(mut stmt) => match stmt.execute(params_from_iter(values)) {
            Ok(affected_rows) => Response {
                status: "ok".to_string(),
                message: format!(
                    "Statement executed successfully. Affected rows: {}",
                    affected_rows
                ),
                columns: None,
                rows: None,
            },
            Err(e) => Response::error(format!("SQL execution error: {}", e)),
        },
        Err(e) => Response::error(format!("SQL preparation error: {}", e)),
    }
}

fn query_statement(conn: &Connection, sql: &str, values: &[ErlangValue]) -> Response {
    let Ok(mut stmt) = conn.prepare(sql) else {
        return Response::error(format!("SQL preparation error: {}", sql));
    };

    let mut all_rows = Vec::new();
    let columns_count = stmt.column_count();

    let rows = stmt.query_map(params_from_iter(values), |row| {
        let mut row_values = Vec::with_capacity(columns_count);
        for i in 0..columns_count {
            match row.get(i) {
                Ok(val) => row_values.push(duckdb_value_to_json(&val)),
                Err(e) => return Err(e),
            }
        }
        all_rows.push(row_values);
        Ok(())
    });

    match rows {
        Ok(_) => Response {
            status: "ok".to_string(),
            message: "Query executed successfully".to_string(),
            columns: Some(stmt.column_names()),
            rows: Some(all_rows),
        },
        Err(e) => Response::error(format!("SQL query error: {}\n\n{}", e, sql)),
    }
}

fn main() {
    let mut stdin = io::stdin();
    let mut stdout = io::stdout();
    let mut buffer = [0u8; 4096];

    let conn = match setup_connection() {
        Ok(conn) => conn,
        Err(e) => {
            eprintln!("{}", e);
            return;
        }
    };

    loop {
        let size = match stdin.read(&mut buffer) {
            Ok(0) => break,
            Ok(n) => n,
            Err(e) => {
                eprintln!("Error reading: {}", e);
                break;
            }
        };

        match std::str::from_utf8(&buffer[..size]) {
            Ok(json_str) => match serde_json::from_str::<Command>(json_str.trim()) {
                Ok(cmd) => {
                    let result = if let Some(sql) = cmd.sql {
                        match cmd.command.as_str() {
                            "query" => query_statement(&conn, &sql, &cmd.values),
                            "execute" => execute_statement(&conn, &sql, &cmd.values),
                            _ => Response::error(format!("Unknown command: {}", cmd.command)),
                        }
                    } else {
                        Response::error("No SQL query provided".to_string())
                    };

                    match serde_json::to_string(&result) {
                        Ok(json_response) => {
                            if let Err(e) = stdout.write_all(json_response.as_bytes()) {
                                eprintln!("Failed to write response: {}", e);
                                break;
                            }
                            if let Err(e) = stdout.write_all(b"\n") {
                                eprintln!("Failed to write newline: {}", e);
                                break;
                            }
                            if let Err(e) = stdout.flush() {
                                eprintln!("Failed to flush response: {}", e);
                                break;
                            }
                        }
                        Err(e) => {
                            eprintln!("Failed to encode JSON response: {:?}", e);
                            break;
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Failed to decode JSON: {:?}", e);
                }
            },
            Err(e) => {
                eprintln!("Failed to convert buffer to UTF-8: {:?}", e);
            }
        }
    }
}

// fn main() -> Result<()> {
//     let conn = Connection::open_in_memory()?;

//     let mut stmt = conn.prepare("SELECT 1 as qwe, 2 as asd;")?;

//     let val: Value = stmt.query_row([], |row| row.get(0))?;
//     println!("First value: {:?}", val);

//     for column in stmt.column_names() {
//         println!("Column name: {}", column);
//     }

//     Ok(())
// }
