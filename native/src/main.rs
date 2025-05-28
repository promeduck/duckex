use duckdb::Connection;
use duckdb::types::{Value, ToSql};
use duckdb::Result;
use serde::{Serialize, Deserialize};
use base64::{engine::general_purpose, Engine as _};
use std::io::{self, Write, Read};
use serde_json;

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

fn erlang_value_to_duckdb(value: &ErlangValue) -> Value {
    match value {
        ErlangValue::Boolean(b) => Value::Boolean(*b),
        ErlangValue::Integer(i) => Value::BigInt(*i),
        ErlangValue::Float(f) => Value::Double(*f),
        ErlangValue::Text(s) => Value::Text(s.clone()),
        ErlangValue::Null => Value::Null,
        ErlangValue::Blob(b) => Value::Blob(general_purpose::STANDARD.decode(b).unwrap_or_default()),
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
    let params: Vec<Value> = values.iter()
        .map(erlang_value_to_duckdb)
        .collect();

    let param_refs: Vec<&dyn ToSql> = params.iter()
        .map(|v| v as &dyn ToSql)
        .collect();

    match conn.prepare(sql) {
        Ok(mut stmt) => {
            match stmt.execute(param_refs.as_slice()) {
                Ok(affected_rows) => {
                    Response {
                        status: "ok".to_string(),
                        message: format!("Statement executed successfully. Affected rows: {}", affected_rows),
                        columns: None,
                        rows: None,
                    }
                },
                Err(e) => Response {
                    status: "error".to_string(),
                    message: format!("SQL execution error: {}", e),
                    columns: None,
                    rows: None,
                }
            }
        },
        Err(e) => Response {
            status: "error".to_string(),
            message: format!("SQL preparation error: {}", e),
            columns: None,
            rows: None,
        }
    }
}

fn query_statement(conn: &Connection, sql: &str, values: &[ErlangValue]) -> Response {
    let params: Vec<Value> = values.iter()
        .map(erlang_value_to_duckdb)
        .collect();

    let param_refs: Vec<&dyn ToSql> = params.iter()
        .map(|v| v as &dyn ToSql)
        .collect();

    match conn.prepare(sql) {
        Ok(mut stmt) => {
            match stmt.query_map(param_refs.as_slice(), |row| {
                let mut row_values = Vec::new();
                
                for i in 0.. {
                    match row.get::<_, Value>(i) {
                        Ok(val) => row_values.push(duckdb_value_to_json(&val)),
                        Err(_) => break,
                    }
                }
                Ok(row_values)
            }) {
                Ok(rows) => {
                    let mut all_rows = Vec::new();
                    
                    for row_result in rows {
                        match row_result {
                            Ok(row_data) => all_rows.push(row_data),
                            Err(e) => return Response {
                                status: "error".to_string(),
                                message: format!("Row processing error: {}", e),
                                columns: None,
                                rows: None,
                            }
                        }
                    }
                    
                    let column_names = stmt.column_names();

                    Response {
                        status: "ok".to_string(),
                        message: "Query executed successfully".to_string(),
                        columns: Some(column_names),
                        rows: Some(all_rows),
                    }
                },
                Err(e) => Response {
                    status: "error".to_string(),
                    message: format!("SQL query error: {}", e),
                    columns: None,
                    rows: None,
                }
            }
        },
        Err(e) => Response {
            status: "error".to_string(),
            message: format!("SQL preparation error: {}", e),
            columns: None,
            rows: None,
        }
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
            Ok(json_str) => {
                match serde_json::from_str::<Command>(json_str.trim()) {
                    Ok(cmd) => {
                        let result = if let Some(sql) = cmd.sql {
                            match cmd.command.as_str() {
                                "query" => query_statement(&conn, &sql, &cmd.values),
                                "execute" => execute_statement(&conn, &sql, &cmd.values),
                                _ => Response {
                                    status: "error".to_string(),
                                    message: format!("Unknown command: {}", cmd.command),
                                    columns: None,
                                    rows: None,
                                }
                            }
                        } else {
                            Response {
                                status: "error".to_string(),
                                message: "No SQL query provided".to_string(),
                                columns: None,
                                rows: None,
                            }
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
                }
            }
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

