// SPDX-FileCopyrightText: 2025 Stas Muzhyk <sts@abc3.dev>
// SPDX-FileCopyrightText: 2025 Łukasz Niemier <~@hauleth.dev>
//
// SPDX-License-Identifier: Apache-2.0

use std::borrow::Cow;
use std::io::{self, prelude::*};

use base64::{engine::general_purpose, Engine as _};

use duckdb::arrow::datatypes::DataType;
use duckdb::params_from_iter;
use duckdb::types::Value;
use duckdb::Connection;

use serde::{Deserialize, Serialize};
use serde_json as json;

mod cache;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "command")]
enum Command {
    Begin,
    Close {
        stmt: usize,
    },
    Commit,
    Deallocate {
        cursor: usize,
    },
    Declare {
        stmt: usize,
        params: Box<[ErlangValue]>,
    },
    Execute {
        stmt: usize,
        params: Box<[ErlangValue]>,
    },
    Fetch {
        cursor: usize,
    },
    Prepare {
        query: String,
    },
    Rollback,
    Status,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "status")]
enum Response {
    Ok {
        columns: Vec<(String, String)>,
        rows: Vec<Box<[json::Value]>>,
        num_rows: usize,
    },
    Error {
        message: Cow<'static, str>,
    },
}

impl Response {
    fn empty() -> Self {
        Self::Ok {
            columns: vec![],
            rows: vec![],
            num_rows: 0,
        }
    }

    fn error<M: Into<Cow<'static, str>>>(message: M) -> Self {
        Self::Error {
            message: message.into(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
enum ErlangValue {
    Integer(i64),
    Float(f64),
    Text(String),
    Boolean(bool),
    Null,
    Blob(String), // base64 encoded
}

impl duckdb::types::ToSql for ErlangValue {
    fn to_sql(&self) -> duckdb::Result<duckdb::types::ToSqlOutput<'_>> {
        use duckdb::types::{ToSqlOutput::*, Value, ValueRef::*};

        match *self {
            Self::Integer(int) => Ok(Borrowed(BigInt(int))),
            Self::Float(float) => Ok(Borrowed(Double(float))),
            Self::Text(ref txt) => Ok(Borrowed(Text(txt.as_bytes()))),
            Self::Boolean(b) => Ok(Borrowed(Boolean(b))),
            Self::Null => Ok(Borrowed(Null)),
            Self::Blob(ref data) => {
                let decoded = general_purpose::STANDARD.decode(data).unwrap_or_default();

                Ok(Owned(Value::Blob(decoded)))
            }
        }
    }
}

fn duckdb_value_to_json(value: Value) -> json::Value {
    match value {
        Value::Null => json::Value::Null,
        Value::Boolean(b) => json::Value::Bool(b),
        Value::TinyInt(i) => json::Value::Number(i.into()),
        Value::SmallInt(i) => json::Value::Number(i.into()),
        Value::Int(i) => json::Value::Number(i.into()),
        Value::BigInt(i) => json::Value::Number(i.into()),
        Value::UTinyInt(i) => json::Value::Number(i.into()),
        Value::USmallInt(i) => json::Value::Number(i.into()),
        Value::UInt(i) => json::Value::Number(i.into()),
        Value::UBigInt(i) => json::Value::Number(i.into()),
        Value::Float(f) => json::Number::from_f64(f as f64)
            .map(json::Value::Number)
            .unwrap_or(json::Value::Null),
        Value::Double(f) => json::Number::from_f64(f)
            .map(json::Value::Number)
            .unwrap_or(json::Value::Null),
        Value::Timestamp(unit, value) => json::Value::Number(unit.to_micros(value).into()),
        Value::Text(s) => json::Value::String(s),
        Value::Blob(b) => json::Value::String(general_purpose::STANDARD.encode(b)),
        Value::Time64(unit, value) => json::Value::Number(unit.to_micros(value).into()),
        Value::List(vec) => json::Value::Array(vec.into_iter().map(duckdb_value_to_json).collect()),
        Value::Enum(s) => json::Value::String(s),
        Value::Struct(s) => json::Value::Object(s.iter().cloned().map(|(k, v)| (k, duckdb_value_to_json(v))).collect()),
        Value::Map(m) => json::Value::Object(m.iter().cloned().map(|(k, v)| (duckdb_value_to_string(k), duckdb_value_to_json(v))).collect()),
        Value::Array(vec) => {
            json::Value::Array(vec.into_iter().map(duckdb_value_to_json).collect())
        }
        Value::Union(val) => duckdb_value_to_json(*val),
        _ => json::Value::String(format!("{:?}", value)),
    }
}

fn duckdb_value_to_string(value: Value) -> String {
    match value {
        Value::Text(s) => s,
        Value::Blob(b) => general_purpose::STANDARD.encode(b),
        _ => format!("{:?}", value)
    }
}

fn setup_connection() -> duckdb::Result<Connection, String> {
    let conn = Connection::open_in_memory()
        .map_err(|e| format!("Failed to create DuckDB connection: {}", e))?;

    Ok(conn)
}

#[derive(Debug, PartialEq)]
enum SQLError {
    Execution(duckdb::Error),
    Preparation(duckdb::Error),
    RowProcessing(duckdb::Error),
    InvalidCacheIndex,
    Unsupported,
}

impl std::fmt::Display for SQLError {
    fn fmt(&self, w: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        match self {
            Self::Execution(err) => write!(w, "SQL execution error: {}", err),
            Self::Preparation(err) => write!(w, "SQL preparation error: {}", err),
            Self::RowProcessing(err) => write!(w, "SQL row processing error: {}", err),
            Self::InvalidCacheIndex => write!(w, "Invalid cache index"),
            Self::Unsupported => write!(w, "Feature not supported"),
        }
    }
}

impl std::error::Error for SQLError {}

impl From<SQLError> for Response {
    fn from(err: SQLError) -> Response {
        Response::error(err.to_string())
    }
}

impl<V, E> From<Result<V, E>> for Response
where
    V: Into<Response>,
    E: Into<Response>,
{
    fn from(result: Result<V, E>) -> Response {
        match result {
            Ok(ok) => ok.into(),
            Err(err) => err.into(),
        }
    }
}

fn execute<'a>(
    stmt: &mut duckdb::Statement<'a>,
    values: &[ErlangValue],
) -> Result<Response, SQLError> {
    let rows = stmt
        .query_map(params_from_iter(values), |row| {
            (0..)
                .map_while(|i| match row.get::<_, Value>(i).map(duckdb_value_to_json) {
                    val @ Ok(_) => Some(val),
                    _ => None,
                })
                .collect()
        })
        .map_err(SQLError::Execution)?;

    let rows = rows
        .collect::<Result<Vec<_>, _>>()
        .map_err(SQLError::RowProcessing)?;

    let num_rows = rows.len();

    let columns = stmt
        .column_names()
        .into_iter()
        .enumerate()
        .map(|(idx, name)| (name, stmt.column_type(idx).to_string()))
        .collect();

    Ok(Response::Ok {
        columns,
        rows,
        num_rows,
    })
}

fn prepare<'a>(
    conn: &'a Connection,
    sql: &str,
    cache: &mut cache::Cache<duckdb::Statement<'a>>,
) -> Result<Response, SQLError> {
    let stmt = conn.prepare(sql).map_err(SQLError::Preparation)?;

    let id = cache.store(stmt);

    Ok(Response::Ok {
        columns: vec![("ref".into(), DataType::UInt32.to_string())],
        rows: vec![Box::new([id.into()])],
        num_rows: 1,
    })
}

fn simple_command(conn: &Connection, sql: &str) -> Result<Response, SQLError> {
    let mut stmt = conn.prepare(sql).map_err(SQLError::Preparation)?;

    stmt.execute([])
        .map_err(SQLError::Execution)
        .map(|_| Response::empty())
}

fn main() {
    let stdin = io::stdin().lock();
    let reader = io::BufReader::new(stdin);
    let mut stdout = io::stdout().lock();

    let conn = setup_connection().expect("Couldn't setup connection");

    let mut statements = cache::Cache::with_capacity(1024);
    // let mut cursors = RefCell::new(cache::Cache::with_capacity(1024));

    for line in reader.lines() {
        let line = line.expect("Couldn't read input string");
        let cmd: Command = json::from_str(&line).expect("Couldn't decode message");
        let result: Response = match cmd {
            Command::Prepare { query } => prepare(&conn, &query, &mut statements),
            Command::Close { stmt } => {
                statements.remove(stmt);

                Ok(Response::empty())
            }
            Command::Begin => simple_command(&conn, "BEGIN"),
            Command::Commit => simple_command(&conn, "COMMIT"),
            Command::Rollback => simple_command(&conn, "ROLLBACK"),
            Command::Execute { stmt, params } => statements
                .get_mut(stmt)
                .ok_or(SQLError::InvalidCacheIndex)
                .and_then(|stmt| execute(stmt, &params)),
            Command::Status => Ok(Response::empty()),
            _ => Err(SQLError::Unsupported),
        }
        .into();

        let response = json::to_string(&result).expect("Response should always be encodeable");

        match write!(stdout, "{}\n", response) {
            Ok(_) => (),
            Err(err) if err.kind() == io::ErrorKind::BrokenPipe => return,
            Err(err) => panic!("Unexpected error: {err}")
        }
    }
}
