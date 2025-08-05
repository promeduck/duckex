<!--
SPDX-FileCopyrightText: 2025 Stas Muzhyk <sts@abc3.dev>
SPDX-FileCopyrightText: 2025 Łukasz Niemier <~@hauleth.dev>

SPDX-License-Identifier: Apache-2.0
-->

# Duckex

A naive DuckDB binding for Elixir via Rust port, primarily for testing DuckLake functionality.

## Installation

Add `duckex` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:duckex, github: "http://github.com/promeduck/duckex"}
  ]
end
```

## Usage

Start the process and execute queries:

```elixir
# Install extensions
Duckex.install_extensions([:ducklake, :postgres])

# Start Duckex with Ducklake connection
{:ok, conn} = Duckex.start_link(
  attach: [
    {"ducklake:postgres:dbname=ducklake_catalog",
      as: :my_ducklake,
      options: [
        data_path: "tmp/data_files"
      ]
    }
  ]
)

# Create table
Duckex.query!(conn, """
  CREATE TABLE my_ducklake.my_demo_table (
    id INTEGER,
    name TEXT,
    created_at TIMESTAMP
  )
""")

# Insert data
Duckex.query!(conn, """
INSERT INTO my_ducklake.my_demo_table (id, name, created_at)
VALUES (2, 'Bob', CURRENT_TIMESTAMP), (3, 'Charlie', CURRENT_TIMESTAMP)
""", [])

# Query data
Duckex.query!(conn, "SELECT * FROM my_ducklake.my_demo_table LIMIT 100", [])
```

## Result Format

Returns `%Duckex.Result{}` struct:

```elixir
%Duckex.Result{
  columns: [
    ["id", "Int32"],
    ["name", "Utf8"],
    ["created_at", "Timestamp(Microsecond, None)"]
  ],
  rows: [
    [2, "Bob", ~U[2025-08-06 17:38:38.512000Z]],
    [3, "Charlie", ~U[2025-08-06 17:38:38.512000Z]]
  ],
  num_rows: 2
}
```

## Error Format

Returns `%Duckex.Error{}` struct on SQL errors:

```elixir
Duckex.query(conn, "some unexisting sql", [])
{:error,
 %Duckex.Error{
   message: "SQL preparation error: Parser Error: syntax error at or near \"some\"\n\nLINE 1: some unexisting sql\n        ^",
   query: %{command: "prepare", query: "some unexisting sql"}
 }}
```
