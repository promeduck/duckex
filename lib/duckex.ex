defmodule Duckex do
  @moduledoc """
  A naive DuckDB binding for Elixir via Rust port.

  Provides basic functionality to execute SQL queries against DuckDB,
  primarily for testing DuckLake functionality.
  """

  alias Duckex.Port

  @doc """
  Starts the DuckDB process.

  Returns `{:ok, pid}` on success.
  """
  def start_link do
    Port.start_link(name: __MODULE__)
  end

  @doc """
  Executes a SQL statement (typically DDL or administrative commands).

  ## Parameters
  - `sql` - The SQL statement to execute
  - `values` - Optional parameter values (default: [])
  - `timeout` - Timeout in milliseconds (default: 25 seconds)

  ## Returns
  `%Duckex.Result{}` on success or `%Duckex.Error{}` on failure.
  """
  defdelegate execute(sql, values \\ [], timeout \\ :timer.seconds(25)), to: Port

  @doc """
  Executes a SQL query and returns results.

  ## Parameters
  - `sql` - The SQL query to execute
  - `values` - Optional parameter values (default: [])
  - `timeout` - Timeout in milliseconds (default: 25 seconds)

  ## Returns
  `%Duckex.Result{}` with query results on success or `%Duckex.Error{}` on failure.
  """
  defdelegate query(sql, values \\ [], timeout \\ :timer.seconds(25)), to: Port

  @doc """
  Helper function for quick DuckLake setup.

  Starts DuckDB, installs required extensions, and attaches a PostgreSQL
  database via DuckLake for testing purposes.
  """
  def dummy_start() do
    Duckex.start_link()
    Duckex.execute("INSTALL ducklake;")
    Duckex.execute("INSTALL postgres;")

    Duckex.execute(
      "ATTACH 'ducklake:postgres:dbname=ducklake_catalog host=127.0.0.1 port=5452 password=postgres user=postgres' AS my_ducklake (DATA_PATH 'data_files/');"
    )
  end
end
