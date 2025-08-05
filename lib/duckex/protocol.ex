# SPDX-FileCopyrightText: 2025 Stas Muzhyk <sts@abc3.dev>
# SPDX-FileCopyrightText: 2025 Łukasz Niemier <~@hauleth.dev>
#
# SPDX-License-Identifier: Apache-2.0

defmodule Duckex.Protocol do
  @moduledoc false

  use DBConnection

  alias Duckex.Port
  alias Duckex.Result

  ## ------------------------------------------------------------------
  ## gen_server Function Definitions
  ## ------------------------------------------------------------------

  @impl true
  def connect(opts) do
    {:ok, port} = Port.start_link(opts)

    state = %{port: port}

    {:ok, state}
  end

  @impl true
  def checkout(state), do: {:ok, state}

  @impl true
  def disconnect(_err, state) do
    Port.stop(state.port)

    :ok
  end

  @impl true
  def handle_begin(opts, %{} = state) do
    case Port.command(state.port, %{command: "begin"}, opts) do
      {:ok, resp} ->
        {:ok, resp, state}

      {:error, err} ->
        {:disconnect, err, state}
    end
  end

  @impl true
  def handle_close(query, opts, %{} = state) do
    case Port.command(state.port, %{command: "close", stmt: query.stmt}, opts) do
      {:ok, resp} ->
        {:ok, resp, state}

      {:error, err} ->
        {:error, err, state}
    end
  end

  @impl true
  def handle_commit(opts, %{} = state) do
    case Port.command(state.port, %{command: "commit"}, opts) do
      {:ok, resp} -> {:ok, resp, state}
      {:error, err} -> {:disconnect, err, state}
    end
  end

  @impl true
  def handle_deallocate(_query, cursor, opts, %{} = state) do
    case Port.command(state.port, %{command: "deallocate", cursor: cursor}, opts) do
      {:ok, resp} ->
        {:ok, resp, state}

      {:error, err} ->
        {:error, err, state}
    end
  end

  @impl true
  def handle_declare(query, params, opts, %{} = state) do
    case Port.command(state.port, %{command: "declare", stmt: query.stmt, params: params}, opts) do
      {:ok, resp} ->
        {:ok, query, resp, state}

      {:error, err} ->
        {:error, err, state}
    end
  end

  @impl true
  def handle_execute(query, params, opts, %{} = state) do
    case Port.command(
           state.port,
           %{
             command: "execute",
             stmt: query.stmt,
             params: params
           },
           opts
         ) do
      {:ok, resp} ->
        {:ok, query, resp, state}

      {:error, err} ->
        {:error, err, state}
    end
  end

  @impl true
  def handle_fetch(query, cursor, opts, %{} = state) do
    case Port.command(
           state.port,
           %{
             command: "execute",
             stmt: query.stmt,
             cursor: cursor
           },
           opts
         ) do
      {:ok, resp} ->
        {:ok, query, resp, state}

      {:error, err} ->
        {:error, err, state}
    end
  end

  @impl true
  def handle_prepare(query, opts, %{} = state) do
    case Port.command(state.port, %{command: "prepare", query: query.query}, opts) do
      {:ok, %Result{rows: [[stmt_id]]}} when not is_nil(stmt_id) ->
        {:ok, %{query | stmt: stmt_id}, state}

      {:ok, %Result{rows: [[nil]]}} ->
        {:error, %Duckex.Error{message: "Exhausted prepared statements cache"}, state}

      {:error, err} ->
        {:error, err, state}
    end
  end

  @impl true
  def handle_rollback(opts, %{} = state) do
    case Port.command(
           state.port,
           %{
             command: "rollback"
           },
           opts
         ) do
      {:ok, result} ->
        {:ok, result, state}

      {:error, err} ->
        {:disconnect, err, state}
    end
  end

  @impl true
  def handle_status(opts, %{} = state) do
    case Port.command(
           state.port,
           %{
             command: "status"
           },
           opts
         ) do
      {:ok, _} ->
        {:idle, state}

      {:error, _} ->
        {:error, state}
    end
  end

  @impl true
  def ping(state), do: {:ok, state}
end
