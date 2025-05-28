defmodule Duckex.Port do
  @moduledoc false
  use GenServer
  require Logger

  alias Duckex.Result
  alias Duckex.Error

  ## ------------------------------------------------------------------
  ## API Function Definitions
  ## ------------------------------------------------------------------

  def start_link(), do: start_link(%{})
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  def execute(sql, values \\ [], timeout \\ :timer.seconds(25)) do
    start_time = System.monotonic_time(:millisecond)

    result =
      GenServer.call(
        __MODULE__,
        {:command,
         %{
           command: "execute",
           sql: sql,
           values: values
         }},
        timeout
      )

    exec_time_ms = System.monotonic_time(:millisecond) - start_time
    process_response(result, sql, exec_time_ms)
  end

  def query(sql, values \\ [], timeout \\ :timer.seconds(25)) do
    start_time = System.monotonic_time(:millisecond)

    result =
      GenServer.call(
        __MODULE__,
        {:command,
         %{
           command: "query",
           sql: sql,
           values: values
         }},
        timeout
      )

    exec_time_ms = System.monotonic_time(:millisecond) - start_time
    process_response(result, sql, exec_time_ms)
  end

  def stop(timeout \\ :timer.seconds(25)), do: GenServer.stop(__MODULE__, timeout)

  ## ------------------------------------------------------------------
  ## gen_server Function Definitions
  ## ------------------------------------------------------------------

  @impl true
  def init(_opts) do
    Process.flag(:trap_exit, true)

    # Logger.info("Starting port process")

    state =
      %{
        port: nil,
        # status: :busy,
        caller: nil
        # requested_at: 0,
        # result: nil,
        # sql: nil,
        # query_mq: :queue.new(),
        # jvm_logs?: opts[:logs?] || false,
        # warehouse_path: warehouse_path,
        # catalog_name: catalog_name,
        # executors_count: opts[:executors_count] || 1
      }

    {:ok, state, {:continue, :start_port}}
    # end
  end

  @impl true
  def handle_continue(:start_port, state) do
    _configs = []

    cmd = get_binary_path()

    # Logger.info("Starting DuckDB process with command: #{cmd}")

    port =
      Port.open({:spawn, cmd}, [
        :stderr_to_stdout,
        :use_stdio,
        :binary,
        :exit_status
      ])

    {:noreply, %{state | port: port}}
  end

  @impl true
  def handle_call({:command, command}, from, state) do
    send_command(state.port, command)
    {:noreply, %{state | caller: from}}
  end

  @impl true
  def handle_info({_, {:data, msg}}, state) do
    # Logger.debug("Received data: #{inspect(msg)}")

    res =
      try do
        # res = :erlang.binary_to_term(msg)
        res = Jason.decode!(msg)
        {:ok, res}
      rescue
        error ->
          msg = "Error: #{inspect(error)}"
          {:error, msg}
      end

    caller =
      case res do
        {:ok, res} ->
          GenServer.reply(state.caller, res)
          nil

        {:error, msg} ->
          Logger.error("Error processing response: #{msg}")
          GenServer.reply(state.caller, {:error, msg})
          nil
      end

    {:noreply, %{state | caller: caller}}
  end

  def handle_info(msg, state) do
    Logger.warning("Unexpected message: #{inspect(msg)}")
    {:noreply, state}
  end

  ## ------------------------------------------------------------------
  ## Internal Function Definitions
  ## ------------------------------------------------------------------

  defp get_binary_path do
    if System.get_env("DUCKEX_DEV") == "true" do
      Path.join(["native", "target", "debug", "duckex"])
    else
      Path.join([:code.priv_dir(:duckex), "duckex"])
    end
  end

  defp send_command(port, command) do
    # msg_bin = :erlang.term_to_binary(command)
    msg_bin = Jason.encode!(command)
    # Logger.debug("Sending command: #{inspect(command)}")
    Port.command(port, [msg_bin])
  end

  defp process_response({:error, msg}, _sql, exec_time_ms) do
    %Error{
      message: msg,
      exec_time_ms: exec_time_ms
    }
  end

  defp process_response(%{"status" => "ok"} = response, _sql, exec_time_ms) do
    columns = response["columns"] || []
    rows = response["rows"] || []
    num_rows = length(rows)

    %Result{
      message: response["message"] || "Query completed successfully",
      columns: columns,
      rows: rows,
      num_rows: num_rows,
      exec_time_ms: exec_time_ms
    }
  end

  defp process_response(%{"status" => "error"} = response, _sql, exec_time_ms) do
    %Error{
      message: response["message"],
      exec_time_ms: exec_time_ms
    }
  end

  defp process_response(other, _sql, exec_time_ms) do
    %Error{
      message: "Unexpected response format: #{inspect(other)}",
      exec_time_ms: exec_time_ms
    }
  end

  # @spec ts() :: non_neg_integer()
  # def ts(), do: System.monotonic_time(:millisecond)
end
