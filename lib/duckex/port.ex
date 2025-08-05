# SPDX-FileCopyrightText: 2025 Stas Muzhyk <sts@abc3.dev>
# SPDX-FileCopyrightText: 2025 Łukasz Niemier <~@hauleth.dev>
#
# SPDX-License-Identifier: Apache-2.0

defmodule Duckex.Port do
  @moduledoc false

  use GenServer

  require Logger

  alias Duckex.Error
  alias Duckex.Result

  @default_timeout :timer.seconds(15)

  ## ------------------------------------------------------------------
  ## API Function Definitions
  ## ------------------------------------------------------------------

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  def command(pid, message, opts) do
    timeout = opts[:timeout] || @default_timeout

    GenServer.call(pid, {:command, message}, timeout)
    |> process_response(message)
  end

  def stop(pid, timeout \\ :timer.seconds(25)), do: GenServer.stop(pid, timeout)

  ## ------------------------------------------------------------------
  ## gen_server Function Definitions
  ## ------------------------------------------------------------------

  @impl true
  def init(_opts) do
    cmd = get_binary_path()

    port =
      Port.open({:spawn, cmd}, [
        # 5 KiB
        {:line, 5_120},
        :use_stdio,
        :binary,
        :exit_status
      ])

    state =
      %{
        port: port,
        caller: nil,
        buf: []
      }

    {:ok, state}
  end

  @impl true
  def handle_call({:command, command}, from, state) do
    send_command(state.port, command)

    {:noreply, %{state | caller: from}}
  end

  @impl true
  def handle_info({port, {:data, {:noeol, msg}}}, %{port: port} = state) do
    {:noreply, %{state | buf: [state.buf | msg]}}
  end

  def handle_info({port, {:data, {:eol, msg}}}, %{port: port, caller: caller} = state)
      when not is_nil(caller) do
    reply =
      case [state.buf | msg]
           |> IO.iodata_to_binary()
           |> JSON.decode() do
        {:ok, res} ->
          Logger.debug("duckex <- #{inspect(res)}", domain: [:duckex, :receive])

          {:ok, res}

        {:error, err} ->
          {:error, err}
      end

    GenServer.reply(state.caller, reply)

    {:noreply, %{state | buf: [], caller: nil}}
  end

  def handle_info({port, {:exit_status, status}}, %{port: port} = state) do
    Logger.error("Unexpected exit of Duckex process with status: #{status}")

    {:stop, {:unexpected_exit, status}, state}
  end

  def handle_info(msg, state) do
    Logger.warning("Unexpected message: #{inspect(msg)}")
    {:noreply, state}
  end

  ## ------------------------------------------------------------------
  ## Internal Function Definitions
  ## ------------------------------------------------------------------

  defp get_binary_path do
    Path.join([:code.priv_dir(:duckex), "native", "duckex"])
  end

  defp send_command(port, command) do
    Logger.debug("duckex -> #{inspect(command)}", domain: [:duckex, :send])

    msg_bin = JSON.encode!(command)

    Port.command(port, [msg_bin, ?\n])
  end

  defp process_response({:error, msg}, query) do
    {:error,
     %Error{
       message: msg,
       query: query
     }}
  end

  defp process_response({:ok, %{"status" => "ok"} = response}, _message) do
    columns = response["columns"] || []
    rows = response["rows"] || []
    num_rows = response["num_rows"] || 0

    {:ok,
     %Result{
       columns: columns,
       rows: rows,
       num_rows: num_rows
     }}
  end

  defp process_response({:ok, %{"status" => "error"} = response}, query) do
    {:error,
     %Error{
       message: response["message"],
       query: query
     }}
  end

  # @spec ts() :: non_neg_integer()
  # def ts(), do: System.monotonic_time(:millisecond)
end
