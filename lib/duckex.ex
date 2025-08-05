# SPDX-FileCopyrightText: 2025 Stas Muzhyk <sts@abc3.dev>
# SPDX-FileCopyrightText: 2025 Łukasz Niemier <~@hauleth.dev>
#
# SPDX-License-Identifier: Apache-2.0

defmodule Duckex do
  @moduledoc """
  A naive DuckDB binding for Elixir via Rust port.
  Provides basic functionality to execute SQL queries against DuckDB,
  primarily for testing DuckLake functionality.
  """

  alias Duckex.Error
  alias Duckex.Protocol
  alias Duckex.Query
  alias Duckex.Result

  @type connection_option() ::
          {:attach, [attach()]}
          | DBConnection.connection_option()

  @type attach() ::
          {path :: String.t(), keyword()}
          | {path :: String.t(), keyword(), keyword()}

  @type secret() ::
          {atom(), keyword()}
          | {atom(), {keyword(), keyword()}}

  @doc false
  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]}
    }
  end

  @doc """
  Starts the DuckDB process.

  ## Options

  Duckex provides some helper options to setup connection before it is
  available.

  - `:secrets` - secrets to be set up before instance is made ready. It is list
    of tuples where first element is name of secret and second is keyword lists
    containing secret details.
  - `:attach` - list of tuples where first element is the attach string and
    the second one is list of options, optional 3rd element contains connection
    options, see `attach/4`.

  Secrets are set up before attaching connections, so you can use these secrets
  for attaching (like S3 secrets).
  """
  @spec start_link([connection_option()]) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    old = opts[:after_connect]
    attach = opts[:attach] || []
    secrets = opts[:secrets] || []

    after_connect = fn conn ->
      case old do
        nil -> :ok
        {m, f, a} -> apply(m, f, [conn | a])
        fun when is_function(fun, 1) -> fun.(conn)
      end

      Enum.each(secrets, fn
        {name, {spec, opts}} -> create_secret!(conn, name, spec, opts)
        {name, spec} -> create_secret!(conn, name, spec)
      end)

      Enum.each(attach, fn
        {name, spec} -> attach!(conn, name, spec)
        {name, spec, opts} -> attach!(conn, name, spec, opts)
      end)
    end

    opts = Keyword.put(opts, :after_connect, after_connect)

    DBConnection.start_link(Protocol, opts)
  end

  @doc """
  Prepares query.
  """
  @spec prepare(DBConnection.conn(), String.t(), list()) ::
          {:ok, Query.t()} | {:error, Error.t()}
  def prepare(conn, statement, opts \\ []) do
    DBConnection.prepare(conn, %Query{query: statement}, opts)
  end

  @doc """
  Prepares query and returns the prepared query or raises `Duckex.Error`
  if there is an error. See `prepare/3`.
  """
  @spec prepare!(DBConnection.conn(), String.t(), list()) :: Query.t()
  def prepare!(conn, statement, opts \\ []) do
    DBConnection.prepare!(conn, %Query{query: statement}, opts)
  end

  @doc """
  Prepares and executes query in the single step.
  """
  @spec prepare_execute(DBConnection.conn(), String.t(), list(), list()) ::
          {:ok, Query.t(), Result.t()} | {:error, Error.t()}
  def prepare_execute(conn, statement, params, opts \\ []) do
    DBConnection.prepare_execute(conn, %Query{query: statement}, params, opts)
  end

  @doc """
  Prepares and executes query in the single step and returns the prepared query
  or raises `Duckex.Error` if there is an error. See `prepare_execute/5`.
  """
  @spec prepare_execute!(DBConnection.conn(), String.t(), list(), list()) ::
          {Query.t(), Result.t()}
  def prepare_execute!(conn, statement, params, opts \\ []) do
    DBConnection.prepare_execute!(conn, %Query{query: statement}, params, opts)
  end

  @spec query(DBConnection.conn(), String.t(), list(), list()) ::
          {:ok, Result.t()} | {:error, Error.t()}
  def query(conn, statement, params, opts \\ []) do
    with {:ok, query, result} <- prepare_execute(conn, statement, params, opts),
         {:ok, _} <- close(conn, query) do
      {:ok, result}
    end
  end

  @spec query!(DBConnection.conn(), String.t(), list(), list()) :: Result.t()
  def query!(conn, statement, params, opts \\ []) do
    case query(conn, statement, params, opts) do
      {:ok, result} -> result
      {:error, error} -> raise error
    end
  end

  defdelegate close(pid, query, opts \\ []), to: DBConnection
  defdelegate close!(pid, query, opts \\ []), to: DBConnection

  defdelegate execute(pid, query, values, opts \\ []), to: DBConnection
  defdelegate execute!(pid, query, values, opts \\ []), to: DBConnection

  defdelegate transaction(conn, fun, opts \\ []), to: DBConnection

  @spec rollback(DBConnection.t(), reason :: any()) :: no_return()
  defdelegate rollback(conn, reason), to: DBConnection

  defp escape(val), do: String.replace(to_string(val), "'", "''")

  @doc """
  Install extensions for DuckDB.

  This will spawn temporary connection and will install extensions within that
  connection. After that each new DuckDB instance is capable of using that
  extensions.

  It is a helper function to provide a cleaner syntax - extensions use similar
  syntax to `Mix.install/2` and are either:
  - atom containing name of the extension to be installed
  - tuple in form of `{atom(), opts}`

  ## Options

  - `:source` - source from which the extension should be installed. Supported
    options are:
    + `:default` (used when option is not specified) - uses default
      registry configured for connection
    + `:core`
    + `:nightly` - equivalent of `core_nightly` from DuckDB
    + String representing URL from which the extension should be installed
  - `:force` - forcefully install extension, even when it is already installed.
    Can be used to update extension.
  """
  @spec install_extensions([extension], [connection_option()]) :: :ok | {:error, term()}
        when extension_opt:
               {:source, :default | :core | :nightly | String.t()} | {:force, boolean()},
             extension: atom() | {atom(), [extension_opt]}
  def install_extensions(extensions, opts \\ []) do
    with {:ok, conn} <- start_link(opts) do
      Enum.each(extensions, &do_install(conn, &1))

      Process.exit(conn, :normal)

      :ok
    end
  end

  defp do_install(conn, name) when is_atom(name), do: do_install(conn, {name, []})

  defp do_install(conn, {name, opts}) do
    from =
      case opts[:source] do
        :core -> " FROM core"
        :nightly -> " FROM core_nightly"
        nil -> ""
        :default -> ""
        repo when is_binary(repo) -> " FROM '#{escape(repo)}'"
      end

    force =
      if opts[:force] do
        "FORCE "
      else
        ""
      end

    query!(conn, "#{force}INSTALL #{name}#{from}", [])
  end

  def attach(conn, path, opts \\ [], conn_opts \\ []) do
    path = escape(path)
    as = if val = opts[:as], do: " AS #{val}"
    options = format_attach_options(opts[:options])

    query(conn, "ATTACH '#{path}'#{as} (#{options})", [], conn_opts)
  end

  def attach!(conn, path, opts \\ [], conn_opts \\ []) do
    case attach(conn, path, opts, conn_opts) do
      {:ok, result} -> result
      {:error, error} -> raise error
    end
  end

  defp format_attach_options(nil), do: ""

  defp format_attach_options(opts) do
    opts
    |> Enum.flat_map(fn
      {_key, false} -> []
      {key, true} -> ["#{key}"]
      {key, value} when is_atom(value) -> ["#{key} #{value}"]
      {key, value} -> ["#{key} '#{escape(to_string(value))}'"]
    end)
    |> Enum.join(", ")
  end

  def create_secret(conn, name, spec, opts \\ []) do
    {spec, params} = format_secret_options(spec)

    or_replace =
      if opts[:or_replace] do
        " OR REPLACE"
      else
        ""
      end

    Duckex.query(conn, "CREATE#{or_replace} SECRET #{name} (#{spec})", params, opts)
  end

  def create_secret!(conn, path, spec, opts \\ []) do
    case create_secret(conn, path, spec, opts) do
      {:ok, result} -> result
      {:error, error} -> raise error
    end
  end

  defp format_secret_options(opts) do
    {query, params} =
      Enum.map_reduce(opts, [], fn
        {name, val}, acc when is_atom(val) ->
          {"#{name} #{val}", acc}

        {name, val}, acc ->
          {"#{name} ?", [val | acc]}
      end)

    {Enum.join(query, ", "), Enum.reverse(params)}
  end

  # XXX: Currently unsupported
  # defdelegate stream(conn, query, params, opts \\ []), to: DBConnection
end
