# SPDX-FileCopyrightText: 2025 Stas Muzhyk <sts@abc3.dev>
# SPDX-FileCopyrightText: 2025 Łukasz Niemier <~@hauleth.dev>
#
# SPDX-License-Identifier: Apache-2.0

defmodule Duckex.MixProject do
  use Mix.Project

  def project do
    [
      app: :duckex,
      description: "DuckDB client library",
      version: "0.1.0",
      elixir: "~> 1.14",
      compilers: [:native] ++ Mix.compilers(),
      aliases: aliases(),
      docs: docs(),
      deps: deps(),
      package: package()
    ]
  end

  defp deps do
    [
      {:db_connection, "~> 2.8"},
      {:ex_doc, ">= 0.0.0", only: [:dev], runtime: false},
      {:credo, ">= 0.0.0", only: [:dev], runtime: false},
      {:dialyxir, ">= 0.0.0", only: [:dev], runtime: false}
    ]
  end

  defp package do
    [
      files: ~W[
        mix.exs
        lib
        .formatter.exs
        README*
        LICENSES
        native
      ],
      licenses: [
        "Apache-2.0"
      ],
      links: %{
        "GitHub" => "https://github.com/promeduck/duckex",
        "DuckDB" => "https://duckdb.org"
      }
    ]
  end

  defp docs do
    [
      extras:
        [
          "README.md"
        ] ++ Path.wildcard("LICENSES/*"),
      groups_for_extras: [
        Licenses: Path.wildcard("LICENSES/*")
      ]
    ]
  end

  defp aliases do
    [
      "compile.native": &native_build/1
    ]
  end

  defp native_build(_args) do
    IO.puts("Building Rust native binary...")

    File.mkdir_p!("priv")

    {result, exit_code} = System.cmd("cargo", ["build", "--release"])
    IO.puts(result)

    if exit_code != 0 do
      raise "Failed to compile Rust binary"
    end

    source_path = Path.join(["target", "release", "duckex"])
    dest_path = Path.join(["priv", "native", "duckex"])

    if File.exists?(source_path) do
      File.cp!(source_path, dest_path)
      File.chmod!(dest_path, 0o755)
      IO.puts("Rust binary copied to #{dest_path}")
    else
      raise "Rust binary not found at #{source_path}"
    end
  end
end
