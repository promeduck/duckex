defmodule Duckex.MixProject do
  use Mix.Project

  def project do
    [
      app: :duckex,
      version: "0.1.0",
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:jason, "~> 1.4"}
    ]
  end

  defp aliases do
    [
      compile: ["compile.rust", "compile"],
      "compile.rust": &compile_rust/1,
      setup: ["deps.get", "compile.rust"]
    ]
  end

  defp compile_rust(_args) do
    IO.puts("Building Rust native binary...")

    File.mkdir_p!("priv")

    {result, exit_code} = System.cmd("cargo", ["build", "--release"], cd: "native")
    IO.puts(result)

    if exit_code != 0 do
      raise "Failed to compile Rust binary"
    end

    source_path = Path.join(["native", "target", "release", "duckex"])
    dest_path = Path.join(["priv", "duckex"])

    if File.exists?(source_path) do
      File.cp!(source_path, dest_path)
      File.chmod!(dest_path, 0o755)
      IO.puts("Rust binary copied to #{dest_path}")
    else
      raise "Rust binary not found at #{source_path}"
    end
  end
end
