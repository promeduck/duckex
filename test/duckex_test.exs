defmodule DuckexTest do
  use ExUnit.Case
  doctest Duckex

  test "greets the world" do
    assert Duckex.hello() == :world
  end
end
