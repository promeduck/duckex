# SPDX-FileCopyrightText: 2025 Stas Muzhyk <sts@abc3.dev>
# SPDX-FileCopyrightText: 2025 Łukasz Niemier <~@hauleth.dev>
#
# SPDX-License-Identifier: Apache-2.0

defmodule DuckexTest do
  use ExUnit.Case, async: true

  @subject Duckex

  doctest @subject

  setup do
    conn = start_supervised!({@subject, attach: []})

    {:ok, conn: conn}
  end

  describe "query/3" do
    test "simple query", %{conn: conn} do
      @subject.query!(conn, "CREATE TABLE person (name TEXT, data INTEGER)", [])
      @subject.query!(conn, "INSERT INTO person (name, data) VALUES (?, ?)", ["Foo", 1])

      assert {:ok,
              %Duckex.Result{
                columns: [["name", "Utf8"], ["data", "Int32"]],
                rows: [["Foo", 1]]
              }} = @subject.query(conn, "SELECT name, data FROM person", [])
    end

    test "errors on invalid query", %{conn: conn} do
      assert {:error, %Duckex.Error{}} = @subject.query(conn, "SELECT name, data FROM person", [])

      assert {:error, %Duckex.Error{}} =
               @subject.query(conn, "SELECT error(?) FROM unnest([1])", ["Some Message"])
    end

    test "large response", %{conn: conn} do
      @subject.query!(conn, "CREATE TABLE person (name TEXT, data INTEGER)", [])

      @subject.transaction(conn, fn tx ->
        for i <- 1..500 do
          @subject.query!(tx, "INSERT INTO person (name, data) VALUES (?, ?)", ["Foo", i])
        end
      end)

      assert {:ok,
              %Duckex.Result{
                columns: [["name", "Utf8"], ["data", "Int32"]],
                rows: rows,
                num_rows: 500
              }} = @subject.query(conn, "SELECT name, data FROM person", [])

      assert length(rows) == 500
    end
  end

  describe "query!/3" do
    test "simple query", %{conn: conn} do
      @subject.query(conn, "CREATE TABLE person (name TEXT, data INTEGER)", [])
      @subject.query(conn, "INSERT INTO person (name, data) VALUES (?, ?)", ["Foo", 1])

      assert %Duckex.Result{
               columns: [["name", "Utf8"], ["data", "Int32"]],
               rows: [["Foo", 1]]
             } = @subject.query!(conn, "SELECT name, data FROM person", [])
    end

    test "raises on invalid query", %{conn: conn} do
      assert_raise Duckex.Error, fn ->
        @subject.query!(conn, "SELECT name, data FROM person", [])
      end

      assert_raise Duckex.Error, fn ->
        @subject.query!(conn, "SELECT error(?) FROM unnest([1])", ["Some Message"])
      end
    end
  end

  describe "prepare/2" do
    test "simple query", %{conn: conn} do
      @subject.query!(conn, "CREATE TABLE person (name TEXT, data INTEGER)", [])

      assert {:ok, %Duckex.Query{}} = @subject.prepare(conn, "SELECT name, data FROM person")
    end

    test "invalid query", %{conn: conn} do
      assert {:error, %Duckex.Error{}} =
               @subject.prepare(conn, "SELECT name, data FROM non_existent")
    end
  end

  describe "close/2" do
    test "prepared statement can be closed", %{conn: conn} do
      @subject.query!(conn, "CREATE TABLE person (name TEXT, data INTEGER)", [])

      assert {:ok, query} = @subject.prepare(conn, "SELECT name, data FROM person")
      assert {:ok, _} = @subject.close(conn, query)
    end

    test "closing is idempotent", %{conn: conn} do
      @subject.query!(conn, "CREATE TABLE person (name TEXT, data INTEGER)", [])

      assert {:ok, query} = @subject.prepare(conn, "SELECT name, data FROM person")
      assert {:ok, _} = @subject.close(conn, query)
      assert {:ok, _} = @subject.close(conn, query)
    end
  end

  describe "close!/2" do
    test "prepared statement can be closed", %{conn: conn} do
      @subject.query!(conn, "CREATE TABLE person (name TEXT, data INTEGER)", [])

      assert {:ok, query} = @subject.prepare(conn, "SELECT name, data FROM person")
      assert _ = @subject.close!(conn, query)
    end

    test "closing is idempotent", %{conn: conn} do
      @subject.query!(conn, "CREATE TABLE person (name TEXT, data INTEGER)", [])

      assert {:ok, query} = @subject.prepare(conn, "SELECT name, data FROM person")
      assert _ = @subject.close!(conn, query)
      assert _ = @subject.close!(conn, query)
    end
  end

  describe "prepare!/2" do
    test "simple query", %{conn: conn} do
      @subject.query!(conn, "CREATE TABLE person (name TEXT, data INTEGER)", [])

      assert %Duckex.Query{} = @subject.prepare!(conn, "SELECT name, data FROM person")
    end

    test "prepared query can be executed", %{conn: conn} do
      @subject.query!(conn, "CREATE TABLE person (name TEXT, data INTEGER)", [])

      @subject.query!(conn, "INSERT INTO person (name, data) VALUES (?, ?), (?, ?)", [
        "Foo",
        1,
        "Bar",
        2
      ])

      assert query = @subject.prepare!(conn, "SELECT name, data FROM person")
      assert {:ok, _, _} = @subject.execute(conn, query, [])
    end

    test "prepared statements cache exhaustion", %{conn: conn} do
      @subject.query!(conn, "CREATE TABLE person (name TEXT, data INTEGER)", [])

      assert_raise Duckex.Error, "Exhausted prepared statements cache", fn ->
        for _ <- 0..2000 do
          @subject.prepare!(conn, "SELECT name, data FROM person")
        end
      end
    end

    test "prepared query can be executed with other params", %{conn: conn} do
      @subject.query!(conn, "CREATE TABLE person (name TEXT, data INTEGER)", [])

      @subject.query!(conn, "INSERT INTO person (name, data) VALUES (?, ?), (?, ?)", [
        "Foo",
        1,
        "Bar",
        2
      ])

      assert query = @subject.prepare!(conn, "SELECT name, data FROM person WHERE data = ?")
      assert {:ok, _, result1} = @subject.execute(conn, query, [1])
      assert {:ok, _, result2} = @subject.execute(conn, query, [2])
      assert result1 != result2
    end

    test "invalid query", %{conn: conn} do
      assert_raise Duckex.Error, fn ->
        @subject.prepare!(conn, "SELECT name, data FROM non_existent")
      end
    end
  end

  describe "execute/3" do
    test "fails when called with invalid params", %{conn: conn} do
      @subject.query!(conn, "CREATE TABLE person (name TEXT, data INTEGER)", [])

      @subject.query!(conn, "INSERT INTO person (name, data) VALUES (?, ?), (?, ?)", [
        "Foo",
        1,
        "Bar",
        2
      ])

      assert query = @subject.prepare!(conn, "SELECT name, data FROM person WHERE data = ?")
      assert {:error, _} = @subject.execute(conn, query, ["foo"])
    end
  end

  describe "execute!/3" do
    test "prepared query can be executed", %{conn: conn} do
      @subject.query!(conn, "CREATE TABLE person (name TEXT, data INTEGER)", [])

      @subject.query!(conn, "INSERT INTO person (name, data) VALUES (?, ?), (?, ?)", [
        "Foo",
        1,
        "Bar",
        2
      ])

      assert query = @subject.prepare!(conn, "SELECT name, data FROM person")
      assert _ = @subject.execute!(conn, query, [])
    end

    test "prepared query can be executed with other params", %{conn: conn} do
      @subject.query!(conn, "CREATE TABLE person (name TEXT, data INTEGER)", [])

      @subject.query!(conn, "INSERT INTO person (name, data) VALUES (?, ?), (?, ?)", [
        "Foo",
        1,
        "Bar",
        2
      ])

      assert query = @subject.prepare!(conn, "SELECT name, data FROM person WHERE data = ?")
      assert result1 = @subject.execute!(conn, query, [1])
      assert result2 = @subject.execute!(conn, query, [2])
      assert result1 != result2
    end

    test "fails when called with invalid params", %{conn: conn} do
      @subject.query!(conn, "CREATE TABLE person (name TEXT, data INTEGER)", [])

      @subject.query!(conn, "INSERT INTO person (name, data) VALUES (?, ?), (?, ?)", [
        "Foo",
        1,
        "Bar",
        2
      ])

      assert query = @subject.prepare!(conn, "SELECT name, data FROM person WHERE data = ?")

      assert_raise Duckex.Error, fn ->
        @subject.execute!(conn, query, ["foo"])
      end
    end
  end

  describe "prepare_execute/2" do
    test "simple query", %{conn: conn} do
      @subject.query!(conn, "CREATE TABLE person (name TEXT, data INTEGER)", [])

      @subject.query!(conn, "INSERT INTO person (name, data) VALUES (?, ?), (?, ?)", [
        "Foo",
        1,
        "Bar",
        2
      ])

      assert {:ok, %Duckex.Query{}, result} =
               @subject.prepare_execute(conn, "SELECT name, data FROM person WHERE data = ?", [1])

      assert %Duckex.Result{
               columns: [["name", "Utf8"], ["data", "Int32"]],
               rows: [["Foo", 1]]
             } = result
    end

    test "returned query can be ran with different params", %{conn: conn} do
      @subject.query!(conn, "CREATE TABLE person (name TEXT, data INTEGER)", [])

      @subject.query!(conn, "INSERT INTO person (name, data) VALUES (?, ?), (?, ?)", [
        "Foo",
        1,
        "Bar",
        2
      ])

      assert {:ok, query, result1} =
               @subject.prepare_execute(conn, "SELECT name, data FROM person WHERE data = ?", [1])

      assert {:ok, %Duckex.Query{}, result2} = @subject.execute(conn, query, [2])
      assert result1 != result2
    end

    test "invalid query", %{conn: conn} do
      assert {:error, %Duckex.Error{}} =
               @subject.prepare(conn, "SELECT name, data FROM non_existent")
    end
  end

  describe "prepare_execute!/2" do
    test "simple query", %{conn: conn} do
      @subject.query!(conn, "CREATE TABLE person (name TEXT, data INTEGER)", [])

      @subject.query!(conn, "INSERT INTO person (name, data) VALUES (?, ?), (?, ?)", [
        "Foo",
        1,
        "Bar",
        2
      ])

      assert {%Duckex.Query{}, result} =
               @subject.prepare_execute!(conn, "SELECT name, data FROM person WHERE data = ?", [1])

      assert %Duckex.Result{
               columns: [["name", "Utf8"], ["data", "Int32"]],
               rows: [["Foo", 1]]
             } = result
    end

    test "returned query can be ran with different params", %{conn: conn} do
      @subject.query!(conn, "CREATE TABLE person (name TEXT, data INTEGER)", [])

      @subject.query!(conn, "INSERT INTO person (name, data) VALUES (?, ?), (?, ?)", [
        "Foo",
        1,
        "Bar",
        2
      ])

      assert {query, result1} =
               @subject.prepare_execute!(conn, "SELECT name, data FROM person WHERE data = ?", [1])

      assert {:ok, %Duckex.Query{}, result2} = @subject.execute(conn, query, [2])
      assert result1 != result2
    end

    test "invalid query", %{conn: conn} do
      assert_raise Duckex.Error, fn ->
        @subject.prepare!(conn, "SELECT name, data FROM non_existent")
      end
    end
  end

  describe "transaction/2" do
    test "simple", %{conn: conn} do
      @subject.query(conn, "CREATE TABLE person (name TEXT, data INTEGER)", [])

      @subject.transaction(conn, fn tx ->
        @subject.query!(tx, "INSERT INTO person (name, data) VALUES (?, ?)", ["Foo", 21])
        @subject.query!(tx, "INSERT INTO person (name, data) VALUES (?, ?)", ["Bar", 37])
      end)

      assert %Duckex.Result{
               columns: [["name", "Utf8"], ["data", "Int32"]],
               rows: [["Foo", 21], ["Bar", 37]]
             } = @subject.query!(conn, "SELECT name, data FROM person", [])
    end

    test "failed", %{conn: conn} do
      @subject.query(conn, "CREATE TABLE person (name TEXT, data INTEGER)", [])
      @subject.query!(conn, "INSERT INTO person (name, data) VALUES (?, ?)", ["Foo", 21])

      assert {:error, :errored_out} ==
               @subject.transaction(conn, fn tx ->
                 @subject.query!(tx, "INSERT INTO person (name, data) VALUES (?, ?)", ["Bar", 37])
                 @subject.rollback(tx, :errored_out)
               end)

      assert %Duckex.Result{
               columns: [["name", "Utf8"], ["data", "Int32"]],
               rows: [["Foo", 21]]
             } = @subject.query!(conn, "SELECT name, data FROM person", [])
    end
  end

  describe "types conversion" do
    test "timestamp", %{conn: conn} do
      dt = DateTime.utc_now()
      @subject.query(conn, "CREATE TABLE a (ts TIMESTAMP)", [])
      @subject.query!(conn, "INSERT INTO a VALUES (?)", [dt])

      assert %Duckex.Result{
               rows: [[^dt]]
             } = @subject.query!(conn, "SELECT * FROM a", [])
    end

    test "array", %{conn: conn} do
      arr = [2, 1, 3, 7]
      @subject.query(conn, "CREATE TABLE a (v INTEGER[4])", [])
      # TODO: Add support for encoding Arrays/Lists
      @subject.query!(conn, "INSERT INTO a VALUES (?::INTEGER[4])", [JSON.encode!(arr)])

      assert %Duckex.Result{
               rows: [[^arr]]
             } = @subject.query!(conn, "SELECT * FROM a", [])
    end

    test "list", %{conn: conn} do
      arr = [2, 1, 3, 7]
      @subject.query(conn, "CREATE TABLE a (v INTEGER[])", [])
      # TODO: Add support for encoding Arrays/Lists
      @subject.query!(conn, "INSERT INTO a VALUES (?::INTEGER[])", [JSON.encode!(arr)])

      assert %Duckex.Result{
               rows: [[^arr]]
             } = @subject.query!(conn, "SELECT * FROM a", [])
    end
  end

  # Currently unsupported
  # describe "stream/3" do
  #   test "simple query", %{conn: conn} do
  #     @subject.query!(conn, "CREATE TABLE person (name TEXT, data INTEGER)", [])
  #
  #     @subject.query!(conn, "INSERT INTO person (name, data) VALUES (?, ?), (?, ?)", [
  #       "Foo",
  #       1,
  #       "Bar",
  #       2
  #     ])
  #
  #     query = @subject.prepare!(conn, "SELECT * FROM person")
  #
  #     @subject.transaction(conn, fn tx ->
  #       @subject.stream(tx, query, [])
  #       |> Enum.to_list()
  #     end)
  #   end
  # end
end
