# SPDX-FileCopyrightText: 2025 Stas Muzhyk <sts@abc3.dev>
# SPDX-FileCopyrightText: 2025 Łukasz Niemier <~@hauleth.dev>
#
# SPDX-License-Identifier: Apache-2.0

defmodule Duckex.Query do
  @moduledoc """
  Query struct returned after successfully preparing query.
  """

  @type t :: %__MODULE__{
          query: String.t(),
          stmt: String.t(),
          columns: list(),
          rows: list()
        }

  defstruct [:query, :stmt, :columns, :rows]

  defimpl DBConnection.Query do
    def decode(_query, %Duckex.Result{} = result, _opts) do
      rows =
        for row <- result.rows do
          Duckex.Result.decode_row(row, result.columns)
        end

      %{result | rows: rows}
    end

    def describe(query, _opts), do: query

    def encode(_query, params, _opts), do: params

    def parse(query, _opts), do: query
  end

  defimpl String.Chars do
    def to_string(%@for{} = query), do: query.query
  end
end
