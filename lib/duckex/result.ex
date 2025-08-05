# SPDX-FileCopyrightText: 2025 Stas Muzhyk <sts@abc3.dev>
# SPDX-FileCopyrightText: 2025 Łukasz Niemier <~@hauleth.dev>
#
# SPDX-License-Identifier: Apache-2.0

defmodule Duckex.Result do
  @moduledoc """
  Result struct returned from any successful query. Its fields are:

  - `columns` - list of field names in form of `[name, type]`
  - `rows` - list of rows, each row is represented as list of fields that
    corresponds to `:column` order
  - `num_rows` - count of rows in `:rows` field
  """

  @type t :: %__MODULE__{
          columns: [[String.t()]],
          rows: [[any()]],
          num_rows: integer
        }

  defstruct [:columns, :rows, :num_rows]

  @doc false
  def decode_row([], []), do: []

  def decode_row([value | vs], [[_name, type] | cs]) do
    [decode_val(value, type) | decode_row(vs, cs)]
  end

  defp decode_val(us, "Timestamp(" <> _) do
    DateTime.from_unix!(us, :microsecond)
  end

  defp decode_val(val, _), do: val
end
