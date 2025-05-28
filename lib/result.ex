defmodule Duckex.Result do
  @type t :: %__MODULE__{
          message: String.t(),
          columns: [String.t()],
          rows: [any()],
          num_rows: integer,
          exec_time_ms: integer
        }

  defstruct [:message, :columns, :rows, :num_rows, :exec_time_ms]
end
