defmodule Duckex.Error do
  @type t :: %__MODULE__{
          message: String.t(),
          exec_time_ms: integer
        }

  defstruct [:message, :exec_time_ms]
end
