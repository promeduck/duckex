# SPDX-FileCopyrightText: 2025 Stas Muzhyk <sts@abc3.dev>
# SPDX-FileCopyrightText: 2025 Łukasz Niemier <~@hauleth.dev>
#
# SPDX-License-Identifier: Apache-2.0

defmodule Duckex.Error do
  @type t :: %__MODULE__{
          message: String.t(),
          query: map()
        }

  defexception [:message, :query]
end
