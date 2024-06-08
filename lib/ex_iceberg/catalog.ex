defmodule ExIceberg.Catalog do
  @callback create_namespace(t(), String.t(), map()) :: :ok | {:error, String.t()}

  @type t :: %{
          name: String.t(),
          properties: map()
        }
end
