defmodule ExIceberg.Catalog do
  @callback create_namespace(t(), String.t(), map()) :: {:ok, t(), map()} | {:error, String.t()}
  @callback list_namespaces(t()) :: {:ok, t(), list(String.t())} | {:error, String.t()}

  @type t :: %{
          name: String.t(),
          config: ExIceberg.Config.t()
        }
end
