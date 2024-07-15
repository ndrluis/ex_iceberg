defmodule ExIceberg.Catalog do
  @callback create_namespace(t(), String.t(), map()) :: {:ok, t(), map()} | {:error, String.t()}
  @callback list_namespaces(t()) :: {:ok, t(), list(String.t())} | {:error, String.t()}
  @callback drop_namespace(t(), String.t()) :: {:ok, t(), map()} | {:error, String.t()}
  @callback load_namespace_metadata(t(), String.t()) :: {:ok, t(), map()} | {:error, String.t()}
  @callback namespace_exists?(t(), String.t()) :: {:ok, t(), boolean()} | {:error, String.t()}
  @callback update_namespace_properties(t(), String.t(), list(), map()) ::
              {:ok, t(), map()} | {:error, String.t()}

  @type t :: %{
          name: String.t(),
          config: ExIceberg.Config.t()
        }
end
