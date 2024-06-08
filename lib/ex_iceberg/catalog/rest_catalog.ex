defmodule ExIceberg.Catalog.RestCatalog do
  @moduledoc """
  Module to interact with the REST catalog of Apache Iceberg.
  """

  @behaviour ExIceberg.Catalog

  defstruct name: nil, properties: %{}, http_client: ExIceberg.ReqClient

  @type t :: %__MODULE__{
          name: String.t(),
          properties: map(),
          http_client: module()
        }

  @doc """
  Initializes a new RestCatalog.

  ## Parameters

    - `name`: The name of the catalog
    - `properties`: A map of properties for the catalog

  ## Examples

      iex> ExIceberg.Catalog.RestCatalog.new("my_catalog", %{"uri" => "http://localhost:8181"})
      %ExIceberg.Catalog.RestCatalog{
        name: "my_catalog",
        properties: %{"uri" => "http://localhost:8181"},
        http_client: ExIceberg.ReqClient
      }
  """
  def new(name, properties, http_client \\ ExIceberg.ReqClient) do
    %__MODULE__{
      name: name,
      properties: properties,
      http_client: http_client
    }
  end

  @doc """
  Creates a new namespace in the catalog.

  ## Parameters

    - `catalog`: The catalog struct
    - `namespace`: The name of the namespace
    - `properties`: A map of properties for the namespace

  ## Examples

      iex> catalog = ExIceberg.Catalog.RestCatalog.new("my_catalog", %{"uri" => "http://localhost:8181"})
      iex> ExIceberg.Catalog.RestCatalog.create_namespace(catalog, "new_namespace", %{"property" => "value"})
      :ok
  """
  @impl true
  def create_namespace(
        %__MODULE__{properties: %{"uri" => uri}, http_client: http_client} = _catalog,
        namespace,
        properties
      ) do
    url = uri <> "/v1/namespaces"
    body = %{"namespace" => [namespace], "properties" => properties}
    headers = [{"Content-Type", "application/json"}]

    case http_client.request(:post, url, body, headers) do
      {:ok, _body} ->
        :ok

      {:error, %Req.Response{status: status, body: %{"error" => %{"message" => message}}}} ->
        {:error, "Request failed with status #{status}: #{message}"}

      {:error, %Req.TransportError{reason: reason}} ->
        {:error, "HTTP request failed: #{inspect(reason)}"}
    end
  end
end
