defmodule ExIceberg.Rest.Catalog do
  @moduledoc """
  Module to interact with the REST catalog of Apache Iceberg.
  """

  alias ExIceberg.Rest.Client
  alias ExIceberg.Rest.Config

  @behaviour ExIceberg.Catalog

  defstruct name: nil, config: %Config{}

  @type t :: %__MODULE__{name: String.t(), config: Config.t()}

  alias __MODULE__

  def new(name, config) do
    %__MODULE__{name: name, config: Config.new(config)}
    |> authenticate()
    |> get_config()
  end

  @impl true
  def create_namespace(
        %__MODULE__{config: config} = catalog,
        namespace,
        properties
      ) do
    body = %{"namespace" => [namespace], "properties" => properties}

    case Client.request(:create_namespace, config: config, json: body) do
      {:ok, response} -> {:ok, catalog, response}
      {:error, reason} -> {:error, catalog, reason}
    end
  end

  @impl true
  def list_namespaces(%Catalog{config: config} = catalog) do
    case Client.request(:list_namespace, config: config) do
      {:ok, response} -> {:ok, catalog, response}
      {:error, reason} -> {:error, catalog, reason}
    end
  end

  defp authenticate(%Catalog{config: %{credential: cred}} = catalog) when cred != nil do
    {:ok, token} = Client.request(:auth, config: catalog.config)
    %{catalog | config: %{catalog.config | token: token}}
  end

  defp authenticate(catalog), do: catalog

  defp get_config(%Catalog{config: config} = catalog) do
    Client.request(:get_config, config: config)

    # TODO: Parse config and merge
    # (default_config (from api) + user_config + override_config(from api))
    # We need to list all the possible configurations. I know that in the
    # other implementations, it's all dynamic, but I believe that with explicit configuration,
    # we can have better control over the code.

    catalog
  end
end
