defmodule ExIceberg.Rest.Catalog do
  @moduledoc """
  Module to interact with the REST catalog of Apache Iceberg.
  """

  alias ExIceberg.Rest.Client
  alias ExIceberg.Rest.Config

  @behaviour ExIceberg.Catalog

  defstruct name: nil, config: %Config{}, client: Client

  @type t :: %__MODULE__{name: String.t(), config: Config.t(), client: Client}

  alias __MODULE__

  def new(name, config, client \\ Client) do
    %__MODULE__{name: name, config: Config.new(config), client: client}
    |> authenticate()
    |> get_config()
  end

  @impl true
  def create_namespace(
        %__MODULE__{config: config, client: client} = catalog,
        namespace,
        properties
      ) do
    body = %{"namespace" => [namespace], "properties" => properties}

    case client.request(:create_namespace, config: config, json: body) do
      {:ok, response} -> {:ok, catalog, response}
      {:error, reason} -> {:error, catalog, reason}
    end
  end

  @impl true
  def list_namespaces(%Catalog{config: config, client: client} = catalog) do
    case client.request(:list_namespace, config: config) do
      {:ok, response} -> {:ok, catalog, response}
      {:error, reason} -> {:error, catalog, reason}
    end
  end

  defp authenticate(%Catalog{config: config, client: client} = catalog)
       when config.credential != nil do
    uri = config.oauth2_server_uri || config.uri

    optional_params =
      %{audience: config.audience, resource: config.resource}
      |> Map.filter(fn {_, value} -> value != nil end)

    {client_id, client_secret} = Config.split_credential(config.credential)

    params =
      Map.merge(
        %{
          client_id: client_id,
          client_secret: client_secret,
          scope: config.scope,
          grant_type: config.grant_type
        },
        optional_params
      )

    {:ok, %{"access_token" => token}} =
      client.request(
        :get_token,
        config: %{catalog.config | uri: uri},
        form: params
      )

    %{catalog | config: %{config | token: token}}
  end

  defp authenticate(catalog), do: catalog

  defp get_config(%Catalog{config: config, client: client} = catalog) do
    params = if config.warehouse, do: [warehouse: config.warehouse], else: []

    {:ok, external_config} =
      client.request(:get_config, config: config, params: params)

    defaults_config = Config.parse(external_config["defaults"])
    overrides_config = Config.parse(external_config["overrides"])

    new_config = Config.merge(config, defaults_config, overrides_config)

    %{catalog | config: new_config}
  end
end
