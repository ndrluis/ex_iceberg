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
    base_url = config.oauth2_server_uri || config.uri

    optional_params =
      %{audience: config.audience, resource: config.resource}
      |> Map.filter(fn {_, value} -> value != nil end)

    {client_id, client_secret} = Config.split_credential(config.credential)

    params =
      Map.merge(
        %{
          client_id: client_id,
          client_secret: client_secret,
          scope: config.scope
        },
        optional_params
      )

    {:ok, token} =
      client.request(
        :get_token,
        config: catalog.config,
        base_url: base_url,
        form: params
      )

    %{catalog | config: %{config | token: token}}
  end

  defp authenticate(catalog), do: catalog

  defp get_config(%Catalog{config: config, client: client} = catalog) do
    client.request(:get_config, config: config)

    # TODO: Parse config and merge
    # (default_config (from api) + user_config + override_config(from api))
    # We need to list all the possible configurations. I know that in the
    # other implementations, it's all dynamic, but I believe that with explicit configuration,
    # we can have better control over the code.

    catalog
  end
end
