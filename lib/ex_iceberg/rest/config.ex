defmodule ExIceberg.Rest.Config do
  @moduledoc """
  Configuration for the Rest Catalog.

  ## Fields

    * `uri` - `https://rest-catalog/ws`: URI identifying the REST Server.
    * `credential` - `t-1234:secret`: Credential to use for OAuth2 credential flow when initializing the catalog.
    * `token` - `FEW23.DFSDF.FSDF`: Bearer token value to use for the `Authorization` header.
    * `scope` - `openid offline corpds:ds:profile`: Desired scope of the requested security token (default: `catalog`).
    * `resource` - `rest_catalog.iceberg.com`: URI for the target resource or service.
    * `audience` - `rest_catalog`: Logical name of the target resource or service.
    * `grant_type` - `client_credentials`: Grant type to use for client credentials authentication. (default: `client_credentials`)
    * `oauth2_server_uri` - `https://auth-service/cc`: Authentication URL to use for client credentials authentication (default: `uri` + 'v1/oauth/tokens').
    * `warehouse` - `sandbox`: Warehouse to use for the catalog.
  """
  defstruct [
    :uri,
    :prefix,
    :token,
    :credential,
    :resource,
    :audience,
    :oauth2_server_uri,
    :warehouse,
    s3: %{},
    grant_type: "client_credentials",
    scope: "catalog"
  ]

  @type t :: %__MODULE__{
          uri: String.t(),
          prefix: String.t(),
          token: String.t(),
          credential: String.t(),
          scope: String.t(),
          resource: String.t(),
          audience: String.t(),
          oauth2_server_uri: String.t(),
          warehouse: String.t(),
          grant_type: String.t(),
          s3: %ExIceberg.S3.Config{}
        }

  def new(options) do
    s3 =
      Map.new(options)
      |> Map.get(:s3, %{})
      |> ExIceberg.S3.Config.new()

    struct(__MODULE__, Map.put(options, :s3, s3))
  end

  def split_credential(credential) do
    case String.split(credential, ":") do
      [username, password] -> {username, password}
      [username] -> {username, nil}
    end
  end

  def parse(config) do
    %__MODULE__{
      uri: config["uri"],
      prefix: config["prefix"],
      token: config["token"],
      credential: config["credential"],
      scope: config["scope"],
      resource: config["resource"],
      audience: config["audience"],
      grant_type: config["grant_type"],
      oauth2_server_uri: config["oauth2-server-uri"],
      s3: ExIceberg.S3.Config.parse(config)
    }
    |> remove_nil_values()
    |> new()
  end

  def merge(local_config, default_config, override_config) do
    local_config = remove_nil_values(local_config)
    default_config = remove_nil_values(default_config)
    override_config = remove_nil_values(override_config)

    default_config
    |> deep_merge(local_config)
    |> deep_merge(override_config)
    |> new()
  end

  defp remove_nil_values(map) do
    Map.from_struct(map)
    |> Enum.reject(fn {_, v} -> is_nil(v) end)
    |> Enum.map(fn {key, value} ->
      cond do
        is_map(value) -> {key, remove_nil_values(value)}
        true -> {key, value}
      end
    end)
    |> Map.new()
  end

  defp deep_merge(left, right) do
    Map.merge(left, right, &deep_resolve/3)
  end

  defp deep_resolve(_key, left = %{}, right = %{}) do
    deep_merge(left, right)
  end

  defp deep_resolve(_key, _left, right) do
    right
  end
end
