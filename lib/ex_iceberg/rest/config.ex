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
    * `oauth2_server_uri` - `https://auth-service/cc`: Authentication URL to use for client credentials authentication (default: `uri` + 'v1/oauth/tokens').
  """

  defstruct uri: nil,
            prefix: nil,
            token: nil,
            credential: nil,
            scope: "catalog",
            resource: nil,
            audience: nil,
            oauth2_server_uri: nil

  @type t :: %__MODULE__{
          uri: String.t(),
          prefix: String.t(),
          token: String.t(),
          credential: String.t(),
          scope: String.t(),
          resource: String.t(),
          audience: String.t(),
          oauth2_server_uri: String.t()
        }

  def new(options) do
    struct(__MODULE__, options)
  end

  def split_credential(credential) do
    case String.split(credential, ":") do
      [username, password] -> {username, password}
      [username] -> {username, nil}
    end
  end
end
