defmodule ExIceberg.Rest.CatalogConfig do
  @moduledoc false

  defstruct [
    :uri,
    :warehouse,
    :token,
    :credential,
    :oauth2_server_uri,
    :scope,
    :audience,
    :resource
  ]

  @type t :: %__MODULE__{
          uri: String.t(),
          warehouse: String.t() | nil,
          token: String.t() | nil,
          credential: String.t() | nil,
          oauth2_server_uri: String.t() | nil,
          scope: String.t() | nil,
          audience: String.t() | nil,
          resource: String.t() | nil
        }
end
