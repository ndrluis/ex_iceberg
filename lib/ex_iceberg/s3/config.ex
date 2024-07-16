defmodule ExIceberg.S3.Config do
  @moduledoc false
  defstruct [
    :endpoint,
    :access_key_id,
    :secret_access_key,
    :session_token,
    :signer,
    :signer_uri,
    :region,
    :proxy_uri,
    :connect_timeout,
    :delete_enabled
  ]

  @type t :: %__MODULE__{
          endpoint: String.t(),
          access_key_id: String.t(),
          secret_access_key: String.t(),
          session_token: String.t(),
          signer: String.t(),
          signer_uri: String.t(),
          region: String.t(),
          proxy_uri: String.t(),
          connect_timeout: Float.t(),
          delete_enabled: String.t()
        }

  def new(options) do
    struct(__MODULE__, options)
  end

  def parse(config) do
    %__MODULE__{
      endpoint: config["s3.endpoint"],
      access_key_id: config["s3.access-key-id"],
      secret_access_key: config["s3.secret-access"],
      session_token: config["s3.session-token"],
      signer: config["s3.signer"],
      signer_uri: config["s3.signer.uri"],
      region: config["s3.region"],
      proxy_uri: config["s3.proxy-uri"],
      connect_timeout: config["s3.connect-timeout"],
      delete_enabled: config["s3.delete-enabled"]
    }
  end
end
