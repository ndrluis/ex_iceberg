defmodule ExIceberg.Rest.Client do
  @moduledoc false
  alias ExIceberg.Rest.Config

  def new(options \\ []) do
    base_url = build_base_url(options[:config])

    Req.new(base_url: base_url)
    |> Req.Request.register_options([:config])
    |> Req.Request.merge_options(options)
    |> Req.Request.prepend_request_steps(req_rest_catalog_auth: &auth/1)
  end

  def request(action, options \\ []) do
    Req.request(new(options), endpoint(action))
    |> ExIceberg.Rest.Response.parse(action)
  end

  defp auth(%{options: %{config: %{token: token}}} = request) when token != nil do
    Req.Request.merge_options(request, auth: {:bearer, token})
  end

  defp auth(request) do
    request
  end

  defp build_base_url(%Config{uri: uri, prefix: prefix}) when prefix != nil do
    "#{uri}/v1/#{prefix}"
    |> URI.parse()
    |> normalize_path()
  end

  defp build_base_url(%Config{oauth2_server_uri: uri}) when uri != nil do
    uri
  end

  defp build_base_url(%Config{uri: uri}) do
    "#{uri}/v1/"
    |> URI.parse()
    |> normalize_path()
  end

  defp normalize_path(%URI{path: path} = uri) do
    normalized_path =
      path
      |> String.replace(~r{/+}, "/")

    %URI{uri | path: normalized_path}
  end

  # TODO: Add options for exception handling
  defp endpoint(:get_token), do: [url: "/oauth/tokens", method: :post]
  defp endpoint(:get_config), do: [url: "/config", method: :get]
  defp endpoint(:list_namespace), do: [url: "/namespaces", method: :get]
  defp endpoint(:create_namespace), do: [url: "/namespaces", method: :post]
  defp endpoint(:drop_namespace), do: [url: "/namespaces/:namespace", method: :delete]

  defp endpoint(:load_namespace_metadata),
    do: [url: "/namespaces/:namespace", method: :get]

  defp endpoint(:update_namespace_properties),
    do: [url: "/namespaces/:namespace/properties", method: :post]

  defp endpoint(:namespace_exists), do: [url: "/namespaces/:namespace", method: :head]
end
