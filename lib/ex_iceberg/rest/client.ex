defmodule ExIceberg.Rest.Client do
  alias ExIceberg.Rest.Config

  def new(options \\ []) do
    base_url = build_base_url(options[:config])

    Req.new(base_url: base_url)
    |> Req.Request.register_options([:config])
    |> Req.Request.merge_options(config: options[:config])
    |> Req.Request.merge_options(options)
    |> Req.Request.prepend_request_steps(req_rest_catalog_auth: &auth/1)
  end

  def request(action, options \\ []) do
    Req.request(new(options), endpoint(action))
    |> parse_response()
  end

  defp auth(%{config: %{token: token}} = request) do
    Req.Request.merge_options(request, auth: {:bearer, token})
  end

  defp auth(request), do: request

  defp build_base_url(%Config{uri: uri, prefix: prefix}) when prefix != nil do
    uri <> "/v1/" <> prefix
  end

  defp build_base_url(%Config{uri: uri}) do
    uri <> "/v1/"
  end

  # TODO: Add options for exception handling
  defp endpoint(:get_token), do: [url: "/oauth/tokens", method: :post]
  defp endpoint(:get_config), do: [url: "/config", method: :get]
  defp endpoint(:list_namespace), do: [url: "/namespaces", method: :get]
  defp endpoint(:create_namespace), do: [url: "/namespaces", method: :post]

  defp parse_response({:ok, %Req.Response{status: 200, body: body}}) do
    {:ok, body}
  end

  defp parse_response({:ok, %Req.Response{status: status}}) when status >= 400 do
    {:error, "Request failed with status #{status}"}
  end

  defp parse_response({:error, reason}) do
    {:error, reason}
  end
end
