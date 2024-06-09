defmodule ExIceberg.ReqClient do
  @behaviour ExIceberg.HTTPClient

  def request(method, url, body, headers) do
    req =
      Req.new(
        method: method,
        url: url,
        body: Jason.encode!(body),
        headers: headers
      )

    case Req.request(req) do
      {:ok, %Req.Response{status: status, body: body}} when status in 200..299 ->
        {:ok, body}

      {:ok, %Req.Response{status: status, body: body}} ->
        {:error, %Req.Response{status: status, body: body}}

      {:error, %Req.TransportError{reason: reason}} ->
        {:error, %Req.TransportError{reason: reason}}
    end
  end
end
