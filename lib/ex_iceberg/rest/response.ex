defmodule ExIceberg.Rest.Response do
  @moduledoc false

  def parse({:ok, %Req.Response{status: 404}}, :namespace_exists) do
    {:ok, false}
  end

  def parse({:ok, %Req.Response{status: status}}, :namespace_exists)
      when status in [200, 204] do
    {:ok, true}
  end

  def parse({:ok, %Req.Response{status: 200, body: body}}, _) do
    {:ok, body}
  end

  def parse({:ok, %Req.Response{status: 204}}, _) do
    {:ok, []}
  end

  def parse({:ok, %Req.Response{status: status}}, _) when status >= 400 do
    {:error, "Request failed with status #{status}"}
  end
end
