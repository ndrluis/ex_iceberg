defmodule ExIceberg.Rest.ClientTest do
  use ExUnit.Case, async: true

  alias ExIceberg.Rest.Client
  alias ExIceberg.Rest.Config

  describe "request/2" do
    test "prefixes the base url when the prefix config is provided" do
      Req.Test.stub(Client, fn conn ->
        assert conn.request_path == "/v1/my_catalog/config"

        Req.Test.json(conn, %{defaults: %{}, overrides: %{}})
      end)

      Client.request(:get_config,
        config: %Config{uri: "http://localhost:8181", prefix: "my_catalog"},
        plug: {Req.Test, Client}
      )
    end

    test "does not prefix the base url when the prefix config is not provided" do
      Req.Test.stub(Client, fn conn ->
        assert conn.request_path == "/v1/config"

        Req.Test.json(conn, %{defaults: %{}, overrides: %{}})
      end)

      Client.request(:get_config,
        config: %Config{uri: "http://localhost:8181"},
        plug: {Req.Test, Client}
      )
    end

    test "auth endpoint success response" do
      Req.Test.stub(Client, fn conn ->
        assert conn.request_path == "/v1/oauth/tokens"

        Req.Test.json(conn, %{token: "some_token"})
      end)

      params = %{"client_id" => "admin", "client_secret" => "admin"}

      {:ok, response} =
        Client.request(:get_token,
          config: %Config{uri: "http://localhost:8181"},
          form: params,
          plug: {Req.Test, Client}
        )

      assert response == %{"token" => "some_token"}
    end

    test "auth endpoint error response" do
      Req.Test.stub(Client, fn conn ->
        assert conn.request_path == "/v1/oauth/tokens"

        conn = Plug.Conn.put_status(conn, 401)

        Req.Test.json(conn, "Request failed with status 401")
      end)

      params = %{"client_id" => "admin", "client_secret" => "admin"}

      {:error, response} =
        Client.request(:get_token,
          config: %Config{uri: "http://localhost:8181"},
          form: params,
          plug: {Req.Test, Client}
        )

      assert response == "Request failed with status 401"
    end
  end
end
