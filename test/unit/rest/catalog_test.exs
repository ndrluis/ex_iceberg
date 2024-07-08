defmodule ExIceberg.Rest.CatalogTest do
  use ExUnit.Case, async: true
  alias ExIceberg.Rest.Catalog

  describe "new/2" do
    defmodule DefaultMockClient do
      def request(:get_token, config: _, base_url: _, form: _) do
        {:ok, "some_token"}
      end

      def request(:get_config, _), do: {:ok, %{}}
    end

    test "does not authenticate when credential does not exist" do
      defmodule MockClient do
        def request(:get_token, config: _, base_url: _, form: _) do
          send(self(), :get_token)
        end

        def request(:get_config, _), do: {:ok, %{}}
      end

      Catalog.new(
        "catalog",
        %{uri: "http://localhost:8181"},
        MockClient
      )

      refute_receive :get_token
    end

    test "fills optional params when exists" do
      defmodule MockClientWithAudience do
        def request(:get_token, config: _, base_url: _, form: params) do
          assert params == %{
                   client_id: "foo",
                   client_secret: nil,
                   scope: "catalog",
                   audience: "something"
                 }

          {:ok, "some_token"}
        end

        def request(:get_config, _), do: {:ok, %{}}
      end

      defmodule MockClientWithResource do
        def request(:get_token, config: _, base_url: _, form: params) do
          assert params == %{
                   client_id: "foo",
                   client_secret: nil,
                   scope: "catalog",
                   resource: "something"
                 }

          {:ok, "some_token"}
        end

        def request(:get_config, _), do: {:ok, %{}}
      end

      Catalog.new(
        "catalog",
        %{uri: "http://localhost:8181", credential: "foo", audience: "something"},
        MockClientWithAudience
      )

      Catalog.new(
        "catalog",
        %{uri: "http://localhost:8181", credential: "foo", resource: "something"},
        MockClientWithResource
      )
    end

    test "authenticates when credential exists" do
      defmodule MockClientWithFullCred do
        def request(:get_token, config: _, base_url: _, form: params) do
          assert params == %{client_id: "foo", client_secret: "bar", scope: "catalog"}

          {:ok, "some_token"}
        end

        def request(:get_config, _), do: {:ok, %{}}
      end

      defmodule MockClientWithSingleCred do
        def request(:get_token, config: _, base_url: _, form: params) do
          assert params == %{client_id: "foo", client_secret: nil, scope: "catalog"}

          {:ok, "some_token"}
        end

        def request(:get_config, _), do: {:ok, %{}}
      end

      Catalog.new(
        "catalog",
        %{uri: "http://localhost:8181", credential: "foo:bar"},
        MockClientWithFullCred
      )

      Catalog.new(
        "catalog",
        %{uri: "http://localhost:8181", credential: "foo"},
        MockClientWithSingleCred
      )
    end

    test "merge token into config when authenticated" do
      %{config: config} =
        Catalog.new(
          "catalog",
          %{uri: "http://localhost:8181", credential: "foo"},
          DefaultMockClient
        )

      assert config.token == "some_token"
    end

    test "change the auth base url when oauth2_server_uri exists" do
      defmodule MockClientCustomOauth do
        def request(:get_token, config: _, base_url: base_url, form: _) do
          assert base_url == "http://other_host.io"

          {:ok, "some_token"}
        end

        def request(:get_config, _), do: {:ok, %{}}
      end

      Catalog.new(
        "catalog",
        %{
          uri: "http://localhost:8181",
          credential: "foo",
          oauth2_server_uri: "http://other_host.io"
        },
        MockClientCustomOauth
      )
    end
  end
end
