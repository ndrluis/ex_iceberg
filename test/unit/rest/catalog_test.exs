defmodule ExIceberg.Rest.CatalogTest do
  use ExUnit.Case, async: true
  alias ExIceberg.Rest.Catalog

  describe "new/2" do
    test "does not authenticate when credential does not exist" do
      defmodule MockClient do
        def request(:get_token, config: _, base_url: _, form: params) do
          send(self(), :get_token)
        end
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
      end

      defmodule MockClientWithSingleCred do
        def request(:get_token, config: _, base_url: _, form: params) do
          assert params == %{client_id: "foo", client_secret: nil, scope: "catalog"}

          {:ok, "some_token"}
        end
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
      defmodule MockClient do
        def request(:get_token, config: _, base_url: _, form: params) do
          {:ok, "some_token"}
        end
      end

      %{config: config} =
        Catalog.new(
          "catalog",
          %{uri: "http://localhost:8181", credential: "foo"},
          MockClient
        )

      assert config.token == "some_token"
    end
  end
end
