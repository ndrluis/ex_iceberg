defmodule ExIceberg.Rest.CatalogTest do
  use ExUnit.Case, async: true

  alias ExIceberg.Rest.Catalog

  describe "new/2" do
    test "creates a catalog with basic config" do
      config = %{uri: "http://localhost:8181"}
      catalog = Catalog.new("test", config)

      assert %Catalog{} = catalog
      assert catalog.name == "test"
      assert catalog.config.uri == "http://localhost:8181"
      assert is_reference(catalog.nif_catalog_resource)
    end

    test "creates a catalog with OAuth2 config" do
      config = %{
        uri: "http://localhost:8181",
        credential: "user:pass",
        oauth2_server_uri: "http://localhost:8080/token",
        scope: "catalog"
      }

      catalog = Catalog.new("oauth_test", config)

      assert %Catalog{} = catalog
      assert catalog.name == "oauth_test"
      assert catalog.config.uri == "http://localhost:8181"
      assert catalog.config.credential == "user:pass"
      assert catalog.config.oauth2_server_uri == "http://localhost:8080/token"
      assert catalog.config.scope == "catalog"
      assert is_reference(catalog.nif_catalog_resource)
    end
  end

  describe "rename_table/5" do
    setup do
      config = %{uri: "http://localhost:8181"}
      catalog = Catalog.new("test", config)
      {:ok, catalog: catalog}
    end

    test "returns error tuple when catalog is offline", %{catalog: catalog} do
      result = Catalog.rename_table(catalog, "namespace", "old_table", "namespace", "new_table")

      assert {:error, ^catalog, reason} = result
      assert is_binary(reason)
      assert String.contains?(reason, "Failed to rename table")
    end

    test "validates function signature", %{catalog: catalog} do
      # Test that the function accepts the correct parameters
      assert function_exported?(Catalog, :rename_table, 5)

      # Test parameter structure
      result = Catalog.rename_table(catalog, "ns1", "table1", "ns2", "table2")
      assert match?({:error, _, _}, result)
    end
  end
end
