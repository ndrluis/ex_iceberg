defmodule ExIceberg.Rest.CatalogTest do
  use ExUnit.Case, async: true

  alias ExIceberg.Rest.Catalog
  alias ExIceberg.TableIdent

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

  describe "rename_table/3" do
    setup do
      config = %{uri: "http://localhost:8181"}
      catalog = Catalog.new("test", config)
      {:ok, catalog: catalog}
    end

    test "returns error tuple when catalog is offline", %{catalog: catalog} do
      src_ident = TableIdent.from_string("namespace.old_table")
      dest_ident = TableIdent.from_string("namespace.new_table")

      result = Catalog.rename_table(catalog, src_ident, dest_ident)

      assert {:error, ^catalog, reason} = result
      assert is_binary(reason)
      assert String.contains?(reason, "Failed to rename table")
    end

    test "validates function signature", %{catalog: catalog} do
      # Test that the function accepts the correct parameters
      assert function_exported?(Catalog, :rename_table, 3)

      # Test parameter structure with structured identifiers
      src_ident = TableIdent.from_string("ns1.table1")
      dest_ident = TableIdent.from_string("ns2.table2")

      result = Catalog.rename_table(catalog, src_ident, dest_ident)
      assert match?({:error, _, _}, result)
    end
  end
end
