defmodule Integration.Catalog.RestIntegrationTest do
  use ExUnit.Case, async: true
  alias ExIceberg.Rest.Catalog

  @moduletag :integration

  @config %{
    uri: "http://localhost:8080/catalog",
    warehouse: "demo"
  }

  defp generate_unique_name(base) do
    hash = :crypto.strong_rand_bytes(8) |> Base.encode16() |> String.downcase()
    "#{base}_#{hash}"
  end

  describe "create_namespace/3" do
    test "successfully creates a new namespace" do
      namespace = generate_unique_name("some_namespace")
      catalog = Catalog.new("my_catalog", @config)

      assert {:ok, %Catalog{} = _catalog, response} =
               Catalog.create_namespace(catalog, namespace, %{})

      assert response == %{
               "namespace" => [namespace]
             }
    end

    test "fails to create a new namespace due to conflict" do
      namespace = generate_unique_name("examples")

      catalog = Catalog.new("my_catalog", @config)

      Catalog.create_namespace(catalog, namespace, %{})

      assert {:error, %Catalog{} = _catalog, response} =
               Catalog.create_namespace(catalog, namespace, %{})

      assert response ==
               "Request failed with status 409"
    end
  end

  describe "list_namespaces/1" do
    test "successfully lists namespaces" do
      namespace = generate_unique_name("some_namespace")
      catalog = Catalog.new("my_catalog", @config)

      Catalog.create_namespace(catalog, namespace, %{})

      assert {:ok, %Catalog{} = _catalog, %{"namespaces" => namespaces}} =
               Catalog.list_namespaces(catalog)

      assert [namespace] in namespaces
    end
  end

  describe "drop_namespace/2" do
    test "deletes a namespace" do
      namespace = generate_unique_name("some_namespace")
      catalog = Catalog.new("my_catalog", @config)

      Catalog.create_namespace(catalog, namespace, %{})

      assert {:ok, %Catalog{} = _catalog, response} =
               Catalog.drop_namespace(catalog, namespace)

      assert response == []
    end
  end

  describe "namespace_exists?/2" do
    test "returns true if namespace exists" do
      namespace = generate_unique_name("some_namespace")
      catalog = Catalog.new("my_catalog", @config)

      Catalog.create_namespace(catalog, namespace, %{})

      assert {:ok, %Catalog{} = _catalog, exists} =
               Catalog.namespace_exists?(catalog, namespace)

      assert exists == true
    end

    test "returns false if namespace does not exist" do
      namespace = generate_unique_name("some_namespace")
      catalog = Catalog.new("my_catalog", @config)

      assert {:ok, %Catalog{} = _catalog, exists} =
               Catalog.namespace_exists?(catalog, namespace)

      assert exists == false
    end
  end

  describe "load_namespace_metadata/2" do
    test "loads namespace metadata" do
      namespace = generate_unique_name("some_namespace")
      catalog = Catalog.new("my_catalog", @config)

      Catalog.create_namespace(catalog, namespace, %{})

      assert {:ok, %Catalog{} = _catalog, response} =
               Catalog.load_namespace_metadata(catalog, namespace)

      assert response == %{
               "namespace" => [namespace],
               "properties" => %{}
             }
    end
  end

  describe "update_namespace_properties/3" do
    test "updates namespace properties" do
      namespace = generate_unique_name("some_namespace")
      catalog = Catalog.new("my_catalog", @config)

      Catalog.create_namespace(catalog, namespace, %{"key_to_remove" => "value_to_remove"})

      assert {:ok, %Catalog{} = _catalog, response} =
               Catalog.update_namespace_properties(catalog, namespace, ["key_to_remove"], %{
                 "key_to_update" => "some_value"
               })

      assert response == %{
               "removed" => ["key_to_remove"],
               "updated" => ["key_to_update"]
             }
    end
  end
end
