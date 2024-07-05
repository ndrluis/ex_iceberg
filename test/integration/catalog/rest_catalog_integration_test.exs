defmodule ExIceberg.Catalog.RestIntegrationTest do
  use ExUnit.Case, async: true
  alias ExIceberg.Rest.Catalog

  @moduletag :integration

  @test_uri "http://localhost:8181"

  defp generate_unique_name(base) do
    hash = :crypto.strong_rand_bytes(8) |> Base.encode16() |> String.downcase()
    "#{base}_#{hash}"
  end

  describe "create_namespace/3" do
    test "successfully creates a new namespace" do
      namespace = generate_unique_name("some_namespace")
      catalog = Catalog.new("my_catalog", %{uri: @test_uri})

      assert {:ok, %Catalog{} = _catalog, response} =
               Catalog.create_namespace(catalog, namespace, %{})

      assert response == %{
               "namespace" => [namespace],
               "properties" => %{"location" => "s3://icebergdata/demo/#{namespace}"}
             }
    end

    test "fails to create a new namespace due to conflict" do
      namespace = generate_unique_name("examples")

      catalog = Catalog.new("my_catalog", %{uri: @test_uri})

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
      catalog = Catalog.new("my_catalog", %{uri: @test_uri})

      Catalog.create_namespace(catalog, namespace, %{})

      assert {:ok, %Catalog{} = _catalog, %{"namespaces" => namespaces}} =
               Catalog.list_namespaces(catalog)

      assert [namespace] in namespaces
    end
  end
end
