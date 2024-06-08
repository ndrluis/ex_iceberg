defmodule ExIceberg.Catalog.RestCatalogIntegrationTest do
  use ExUnit.Case, async: true
  alias ExIceberg.Catalog.RestCatalog

  @moduletag :integration

  @test_uri "http://localhost:8181"

  defp generate_unique_name(base) do
    hash = :crypto.strong_rand_bytes(8) |> Base.encode16() |> String.downcase()
    "#{base}_#{hash}"
  end

  test "successfully creates a new namespace" do
    namespace = generate_unique_name("some_namespace")
    catalog = RestCatalog.new("my_catalog", %{"uri" => @test_uri})
    assert :ok == RestCatalog.create_namespace(catalog, namespace, %{})
  end

  test "fails to create a new namespace due to conflict" do
    namespace = generate_unique_name("examples")
    catalog = RestCatalog.new("my_catalog", %{"uri" => @test_uri})

    assert :ok == RestCatalog.create_namespace(catalog, namespace, %{})

    assert {:error, "Request failed with status 409: Namespace already exists: #{namespace}"} ==
             RestCatalog.create_namespace(catalog, namespace, %{})
  end
end
