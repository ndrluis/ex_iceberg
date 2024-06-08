defmodule ExIceberg.Catalog.RestCatalogTest do
  use ExUnit.Case, async: true
  import Mox
  alias ExIceberg.Catalog.RestCatalog

  setup :verify_on_exit!

  @test_uri "http://localhost:8181"
  @headers [{"Content-Type", "application/json"}]

  describe "create_namespace/3" do
    test "successfully creates a new namespace" do
      ExIceberg.MockHTTPClient
      |> expect(:request, fn :post,
                             @test_uri <> "/v1/namespaces",
                             %{"namespace" => ["some_namespace"], "properties" => %{}},
                             @headers ->
        {:ok, %Req.Response{status: 200, body: %{}}}
      end)

      catalog = RestCatalog.new("my_catalog", %{"uri" => @test_uri}, ExIceberg.MockHTTPClient)
      assert :ok == RestCatalog.create_namespace(catalog, "some_namespace", %{})
    end

    test "fails to create a new namespace due to conflict" do
      ExIceberg.MockHTTPClient
      |> expect(:request, fn :post,
                             @test_uri <> "/v1/namespaces",
                             %{"namespace" => ["examples"], "properties" => %{}},
                             @headers ->
        {:error,
         %Req.Response{
           status: 409,
           body: %{
             "error" => %{
               "message" =>
                 "Namespace already exists: examples in warehouse 8bcb0838-50fc-472d-9ddb-8feb89ef5f1e",
               "type" => "AlreadyExistsException",
               "code" => 409
             }
           }
         }}
      end)

      catalog = RestCatalog.new("my_catalog", %{"uri" => @test_uri}, ExIceberg.MockHTTPClient)

      assert {:error,
              "Request failed with status 409: Namespace already exists: examples in warehouse 8bcb0838-50fc-472d-9ddb-8feb89ef5f1e"} ==
               RestCatalog.create_namespace(catalog, "examples", %{})
    end
  end
end
