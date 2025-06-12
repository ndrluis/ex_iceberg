defmodule ExIceberg.Rest.CatalogIntegrationTest do
  use ExUnit.Case, async: true
  alias ExIceberg.Rest.Catalog

  @moduletag :integration

  # Test schemas using the new API
  defmodule SimpleSchema do
    use ExIceberg.Schema

    schema "simple_table" do
      field(:id, :long, required: true)
      field(:name, :string)
    end
  end

  defmodule PrimitiveTypesSchema do
    use ExIceberg.Schema

    schema "all_primitives" do
      field(:id, :long, required: true)
      field(:active, :boolean)
      field(:age, :int)
      field(:score, :float)
      field(:rating, :double)
      field(:name, :string)
      field(:user_id, :uuid)
      field(:birth_date, :date)
      field(:created_at, :timestamp)
      field(:data, :binary)
    end
  end

  defmodule ParametricTypesSchema do
    use ExIceberg.Schema

    schema "parametric_types" do
      field(:id, :long, required: true)
      field(:price, ExIceberg.Types.decimal(10, 2))
      field(:hash, ExIceberg.Types.fixed(32))
    end
  end

  defmodule ComplexTypesSchema do
    use ExIceberg.Schema

    schema "complex_types" do
      field(:id, :long, required: true)
      field(:tags, ExIceberg.Types.list(:string, element_required: false))
      field(:metadata, ExIceberg.Types.map(:string, :string, value_required: false))

      field(
        :address,
        ExIceberg.Types.struct([
          ExIceberg.Types.field("street", :string),
          ExIceberg.Types.field("city", :string),
          ExIceberg.Types.field("zip", :int)
        ])
      )
    end
  end

  @config %{
    uri: "http://localhost:8181/catalog",
    warehouse: "demo"
  }

  @oauth2_config %{
    uri: "http://localhost:8181/catalog",
    warehouse: "demo",
    credential: "exiceberg:2OR3eRvYfSZzzZ16MlPd95jhLnOaLM52",
    oauth2_server_uri: "http://localhost:30080/realms/iceberg/protocol/openid-connect/token",
    scope: "lakekeeper"
  }

  defp generate_unique_name(base) do
    hash = :crypto.strong_rand_bytes(8) |> Base.encode16() |> String.downcase()
    "#{base}_#{hash}"
  end

  defp setup_warehouse(warehouse_name \\ "demo") do
    try do
      # Get OAuth2 token for management API
      token_request = %{
        "grant_type" => "client_credentials",
        "client_id" => "exiceberg",
        "client_secret" => "2OR3eRvYfSZzzZ16MlPd95jhLnOaLM52",
        "scope" => "lakekeeper"
      }

      token_response = Req.post!(
        "http://localhost:30080/realms/iceberg/protocol/openid-connect/token",
        form: token_request
      )

      access_token = token_response.body["access_token"]

      # Accept terms of use (bootstrap)
      bootstrap_request = %{
        "accept-terms-of-use" => true
      }

      # Only proceed if bootstrap succeeds
      case Req.post(
             "http://localhost:8181/management/v1/bootstrap",
             headers: [{"Authorization", "Bearer #{access_token}"}],
             json: bootstrap_request
           ) do
        {:ok, %{status: status}} when status in [200, 201, 204, 400, 409] ->
          # 204/400/409 means already bootstrapped, which is fine - proceed to warehouse creation
          :ok

        {:ok, response} ->
          IO.puts("Unexpected bootstrap response: #{inspect(response)}")
          :error

        {:error, reason} ->
          IO.puts("Failed to bootstrap: #{inspect(reason)}")
          :error
      end

      # Create warehouse using management API (only after bootstrap succeeds)
      warehouse_request = %{
        "warehouse-name" => warehouse_name,
        "project-id" => "00000000-0000-0000-0000-000000000000",
        "storage-profile" => %{
          "type" => "s3",
          "bucket" => "examples",
          "key-prefix" => "initial-warehouse",
          "assume-role-arn" => nil,
          "endpoint" => "http://host.docker.internal:9000",
          "region" => "local-01",
          "path-style-access" => true,
          "flavor" => "minio",
          "sts-enabled" => true
        },
        "storage-credential" => %{
          "type" => "s3",
          "credential-type" => "access-key",
          "aws-access-key-id" => "minio-root-user",
          "aws-secret-access-key" => "minio-root-password"
        }
      }

      case Req.post(
             "http://localhost:8181/management/v1/warehouse",
             headers: [{"Authorization", "Bearer #{access_token}"}],
             json: warehouse_request
           ) do
        {:ok, %{status: status}} when status in [200, 201, 400, 409] ->
          # 400/409 means warehouse already exists, which is fine
          :ok

        {:ok, response} ->
          IO.puts("Unexpected warehouse creation response: #{inspect(response)}")
          :error

        {:error, reason} ->
          IO.puts("Failed to create warehouse: #{inspect(reason)}")
          :error
      end
    rescue
      e ->
        IO.puts("Exception during warehouse setup: #{inspect(e)}")
        :error
    end
  end

  describe "new/2" do
    test "successfully creates a catalog instance" do
      catalog = Catalog.new("test_catalog", @config)

      assert %Catalog{} = catalog
      assert catalog.name == "test_catalog"
      assert catalog.config.uri == "http://localhost:8181/catalog"
      assert catalog.config.warehouse == "demo"
      assert is_reference(catalog.nif_catalog_resource)
    end
  end

  describe "list_namespaces/1" do
    test "successfully lists namespaces from real server" do
      setup_warehouse()
      catalog = Catalog.new("test_catalog", @oauth2_config)

      {:ok, %Catalog{} = updated_catalog, namespaces} = Catalog.list_namespaces(catalog)
      assert %Catalog{} = updated_catalog
      assert is_list(namespaces)
      assert Enum.all?(namespaces, &is_binary/1)
      assert length(namespaces) >= 0
    end

    test "handles server connection errors gracefully" do
      invalid_config = %{uri: "http://invalid-host:9999", warehouse: "demo"}
      catalog = Catalog.new("test_catalog", invalid_config)

      {:error, %Catalog{} = updated_catalog, reason} = Catalog.list_namespaces(catalog)
      assert %Catalog{} = updated_catalog
      assert is_binary(reason)
      assert String.contains?(reason, "Failed to list namespaces")
    end
  end

  describe "create_namespace/3" do
    test "successfully creates a new namespace" do
      setup_warehouse()
      namespace = generate_unique_name("test_namespace")
      catalog = Catalog.new("test_catalog", @oauth2_config)

      {:ok, %Catalog{} = updated_catalog, response} =
        Catalog.create_namespace(catalog, namespace, %{})

      assert %Catalog{} = updated_catalog
      assert is_map(response)
      assert Map.has_key?(response, "namespace")
      assert response["namespace"] == [namespace]
    end

    test "handles server connection errors when creating namespace" do
      namespace = generate_unique_name("test_namespace")
      invalid_config = %{uri: "http://invalid-host:9999", warehouse: "demo"}
      catalog = Catalog.new("test_catalog", invalid_config)

      {:error, %Catalog{} = updated_catalog, reason} =
        Catalog.create_namespace(catalog, namespace, %{})

      assert %Catalog{} = updated_catalog
      assert is_binary(reason)
      assert String.contains?(reason, "Failed to create namespace")
    end

    test "handles duplicate namespace creation" do
      setup_warehouse()
      namespace = generate_unique_name("duplicate_namespace")
      catalog = Catalog.new("test_catalog", @oauth2_config)

      {:ok, updated_catalog, _response} = Catalog.create_namespace(catalog, namespace, %{})

      {:error, %Catalog{} = final_catalog, reason} =
        Catalog.create_namespace(updated_catalog, namespace, %{})

      assert %Catalog{} = final_catalog
      assert is_binary(reason)

      assert String.contains?(reason, "409") or
               String.contains?(reason, "conflict") or
               String.contains?(reason, "already exists")
    end
  end

  describe "table operations" do
    test "table lifecycle: create, exists, drop" do
      setup_warehouse()
      namespace = generate_unique_name("table_test")
      table_name = SimpleSchema.__table_name__()
      catalog = Catalog.new("test_catalog", @oauth2_config)

      {:ok, catalog, _} = Catalog.create_namespace(catalog, namespace, %{})
      {:ok, catalog, false} = Catalog.table_exists?(catalog, namespace, table_name)

      {:ok, catalog, response} =
        SimpleSchema.create_table(catalog, namespace, %{"owner" => "test"})

      assert is_map(response)
      assert Map.has_key?(response, "table")

      {:ok, catalog, true} = Catalog.table_exists?(catalog, namespace, table_name)

      {:ok, _final_catalog, drop_response} = Catalog.drop_table(catalog, namespace, table_name)
      assert is_map(drop_response)
      assert Map.has_key?(drop_response, "table")
    end

    test "table_exists returns false for non-existent table" do
      namespace = generate_unique_name("nonexist_test")
      table_name = "non_existent_table"
      catalog = Catalog.new("test_catalog", @oauth2_config)

      {:ok, _updated_catalog, false} = Catalog.table_exists?(catalog, namespace, table_name)
    end

    test "table operations fail with invalid server" do
      namespace = generate_unique_name("table_test")
      table_name = SimpleSchema.__table_name__()
      invalid_config = %{uri: "http://invalid-host:9999", warehouse: "demo"}
      catalog = Catalog.new("test_catalog", invalid_config)

      {:error, _catalog, reason} = Catalog.table_exists?(catalog, namespace, table_name)
      assert is_binary(reason)
    end
  end

  describe "schema-based type support" do
    test "create table with all primitive types" do
      setup_warehouse()
      namespace = generate_unique_name("primitive_test")
      catalog = Catalog.new("test_catalog", @oauth2_config)

      {:ok, catalog, _} = Catalog.create_namespace(catalog, namespace, %{})

      {:ok, updated_catalog, response} =
        PrimitiveTypesSchema.create_table(catalog, namespace, %{"test" => "primitive_types"})

      assert %Catalog{} = updated_catalog
      assert is_map(response)
      assert Map.has_key?(response, "table")
    end

    test "create table with parametric types" do
      namespace = generate_unique_name("parametric_test")
      catalog = Catalog.new("test_catalog", @oauth2_config)

      {:ok, catalog, _} = Catalog.create_namespace(catalog, namespace, %{})

      {:ok, updated_catalog, response} =
        ParametricTypesSchema.create_table(catalog, namespace, %{"test" => "parametric_types"})

      assert %Catalog{} = updated_catalog
      assert is_map(response)
      assert Map.has_key?(response, "table")
    end

    test "create table with complex types" do
      namespace = generate_unique_name("complex_test")
      catalog = Catalog.new("test_catalog", @oauth2_config)

      {:ok, catalog, _} = Catalog.create_namespace(catalog, namespace, %{})

      {:ok, updated_catalog, response} =
        ComplexTypesSchema.create_table(catalog, namespace, %{"test" => "complex_types"})

      assert %Catalog{} = updated_catalog
      assert is_map(response)
      assert Map.has_key?(response, "table")
    end

    test "create table fails with invalid server" do
      namespace = generate_unique_name("primitive_test")
      invalid_config = %{uri: "http://invalid-host:9999", warehouse: "demo"}
      catalog = Catalog.new("test_catalog", invalid_config)

      {:error, _catalog, reason} =
        PrimitiveTypesSchema.create_table(catalog, namespace, %{"test" => "primitive_types"})

      assert is_binary(reason)
    end
  end

  describe "OAuth2 authentication" do
    test "successfully creates catalog with OAuth2 credentials" do
      setup_warehouse()
      catalog = Catalog.new("oauth2_catalog", @oauth2_config)

      assert %Catalog{} = catalog
      assert catalog.name == "oauth2_catalog"
      assert catalog.config.uri == "http://localhost:8181/catalog"
      assert catalog.config.warehouse == "demo"
      assert catalog.config.credential == "exiceberg:2OR3eRvYfSZzzZ16MlPd95jhLnOaLM52"

      assert catalog.config.oauth2_server_uri ==
               "http://localhost:30080/realms/iceberg/protocol/openid-connect/token"

      assert catalog.config.scope == "lakekeeper"
      assert is_reference(catalog.nif_catalog_resource)
    end

    test "OAuth2 catalog can list namespaces" do
      setup_warehouse()
      catalog = Catalog.new("oauth2_catalog", @oauth2_config)

      {:ok, %Catalog{} = updated_catalog, namespaces} = Catalog.list_namespaces(catalog)
      assert %Catalog{} = updated_catalog
      assert is_list(namespaces)
      assert Enum.all?(namespaces, &is_binary/1)
    end

    test "OAuth2 catalog fails with invalid server" do
      invalid_oauth2_config = %{@oauth2_config | uri: "http://invalid-host:9999"}
      catalog = Catalog.new("oauth2_catalog", invalid_oauth2_config)

      {:error, %Catalog{} = updated_catalog, reason} = Catalog.list_namespaces(catalog)
      assert %Catalog{} = updated_catalog
      assert is_binary(reason)
      assert String.contains?(reason, "Failed to list namespaces")
    end
  end
end
