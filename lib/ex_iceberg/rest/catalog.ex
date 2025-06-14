defmodule ExIceberg.Rest.Catalog do
  @moduledoc """
  REST catalog implementation using Rust NIFs.
  """

  alias ExIceberg.Nif
  alias ExIceberg.Rest.CatalogConfig

  defstruct name: nil, config: nil, nif_catalog_resource: nil

  @type t :: %__MODULE__{
          name: String.t(),
          config: CatalogConfig.t(),
          nif_catalog_resource: reference()
        }

  @doc """
  Creates a new REST catalog instance.

  ## Parameters

  - `name` - The name of the catalog
  - `config` - Configuration map containing REST catalog settings

  ## Returns

  `%ExIceberg.Rest.Catalog{}` - The catalog struct

  ## Examples

      config = %{
        uri: "http://localhost:8181",
        token: "my-token",
        warehouse: "s3://my-bucket/warehouse"
      }

      # Or with OAuth2:
      config = %{
        uri: "http://localhost:8181",
        credential: "client_id:client_secret",
        oauth2_server_uri: "http://keycloak:8080/realms/iceberg/protocol/openid-connect/token",
        scope: "lakekeeper",
        warehouse: "s3://my-bucket/warehouse"
      }
      catalog = ExIceberg.Rest.Catalog.new("my_catalog", config)
  """
  def new(name, config) do
    config = struct(CatalogConfig, config)

    # Create REST catalog using Rust NIFs (delegating to existing implementation)
    {:ok, nif_catalog_resource} = Nif.rest_catalog_new(config)

    %__MODULE__{
      name: name,
      config: config,
      nif_catalog_resource: nif_catalog_resource
    }
  end

  @doc """
  Lists all namespaces in the catalog.

  ## Parameters

  - `catalog` - The catalog struct

  ## Returns

  `{:ok, updated_catalog, namespaces}` - Success with list of namespace names
  `{:error, updated_catalog, reason}` - Error with reason

  ## Examples

      {:ok, catalog, namespaces} = ExIceberg.Rest.Catalog.list_namespaces(catalog)
      # namespaces might be ["default", "analytics", "staging"]
  """
  def list_namespaces(%__MODULE__{nif_catalog_resource: nif_catalog_resource} = catalog) do
    case Nif.rest_catalog_list_namespaces(nif_catalog_resource) do
      {:ok, namespaces} -> {:ok, catalog, namespaces}
      {:error, [reason | _]} -> {:error, catalog, reason}
    end
  end

  @doc """
  Creates a new namespace in the catalog.

  ## Parameters

  - `catalog` - The catalog struct
  - `namespace` - The namespace name as a string
  - `properties` - Map of properties for the namespace

  ## Returns

  `{:ok, updated_catalog, response}` - Success with response map
  `{:error, updated_catalog, reason}` - Error with reason

  ## Examples

      {:ok, catalog, response} = ExIceberg.Rest.Catalog.create_namespace(catalog, "my_namespace", %{})
      # response might be %{"namespace" => ["my_namespace"]}
  """
  def create_namespace(
        %__MODULE__{nif_catalog_resource: nif_catalog_resource} = catalog,
        namespace,
        properties \\ %{}
      ) do
    case Nif.rest_catalog_create_namespace(nif_catalog_resource, namespace, properties) do
      {:ok, response} -> {:ok, catalog, response}
      {:error, %{"error" => [reason | _]}} -> {:error, catalog, reason}
    end
  end

  @doc """
  Checks if a table exists in the catalog.

  ## Parameters

  - `catalog` - The catalog struct
  - `namespace` - The namespace name as a string
  - `table_name` - The table name as a string

  ## Returns

  `{:ok, updated_catalog, exists}` - Success with boolean indicating if table exists
  `{:error, updated_catalog, reason}` - Error with reason

  ## Examples

      {:ok, catalog, exists} = ExIceberg.Rest.Catalog.table_exists?(catalog, "my_namespace", "my_table")
      # exists is true or false
  """
  def table_exists?(
        %__MODULE__{nif_catalog_resource: nif_catalog_resource} = catalog,
        namespace,
        table_name
      ) do
    case Nif.rest_catalog_table_exists(nif_catalog_resource, namespace, table_name) do
      {:ok, exists} -> {:ok, catalog, exists}
      {:error, _} -> {:error, catalog, "Failed to check table existence"}
    end
  end

  @doc """
  Drops a table from the catalog.

  ## Parameters

  - `catalog` - The catalog struct
  - `namespace` - The namespace name as a string
  - `table_name` - The table name as a string

  ## Returns

  `{:ok, updated_catalog, response}` - Success with response map
  `{:error, updated_catalog, reason}` - Error with reason

  ## Examples

      {:ok, catalog, response} = ExIceberg.Rest.Catalog.drop_table(catalog, "my_namespace", "my_table")
      # response might be %{"table" => "my_namespace.my_table"}
  """
  def drop_table(
        %__MODULE__{nif_catalog_resource: nif_catalog_resource} = catalog,
        namespace,
        table_name
      ) do
    case Nif.rest_catalog_drop_table(nif_catalog_resource, namespace, table_name) do
      {:ok, response} -> {:ok, catalog, response}
      {:error, %{"error" => reason}} -> {:error, catalog, reason}
    end
  end

  @doc """
  Creates a table in the catalog using structured field definitions.

  ## Parameters

  - `catalog` - The catalog struct
  - `namespace` - The namespace name as a string
  - `table_name` - The table name as a string
  - `fields` - List of `ExIceberg.Types.Field` structs
  - `properties` - Map of table properties (optional)

  ## Returns

  `{:ok, updated_catalog, response}` - Success with response map
  `{:error, updated_catalog, reason}` - Error with reason

  ## Note

  This function now uses the structured type system. For defining tables,
  it's recommended to use `ExIceberg.Schema` for a more declarative approach:

      defmodule MySchema do
        use ExIceberg.Schema

        schema "my_table" do
          field :id, :long, required: true
          field :name, :string
          field :balance, ExIceberg.Types.decimal(10, 2)
        end
      end

      MySchema.create_table(catalog, namespace)

  ## Examples

      # Using structured field definitions directly
      fields = [
        ExIceberg.Types.field("id", :long, required: true),
        ExIceberg.Types.field("name", :string),
        ExIceberg.Types.field("balance", ExIceberg.Types.decimal(10, 2)),
        ExIceberg.Types.field("tags", ExIceberg.Types.list(:string)),
        ExIceberg.Types.field("address", ExIceberg.Types.struct([
          ExIceberg.Types.field("street", :string),
          ExIceberg.Types.field("city", :string)
        ]))
      ]

      {:ok, catalog, response} = ExIceberg.Rest.Catalog.create_table(catalog, "my_namespace", "my_table", fields, %{"owner" => "test"})
  """
  def create_table(
        %__MODULE__{nif_catalog_resource: nif_catalog_resource} = catalog,
        namespace,
        table_name,
        fields,
        properties \\ %{}
      ) do
    case Nif.rest_catalog_create_table(
           nif_catalog_resource,
           namespace,
           table_name,
           fields,
           properties
         ) do
      {:ok, response} -> {:ok, catalog, response}
      {:error, %{"error" => reason}} -> {:error, catalog, reason}
    end
  end

  @doc """
  Loads a table from the catalog.

  ## Parameters

  - `catalog` - The catalog struct
  - `namespace` - The namespace name as a string
  - `table_name` - The table name as a string

  ## Returns

  `{:ok, updated_catalog, table_info}` - Success with table metadata map
  `{:error, updated_catalog, reason}` - Error with reason

  ## Examples

      {:ok, catalog, table_info} = ExIceberg.Rest.Catalog.load_table(catalog, "my_namespace", "my_table")
      # table_info contains schema, properties, location, etc.
  """
  def load_table(
        %__MODULE__{nif_catalog_resource: nif_catalog_resource} = catalog,
        namespace,
        table_name
      ) do
    case Nif.rest_catalog_load_table(nif_catalog_resource, namespace, table_name) do
      {:ok, table_info} ->
        # Parse JSON strings back to Elixir terms for easier consumption
        parsed_info = %{
          "table_uuid" => table_info["table_uuid"],
          "format_version" => String.to_integer(table_info["format_version"]),
          "location" => table_info["location"],
          "schema_id" => String.to_integer(table_info["schema_id"]),
          "fields" => Jason.decode!(table_info["fields"]),
          "properties" => Jason.decode!(table_info["properties"])
        }

        {:ok, catalog, parsed_info}

      {:error, %{"error" => reason}} ->
        {:error, catalog, reason}
    end
  end
end
