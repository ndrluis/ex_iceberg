defmodule ExIceberg.Rest.Catalog do
  @moduledoc """
  REST catalog implementation using Rust NIFs.
  """

  alias ExIceberg.Nif
  alias ExIceberg.Rest.CatalogConfig
  alias ExIceberg.{NamespaceIdent, TableIdent}

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

    nif_catalog_resource =
      case Nif.rest_catalog_new(config) do
        {:ok, nif_catalog_resource} -> nif_catalog_resource
        {:error, reason} -> raise "Failed to create catalog: #{inspect(reason)}"
      end

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

  `{:ok, updated_catalog, namespaces}` - Success with list of NamespaceIdent structs
  `{:error, updated_catalog, reason}` - Error with reason

  ## Examples

      {:ok, catalog, namespaces} = ExIceberg.Rest.Catalog.list_namespaces(catalog)
      # namespaces might be [%NamespaceIdent{parts: ["default"]}, %NamespaceIdent{parts: ["analytics"]}]
  """
  def list_namespaces(%__MODULE__{nif_catalog_resource: nif_catalog_resource} = catalog) do
    case Nif.rest_catalog_list_namespaces(nif_catalog_resource) do
      {:ok, namespaces} -> {:ok, catalog, namespaces}
      {:error, [%NamespaceIdent{parts: [reason]}]} -> {:error, catalog, reason}
    end
  end

  @doc """
  Creates a new namespace in the catalog.

  ## Parameters

  - `catalog` - The catalog struct
  - `namespace` - NamespaceIdent struct
  - `properties` - Map of properties for the namespace

  ## Returns

  `{:ok, updated_catalog, namespace_ident}` - Success with NamespaceIdent struct
  `{:error, updated_catalog, reason}` - Error with reason

  ## Examples

      ns = NamespaceIdent.new("my_namespace")
      {:ok, catalog, created_ns} = ExIceberg.Rest.Catalog.create_namespace(catalog, ns, %{})

      # Multi-level namespace
      ns = NamespaceIdent.new("level1.level2")
      {:ok, catalog, created_ns} = ExIceberg.Rest.Catalog.create_namespace(catalog, ns, %{})
  """
  def create_namespace(
        %__MODULE__{nif_catalog_resource: nif_catalog_resource} = catalog,
        %NamespaceIdent{} = namespace,
        properties \\ %{}
      ) do
    case Nif.rest_catalog_create_namespace(nif_catalog_resource, namespace, properties) do
      {:ok, response} -> {:ok, catalog, response}
      {:error, %NamespaceIdent{parts: [reason]}} -> {:error, catalog, reason}
    end
  end

  @doc """
  Checks if a table exists in the catalog.

  ## Parameters

  - `catalog` - The catalog struct
  - `table_ident` - TableIdent struct

  ## Returns

  `{:ok, updated_catalog, exists}` - Success with boolean indicating if table exists
  `{:error, updated_catalog, reason}` - Error with reason

  ## Examples

      table_ident = TableIdent.from_string("my_namespace.my_table")
      {:ok, catalog, exists} = ExIceberg.Rest.Catalog.table_exists?(catalog, table_ident)
      # exists is true or false

      # Or create TableIdent explicitly
      namespace = NamespaceIdent.new("my_namespace")
      table_ident = TableIdent.new(namespace, "my_table")
      {:ok, catalog, exists} = ExIceberg.Rest.Catalog.table_exists?(catalog, table_ident)
  """
  def table_exists?(
        %__MODULE__{nif_catalog_resource: nif_catalog_resource} = catalog,
        %TableIdent{} = table_ident
      ) do
    case Nif.rest_catalog_table_exists(nif_catalog_resource, table_ident) do
      {:ok, exists} -> {:ok, catalog, exists}
      {:error, _} -> {:error, catalog, "Failed to check table existence"}
    end
  end

  @doc """
  Drops a table from the catalog.

  ## Parameters

  - `catalog` - The catalog struct
  - `table_ident` - TableIdent struct

  ## Returns

  `{:ok, updated_catalog, table_ident}` - Success with dropped TableIdent
  `{:error, updated_catalog, reason}` - Error with reason

  ## Examples

      table_ident = TableIdent.from_string("my_namespace.my_table")
      {:ok, catalog, dropped_table} = ExIceberg.Rest.Catalog.drop_table(catalog, table_ident)

      # Or create TableIdent explicitly
      namespace = NamespaceIdent.new("my_namespace")
      table_ident = TableIdent.new(namespace, "my_table")
      {:ok, catalog, dropped_table} = ExIceberg.Rest.Catalog.drop_table(catalog, table_ident)
  """
  def drop_table(
        %__MODULE__{nif_catalog_resource: nif_catalog_resource} = catalog,
        %TableIdent{} = table_ident
      ) do
    case Nif.rest_catalog_drop_table(nif_catalog_resource, table_ident) do
      {:ok, response} ->
        {:ok, catalog, response}

      {:error, %TableIdent{namespace: %NamespaceIdent{parts: [reason]}}} ->
        {:error, catalog, reason}
    end
  end

  @doc """
  Creates a table in the catalog using structured field definitions.

  ## Parameters

  - `catalog` - The catalog struct
  - `table_ident` - TableIdent struct
  - `fields` - List of `ExIceberg.Types.Field` structs
  - `properties` - Map of table properties (optional)

  ## Returns

  `{:ok, updated_catalog, table}` - Success with Table struct
  `{:error, updated_catalog, reason}` - Error with reason

  ## Note

  This function uses the structured type system. For defining tables,
  it's recommended to use `ExIceberg.Schema` for a more declarative approach:

      defmodule MySchema do
        use ExIceberg.Schema

        schema "my_table" do
          field :id, :long, required: true
          field :name, :string
          field :balance, ExIceberg.Types.decimal(10, 2)
        end
      end

      MySchema.create_table(catalog, table_ident)

  ## Examples

      # Using structured field definitions directly with TableIdent
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

      table_ident = TableIdent.from_string("my_namespace.my_table")
      {:ok, catalog, table} = ExIceberg.Rest.Catalog.create_table(catalog, table_ident, fields, %{"owner" => "test"})

      # Or create TableIdent explicitly
      namespace = NamespaceIdent.new("my_namespace")
      table_ident = TableIdent.new(namespace, "my_table")
      {:ok, catalog, table} = ExIceberg.Rest.Catalog.create_table(catalog, table_ident, fields, %{"owner" => "test"})
  """
  def create_table(
        %__MODULE__{nif_catalog_resource: nif_catalog_resource} = catalog,
        %TableIdent{} = table_ident,
        fields,
        properties \\ %{}
      ) do
    case Nif.rest_catalog_create_table(
           nif_catalog_resource,
           table_ident,
           fields,
           properties
         ) do
      {:ok, table_resource} ->
        table = ExIceberg.Table.new(table_resource)
        {:ok, catalog, table}

      {:error, reason} ->
        {:error, catalog, reason}
    end
  end

  @doc """
  Loads a table from the catalog.

  ## Parameters

  - `catalog` - The catalog struct
  - `table_ident` - TableIdent struct

  ## Returns

  `{:ok, updated_catalog, table}` - Success with Table struct
  `{:error, updated_catalog, reason}` - Error with reason

  ## Examples

      table_ident = TableIdent.from_string("my_namespace.my_table")
      {:ok, catalog, table} = ExIceberg.Rest.Catalog.load_table(catalog, table_ident)

      # Or create TableIdent explicitly
      namespace = NamespaceIdent.new("my_namespace")
      table_ident = TableIdent.new(namespace, "my_table")
      {:ok, catalog, table} = ExIceberg.Rest.Catalog.load_table(catalog, table_ident)
  """
  def load_table(
        %__MODULE__{nif_catalog_resource: nif_catalog_resource} = catalog,
        %TableIdent{} = table_ident
      ) do
    case Nif.rest_catalog_load_table(nif_catalog_resource, table_ident) do
      {:ok, table_resource} ->
        table = ExIceberg.Table.new(table_resource)
        {:ok, catalog, table}

      {:error, reason} ->
        {:error, catalog, reason}
    end
  end

  @doc """
  Renames a table in the catalog.

  ## Parameters

  - `catalog` - The catalog struct
  - `src_table_ident` - Source TableIdent struct
  - `dest_table_ident` - Destination TableIdent struct

  ## Returns

  `{:ok, updated_catalog, response}` - Success with response map
  `{:error, updated_catalog, reason}` - Error with reason

  ## Examples

      src_ident = TableIdent.from_string("my_namespace.old_table")
      dest_ident = TableIdent.from_string("my_namespace.new_table")
      {:ok, catalog, response} = ExIceberg.Rest.Catalog.rename_table(catalog, src_ident, dest_ident)

      # Or create TableIdent explicitly
      src_namespace = NamespaceIdent.new("my_namespace")
      dest_namespace = NamespaceIdent.new("other_namespace")
      src_ident = TableIdent.new(src_namespace, "old_table")
      dest_ident = TableIdent.new(dest_namespace, "new_table")
      {:ok, catalog, response} = ExIceberg.Rest.Catalog.rename_table(catalog, src_ident, dest_ident)
  """
  def rename_table(
        %__MODULE__{nif_catalog_resource: nif_catalog_resource} = catalog,
        %TableIdent{} = src_table_ident,
        %TableIdent{} = dest_table_ident
      ) do
    case Nif.rest_catalog_rename_table(
           nif_catalog_resource,
           src_table_ident,
           dest_table_ident
         ) do
      {:ok, response} -> {:ok, catalog, response}
      {:error, %{"error" => reason}} -> {:error, catalog, reason}
    end
  end
end
