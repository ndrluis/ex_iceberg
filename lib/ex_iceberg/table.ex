defmodule ExIceberg.Table do
  @moduledoc """
  Table represents an Apache Iceberg table.

  This module wraps the Rust Table implementation and provides Elixir-friendly
  access to table metadata and inspection capabilities.
  """

  alias ExIceberg.Nif
  alias ExIceberg.Table.{MetadataTable}

  defstruct [:table_resource]

  @type t :: %__MODULE__{
          table_resource: reference()
        }

  @doc """
  Creates a new Table struct from a table resource.

  This is typically called internally by catalog operations like load_table.
  """
  def new(table_resource) when is_reference(table_resource) do
    %__MODULE__{table_resource: table_resource}
  end

  @doc """
  Returns the table's metadata as a map.

  The metadata includes:
  - table_uuid: Unique identifier for the table
  - format_version: Iceberg format version
  - location: Table's root location
  - schema_id: Current schema ID
  - fields: Schema fields as a list
  - properties: Table properties

  ## Examples

      metadata = ExIceberg.Table.metadata(table)
      # => %{
      #   "table_uuid" => "...",
      #   "format_version" => 2,
      #   "location" => "s3://bucket/path",
      #   "schema_id" => 1,
      #   "fields" => [...],
      #   "properties" => %{...}
      # }
  """
  def metadata(%__MODULE__{table_resource: table_resource}) when is_reference(table_resource) do
    case Nif.table_metadata(table_resource) do
      {:ok, raw_metadata} ->
        # Parse JSON strings back to Elixir terms
        parsed_metadata = %{
          "table_uuid" => raw_metadata["table_uuid"],
          "format_version" => String.to_integer(raw_metadata["format_version"]),
          "location" => raw_metadata["location"],
          "schema_id" => String.to_integer(raw_metadata["schema_id"]),
          "fields" => Jason.decode!(raw_metadata["fields"]),
          "properties" => Jason.decode!(raw_metadata["properties"])
        }

        # Return parsed metadata
        parsed_metadata

      {:error, %{"error" => reason}} ->
        raise "Failed to get table metadata: #{reason}"
    end
  end

  @doc """
  Returns the table's metadata reference.

  This is similar to metadata/1 but may provide a reference-based
  approach for performance in future implementations.

  ## Examples

      metadata_ref = ExIceberg.Table.metadata_ref(table)
  """
  def metadata_ref(%__MODULE__{table_resource: table_resource})
      when is_reference(table_resource) do
    case Nif.table_metadata_ref(table_resource) do
      {:ok, raw_metadata} ->
        # Parse JSON strings back to Elixir terms
        %{
          "table_uuid" => raw_metadata["table_uuid"],
          "format_version" => String.to_integer(raw_metadata["format_version"]),
          "location" => raw_metadata["location"],
          "schema_id" => String.to_integer(raw_metadata["schema_id"]),
          "fields" => Jason.decode!(raw_metadata["fields"]),
          "properties" => Jason.decode!(raw_metadata["properties"])
        }

      {:error, %{"error" => reason}} ->
        raise "Failed to get table metadata reference: #{reason}"
    end
  end

  @doc """
  Returns a MetadataTable for inspecting table metadata.

  The inspect functionality provides table-like access to metadata
  including snapshots, manifests, and other inspection capabilities.

  ## Examples

      metadata_table = ExIceberg.Table.inspect(table)
      # Returns a MetadataTable struct that can access snapshots, manifests, etc.
  """
  def inspect(%__MODULE__{table_resource: table_resource}) when is_reference(table_resource) do
    case Nif.table_inspect(table_resource) do
      {:ok, inspect_data} ->
        # Get metadata for MetadataTable
        metadata = metadata(%__MODULE__{table_resource: table_resource})
        MetadataTable.new(inspect_data, metadata)

      {:error, %{"error" => reason}} ->
        raise "Failed to inspect table: #{reason}"
    end
  end

  @doc """
  Invalidates the metadata cache for this table.

  This forces the next metadata() call to fetch fresh data from the catalog.
  The cache will remain invalidated until metadata is fetched again.

  Use this when you know the table metadata has changed and you need fresh data.

  ## Examples

      # After updating table properties or schema
      ExIceberg.Table.invalidate_cache(table)
      
      # Next call will fetch fresh metadata from catalog
      metadata = ExIceberg.Table.metadata(table)
  """
  def invalidate_cache(%__MODULE__{table_resource: table_resource})
      when is_reference(table_resource) do
    Nif.table_invalidate_cache(table_resource)
    :ok
  end
end
