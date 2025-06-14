defmodule ExIceberg.Table.MetadataTable do
  @moduledoc """
  MetadataTable provides table-like access to inspect table metadata.

  This module allows you to inspect table history, snapshots, manifests,
  and other metadata as structured data.

  Similar to iceberg-rust's MetadataTable functionality.
  """

  alias ExIceberg.Table.{SnapshotsTable, ManifestsTable}

  defstruct [:inspect_data, :table_metadata]

  @type t :: %__MODULE__{
          inspect_data: map(),
          table_metadata: map()
        }

  @doc """
  Creates a new MetadataTable from inspect data and table metadata.

  This is typically called by ExIceberg.Table.inspect/1.
  """
  def new(inspect_data, table_metadata) do
    %__MODULE__{
      inspect_data: inspect_data,
      table_metadata: table_metadata
    }
  end

  @doc """
  Returns basic information about the table.

  ## Examples

      info = ExIceberg.Table.MetadataTable.info(metadata_table)
      # => %{
      #   "identifier" => "namespace.table_name",
      #   "location" => "s3://bucket/path",
      #   "table_uuid" => "...",
      #   "current_snapshot_id" => "...",
      #   "sequence_number" => "..."
      # }
  """
  def info(%__MODULE__{inspect_data: inspect_data}) do
    inspect_data
  end

  @doc """
  Returns a SnapshotsTable for accessing snapshot information.

  ## Examples

      snapshots_table = ExIceberg.Table.MetadataTable.snapshots(metadata_table)
  """
  def snapshots(%__MODULE__{table_metadata: table_metadata}) do
    SnapshotsTable.new(table_metadata)
  end

  @doc """
  Returns a ManifestsTable for accessing manifest information.

  ## Examples

      manifests_table = ExIceberg.Table.MetadataTable.manifests(metadata_table)
  """
  def manifests(%__MODULE__{table_metadata: table_metadata}) do
    ManifestsTable.new(table_metadata)
  end
end
