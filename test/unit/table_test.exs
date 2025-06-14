defmodule ExIceberg.TableTest do
  use ExUnit.Case, async: true
  # Table tests were removed as Table can only be created via catalog operations
end

defmodule ExIceberg.Table.MetadataTableTest do
  use ExUnit.Case, async: true

  alias ExIceberg.Table.{MetadataTable, SnapshotsTable, ManifestsTable}

  setup do
    inspect_data = %{
      "identifier" => "550e8400-e29b-41d4-a716-446655440000",
      "location" => "s3://bucket/path",
      "table_uuid" => "550e8400-e29b-41d4-a716-446655440000"
    }

    table_metadata = %{
      "table_uuid" => "550e8400-e29b-41d4-a716-446655440000",
      "format_version" => 2,
      "location" => "s3://bucket/path"
    }

    metadata_table = MetadataTable.new(inspect_data, table_metadata)

    %{metadata_table: metadata_table, inspect_data: inspect_data, table_metadata: table_metadata}
  end

  describe "MetadataTable.info/1" do
    test "returns inspect data", %{metadata_table: metadata_table, inspect_data: inspect_data} do
      info = MetadataTable.info(metadata_table)
      assert info == inspect_data
    end
  end

  describe "MetadataTable.snapshots/1" do
    test "returns SnapshotsTable", %{
      metadata_table: metadata_table,
      table_metadata: table_metadata
    } do
      snapshots_table = MetadataTable.snapshots(metadata_table)

      assert %SnapshotsTable{table_metadata: ^table_metadata} = snapshots_table
    end
  end

  describe "MetadataTable.manifests/1" do
    test "returns ManifestsTable", %{
      metadata_table: metadata_table,
      table_metadata: table_metadata
    } do
      manifests_table = MetadataTable.manifests(metadata_table)

      assert %ManifestsTable{table_metadata: ^table_metadata} = manifests_table
    end
  end
end

defmodule ExIceberg.Table.SnapshotsTableTest do
  use ExUnit.Case, async: true

  alias ExIceberg.Table.SnapshotsTable

  describe "SnapshotsTable.new/1" do
    test "creates snapshots table from metadata" do
      table_metadata = %{"table_uuid" => "test"}
      snapshots_table = SnapshotsTable.new(table_metadata)

      assert %SnapshotsTable{table_metadata: ^table_metadata} = snapshots_table
    end
  end

  describe "SnapshotsTable.schema/1" do
    test "returns snapshots schema" do
      table_metadata = %{}
      snapshots_table = SnapshotsTable.new(table_metadata)
      schema = SnapshotsTable.schema(snapshots_table)

      assert is_map(schema)
      assert Map.has_key?(schema, "fields")
      assert is_list(schema["fields"])
    end
  end

  describe "SnapshotsTable.info/1" do
    test "returns basic info" do
      table_metadata = %{}
      snapshots_table = SnapshotsTable.new(table_metadata)
      info = SnapshotsTable.info(snapshots_table)

      assert is_map(info)
      assert Map.has_key?(info, "description")
    end
  end
end

defmodule ExIceberg.Table.ManifestsTableTest do
  use ExUnit.Case, async: true

  alias ExIceberg.Table.ManifestsTable

  describe "ManifestsTable.new/1" do
    test "creates manifests table from metadata" do
      table_metadata = %{"table_uuid" => "test"}
      manifests_table = ManifestsTable.new(table_metadata)

      assert %ManifestsTable{table_metadata: ^table_metadata} = manifests_table
    end
  end

  describe "ManifestsTable.schema/1" do
    test "returns manifests schema" do
      table_metadata = %{}
      manifests_table = ManifestsTable.new(table_metadata)
      schema = ManifestsTable.schema(manifests_table)

      assert is_map(schema)
      assert Map.has_key?(schema, "fields")
      assert is_list(schema["fields"])
    end
  end

  describe "ManifestsTable.info/1" do
    test "returns basic info" do
      table_metadata = %{}
      manifests_table = ManifestsTable.new(table_metadata)
      info = ManifestsTable.info(manifests_table)

      assert is_map(info)
      assert Map.has_key?(info, "description")
    end
  end
end
