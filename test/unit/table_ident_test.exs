defmodule ExIceberg.TableIdentTest do
  use ExUnit.Case, async: true

  alias ExIceberg.{NamespaceIdent, TableIdent}

  describe "NamespaceIdent" do
    test "new/1 creates namespace from string" do
      ns = NamespaceIdent.new("simple")
      assert %NamespaceIdent{parts: ["simple"]} = ns
    end

    test "new/1 creates multi-level namespace" do
      ns = NamespaceIdent.new("level1.level2.level3")
      assert %NamespaceIdent{parts: ["level1", "level2", "level3"]} = ns
    end

    test "from_list/1 creates namespace from list" do
      ns = NamespaceIdent.from_list(["a", "b", "c"])
      assert %NamespaceIdent{parts: ["a", "b", "c"]} = ns
    end

    test "parent/1 returns parent namespace" do
      ns = NamespaceIdent.from_list(["level1", "level2", "level3"])
      assert {:ok, %NamespaceIdent{parts: ["level1", "level2"]}} = NamespaceIdent.parent(ns)
    end

    test "parent/1 returns :none for single level" do
      ns = NamespaceIdent.new("root")
      assert :none = NamespaceIdent.parent(ns)
    end

    test "to_string/1 returns dot-separated string" do
      ns = NamespaceIdent.from_list(["level1", "level2", "level3"])
      assert "level1.level2.level3" = NamespaceIdent.to_string(ns)
    end

    test "String.Chars protocol works" do
      ns = NamespaceIdent.from_list(["level1", "level2"])
      assert "level1.level2" = to_string(ns)
    end

    test "Inspect protocol works" do
      ns = NamespaceIdent.from_list(["level1", "level2"])
      assert "#NamespaceIdent<level1.level2>" = inspect(ns)
    end
  end

  describe "TableIdent" do
    test "new/2 creates table with namespace and name" do
      ns = NamespaceIdent.new("my_namespace")
      table = TableIdent.new(ns, "my_table")

      assert %TableIdent{
               namespace: %NamespaceIdent{parts: ["my_namespace"]},
               name: "my_table"
             } = table
    end

    test "from_string/1 creates table from dot-separated string" do
      table = TableIdent.from_string("ns1.ns2.my_table")

      assert %TableIdent{
               namespace: %NamespaceIdent{parts: ["ns1", "ns2"]},
               name: "my_table"
             } = table
    end

    test "from_string/1 creates table with empty namespace" do
      table = TableIdent.from_string("my_table")

      assert %TableIdent{
               namespace: %NamespaceIdent{parts: []},
               name: "my_table"
             } = table
    end

    test "from_list/1 creates table from list" do
      table = TableIdent.from_list(["ns1", "ns2", "my_table"])

      assert %TableIdent{
               namespace: %NamespaceIdent{parts: ["ns1", "ns2"]},
               name: "my_table"
             } = table
    end

    test "to_string/1 returns dot-separated string" do
      ns = NamespaceIdent.from_list(["ns1", "ns2"])
      table = TableIdent.new(ns, "my_table")

      assert "ns1.ns2.my_table" = TableIdent.to_string(table)
    end

    test "to_string/1 returns just table name for empty namespace" do
      ns = NamespaceIdent.from_list([])
      table = TableIdent.new(ns, "my_table")

      assert "my_table" = TableIdent.to_string(table)
    end

    test "String.Chars protocol works" do
      table = TableIdent.from_string("ns1.ns2.my_table")
      assert "ns1.ns2.my_table" = to_string(table)
    end

    test "Inspect protocol works" do
      table = TableIdent.from_string("ns1.ns2.my_table")
      assert "#TableIdent<ns1.ns2.my_table>" = inspect(table)
    end
  end
end
