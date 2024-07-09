defmodule ExIceberg.Catalog.TabularIntegrationTest do
  use ExUnit.Case, async: true
  alias ExIceberg.Rest.Catalog

  @moduletag :tabular

  describe "new/1" do
    test "successfully creates a new catalog" do
      Catalog.new("sandbox", %{
        uri: "https://api.tabular.io/ws/",
        credential: System.get_env("TABULAR_CREDENTIAL"),
        warehouse: "sandbox"
      })
    end
  end
end
