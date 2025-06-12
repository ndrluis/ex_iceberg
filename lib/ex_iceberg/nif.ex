defmodule ExIceberg.Nif do
  @moduledoc false

  version = Mix.Project.config()[:version]
  github_url = "https://github.com/ndrluis/ex_iceberg"

  use RustlerPrecompiled,
    otp_app: :ex_iceberg,
    base_url: "#{github_url}/releases/download/v#{version}",
    version: version,
    nif_versions: ["2.15"],
    force_build: System.get_env("EX_ICEBERG_BUILD") in ["1", "true"]

  # REST Catalog NIF functions
  def rest_catalog_new(_config), do: :erlang.nif_error(:nif_not_loaded)
  def rest_catalog_list_namespaces(_catalog_resource), do: :erlang.nif_error(:nif_not_loaded)

  def rest_catalog_create_namespace(_catalog_resource, _namespace, _properties),
    do: :erlang.nif_error(:nif_not_loaded)

  def rest_catalog_table_exists(_catalog_resource, _namespace, _table_name),
    do: :erlang.nif_error(:nif_not_loaded)

  def rest_catalog_drop_table(_catalog_resource, _namespace, _table_name),
    do: :erlang.nif_error(:nif_not_loaded)

  def rest_catalog_create_table(_catalog_resource, _namespace, _table_name, _fields, _properties),
    do: :erlang.nif_error(:nif_not_loaded)
end
