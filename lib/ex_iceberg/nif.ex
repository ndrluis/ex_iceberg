defmodule ExIceberg.Nif do
  @moduledoc false

  version = Mix.Project.config()[:version]
  github_url = "https://github.com/ndrluis/ex_iceberg"

  use RustlerPrecompiled,
    otp_app: :ex_iceberg,
    base_url: "#{github_url}/releases/download/v#{version}",
    version: version,
    targets: ~w(
      aarch64-apple-darwin
      aarch64-unknown-linux-gnu
      aarch64-unknown-linux-musl
      x86_64-apple-darwin
      x86_64-pc-windows-msvc
      x86_64-pc-windows-gnu
      x86_64-unknown-linux-gnu
      x86_64-unknown-linux-musl
      arm-unknown-linux-gnueabihf
    ),
    nif_versions: ["2.15"],
    force_build: System.get_env("EX_ICEBERG_BUILD") in ["1", "true"]

  # REST Catalog NIF functions
  def rest_catalog_new(_config), do: :erlang.nif_error(:nif_not_loaded)
  def rest_catalog_list_namespaces(_catalog_resource), do: :erlang.nif_error(:nif_not_loaded)

  def rest_catalog_create_namespace(_catalog_resource, _namespace_ident, _properties),
    do: :erlang.nif_error(:nif_not_loaded)

  def rest_catalog_table_exists(_catalog_resource, _table_ident),
    do: :erlang.nif_error(:nif_not_loaded)

  def rest_catalog_drop_table(_catalog_resource, _table_ident),
    do: :erlang.nif_error(:nif_not_loaded)

  def rest_catalog_create_table(_catalog_resource, _table_ident, _fields, _properties),
    do: :erlang.nif_error(:nif_not_loaded)

  def rest_catalog_load_table(_catalog_resource, _table_ident),
    do: :erlang.nif_error(:nif_not_loaded)

  def rest_catalog_rename_table(_catalog_resource, _src_table_ident, _dest_table_ident),
    do: :erlang.nif_error(:nif_not_loaded)

  # Table operations using SmartTableResource
  def table_metadata(_table_resource), do: :erlang.nif_error(:nif_not_loaded)
  def table_metadata_ref(_table_resource), do: :erlang.nif_error(:nif_not_loaded)
  def table_inspect(_table_resource), do: :erlang.nif_error(:nif_not_loaded)
  def table_invalidate_cache(_table_resource), do: :erlang.nif_error(:nif_not_loaded)
end
