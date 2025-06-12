defmodule ExIceberg.Nif do
  @moduledoc false

  version = Mix.Project.config()[:version]
  github_url = "https://github.com/ndrluis/ex_iceberg"

  use RustlerPrecompiled,
    otp_app: :ex_iceberg,
    crate: "ex_iceberg_nif",
    base_url: "#{github_url}/releases/download/v#{version}",
    version: version,
    targets: [
      "aarch64-apple-darwin",
      "aarch64-unknown-linux-gnu",
      "aarch64-unknown-linux-musl",
      "x86_64-apple-darwin",
      "x86_64-pc-windows-gnu",
      "x86_64-pc-windows-msvc",
      "x86_64-unknown-linux-gnu",
      "x86_64-unknown-linux-musl"
    ],
    nif_versions: ["2.15", "2.16", "2.17"],
    force_build: true

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
