# ExIceberg

**ExIceberg** is an Elixir library for interacting with [Apache Iceberg](https://iceberg.apache.org/) REST catalogs. Built on top of [iceberg-rust](https://github.com/apache/iceberg-rust) with native Rust NIFs for high performance.

## Features

- **REST Catalog Support** - Connect to Iceberg REST catalogs with OAuth2 authentication
- **Schema Definition** - Ecto-inspired API for defining table schemas
- **Cross-Platform** - Precompiled binaries for major platforms (no Rust toolchain required)
- **High Performance** - Native Rust implementation via NIFs

## Installation

Add `ex_iceberg` to your `mix.exs` dependencies:

```elixir
def deps do
  [
    {:ex_iceberg, "~> 0.3.0"}
  ]
end
```

## Usage

### Basic REST Catalog Operations

```elixir
# Configure catalog
config = %{
  uri: "http://localhost:8181/catalog",
  warehouse: "my_warehouse"
}

# Create catalog instance
catalog = ExIceberg.Rest.Catalog.new("my_catalog", config)

# List namespaces
{:ok, catalog, namespaces} = ExIceberg.Rest.Catalog.list_namespaces(catalog)

# Create namespace
{:ok, catalog, _} = ExIceberg.Rest.Catalog.create_namespace(catalog, "my_namespace", %{})

# Check if table exists
{:ok, catalog, exists?} = ExIceberg.Rest.Catalog.table_exists?(catalog, "my_namespace", "my_table")
```

### OAuth2 Authentication

```elixir
config = %{
  uri: "http://localhost:8181/catalog",
  warehouse: "my_warehouse",
  credential: "client_id:client_secret",
  oauth2_server_uri: "http://keycloak:8080/realms/iceberg/protocol/openid-connect/token",
  scope: "catalog"
}

catalog = ExIceberg.Rest.Catalog.new("my_catalog", config)
```

## Development

### Prerequisites

- Elixir 1.16+
- Rust toolchain (only for local compilation)
- Docker (for integration tests)

### Setup

```bash
# Install dependencies
mix deps.get

# Compile (downloads precompiled binaries automatically)
mix compile

# Run tests
mix test

# Run integration tests (requires Docker)
docker-compose up -d
mix test.integration
```

### Force Local Compilation

If you need to compile from source instead of using precompiled binaries:

```bash
EX_ICEBERG_BUILD=true mix compile
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for your changes
4. Ensure all tests pass
5. Submit a pull request

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Built on [iceberg-rust](https://github.com/apache/iceberg-rust) - The official Rust implementation of Apache Iceberg
- Inspired by [Ecto](https://github.com/elixir-ecto/ecto) - For the schema definition API
- Uses [Rustler](https://github.com/rusterlium/rustler) - For seamless Elixir/Rust integration
- Testing environment powered by [Lakekeeper](https://github.com/lakekeeper/lakekeeper) - Modern Iceberg REST catalog implementation
