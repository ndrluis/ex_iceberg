# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
* Add Table module with metadata access and inspection capabilities
* Add MetadataTable module for table inspection (snapshots, manifests)
* Add SnapshotsTable and ManifestsTable modules for metadata inspection
* Add load_table functionality to REST catalog returning Table instances
* Add rename_table functionality to REST catalog for table renaming and namespace migration

### Changed
* Upgrade iceberg-rust to specific commit bcd1033ba5 for HTTPS protocol support and OAuth2 content-type fix
* Improve Rust code idiomaticity by refactoring build_config() method to use pattern matching instead of if-else chains

### Fixed
* Fix HTTPS to HTTP conversion issue when connecting to production Iceberg clusters with Keycloak authentication
* Fix OAuth2 authentication missing content-type header issue

## [0.3.0]

### Added
* Minimal implementation for REST Catalog using Iceberg Rust
  * list_namespaces
  * create_namespace
  * table_exists?
  * drop_table
  * create_table
* Schema implementation based on Ecto Schema style

### Changed
* Rewritten to use iceberg-rust

### Removed
* Elixir implementation to connect with Rest Catalog.

## [0.2.0]

Official first release

## [0.1.0]

First release (retired)
