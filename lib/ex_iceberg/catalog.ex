defmodule ExIceberg.Catalog do
  @moduledoc """
  This module provides functions to interact with the catalog.

  ## Usage Example

      catalog = ExIceberg.Catalog.new("my_catalog", :rest, %{})

      {:ok, catalog, _} = ExIceberg.Catalog.create_namespace(catalog, "my_namespace", %{})
      {:ok, catalog, _} = ExIceberg.Catalog.list_namespaces(catalog)

    As you see in the example above, you need to pass the reasign and pass the catalog to the next function.
  """

  @callback create_namespace(t(), String.t(), map()) :: {:ok, t(), map()} | {:error, String.t()}
  @callback list_namespaces(t()) :: {:ok, t(), list(String.t())} | {:error, String.t()}
  @callback drop_namespace(t(), String.t()) :: {:ok, t(), map()} | {:error, String.t()}
  @callback load_namespace_metadata(t(), String.t()) :: {:ok, t(), map()} | {:error, String.t()}
  @callback namespace_exists?(t(), String.t()) :: {:ok, t(), boolean()} | {:error, String.t()}
  @callback update_namespace_properties(t(), String.t(), list(), map()) ::
              {:ok, t(), map()} | {:error, String.t()}

  @type t :: %{
          name: String.t(),
          type: String.t(),
          config: struct()
        }

  alias ExIceberg.Rest.Catalog, as: RestCatalog

  @doc """
  Defines a catalog to be accessed by the client.

  ## Examples

      iex> ExIceberg.Catalog.new("my_catalog", :rest, %{})
      %ExIceberg.Rest.Catalog{name: "my_catalog", config: %{}}

  ## Catalog Types

    * `:rest` - REST API catalog.

  ## Configuration

    The configuration for the catalog is specific to the catalog type.

    * [Rest Catalog Configuration](`ExIceberg.Rest.Config`)
  """
  def new(name, :rest = _type, config), do: RestCatalog.new(name, config)
  def new(_name, _type, _config), do: {:error, "Invalid catalog type"}

  @doc """
  Creates a new namespace in the catalog.

  ## Examples

      iex> ExIceberg.Catalog.create_namespace(catalog, "my_namespace", %{})
      {:ok, catalog, %{}}
  """
  def create_namespace(catalog, namespace, properties) do
    catalog.__struct__.create_namespace(catalog, namespace, properties)
  end

  @doc """
  Lists the namespaces in the catalog.

  ## Examples

      iex> ExIceberg.Catalog.list_namespaces(catalog)
      {:ok, catalog, ["namespace1", "namespace2"]}
  """
  def list_namespaces(catalog) do
    catalog.__struct__.list_namespaces(catalog)
  end

  @doc """
  Drops a namespace from the catalog.

  ## Examples

      iex> ExIceberg.Catalog.drop_namespace(catalog, "my_namespace")
      {:ok, catalog, %{}}
  """
  def drop_namespace(catalog, namespace) do
    catalog.__struct__.drop_namespace(catalog, namespace)
  end

  @doc """
  Loads the metadata for a namespace in the catalog.

  ## Examples

      iex> ExIceberg.Catalog.load_namespace_metadata(catalog, "my_namespace")
      {:ok, catalog, %{}}
  """
  def load_namespace_metadata(catalog, namespace) do
    catalog.__struct__.load_namespace_metadata(catalog, namespace)
  end

  @doc """
  Checks if a namespace exists in the catalog.

  ## Examples

      iex> ExIceberg.Catalog.namespace_exists?(catalog, "my_namespace")
      {:ok, catalog, true}
  """
  def namespace_exists?(catalog, namespace) do
    catalog.__struct__.namespace_exists?(catalog, namespace)
  end

  @doc """
  Updates the properties of a namespace in the catalog.

  ## Examples

      iex> ExIceberg.Catalog.update_namespace_properties(catalog, "my_namespace", [], %{})
      {:ok, catalog, %{}}
  """
  def update_namespace_properties(catalog, namespace, removals, updates) do
    catalog.__struct__.update_namespace_properties(catalog, namespace, removals, updates)
  end
end
