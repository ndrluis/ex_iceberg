defmodule ExIceberg.NamespaceIdent do
  @moduledoc """
  Represents a namespace identifier in Iceberg.

  A namespace identifier is composed of a list of string parts that form
  a hierarchical namespace path. For example, a namespace "level1.level2.level3"
  would be represented as `%NamespaceIdent{parts: ["level1", "level2", "level3"]}`.
  """

  @type t :: %__MODULE__{
          parts: [String.t()]
        }

  defstruct [:parts]

  @doc """
  Creates a new namespace identifier from a single string.

  ## Examples

      iex> ExIceberg.NamespaceIdent.new("my_namespace")
      %ExIceberg.NamespaceIdent{parts: ["my_namespace"]}

      iex> ExIceberg.NamespaceIdent.new("level1.level2.level3")
      %ExIceberg.NamespaceIdent{parts: ["level1", "level2", "level3"]}
  """
  @spec new(String.t()) :: t()
  def new(name) when is_binary(name) do
    parts = String.split(name, ".")
    %__MODULE__{parts: parts}
  end

  @doc """
  Creates a new namespace identifier from a list of strings.

  ## Examples

      iex> ExIceberg.NamespaceIdent.from_list(["level1", "level2", "level3"])
      %ExIceberg.NamespaceIdent{parts: ["level1", "level2", "level3"]}
  """
  @spec from_list([String.t()]) :: t()
  def from_list(parts) when is_list(parts) do
    %__MODULE__{parts: parts}
  end

  @doc """
  Returns the parent namespace, if any.

  ## Examples

      iex> ns = ExIceberg.NamespaceIdent.from_list(["level1", "level2", "level3"])
      iex> ExIceberg.NamespaceIdent.parent(ns)
      {:ok, %ExIceberg.NamespaceIdent{parts: ["level1", "level2"]}}

      iex> ns = ExIceberg.NamespaceIdent.new("root")
      iex> ExIceberg.NamespaceIdent.parent(ns)
      :none
  """
  @spec parent(t()) :: {:ok, t()} | :none
  def parent(%__MODULE__{parts: [_]}), do: :none
  def parent(%__MODULE__{parts: []}), do: :none

  def parent(%__MODULE__{parts: parts}) do
    parent_parts = Enum.drop(parts, -1)
    {:ok, %__MODULE__{parts: parent_parts}}
  end

  @doc """
  Returns the namespace as a dot-separated string.

  ## Examples

      iex> ns = ExIceberg.NamespaceIdent.from_list(["level1", "level2", "level3"])
      iex> ExIceberg.NamespaceIdent.to_string(ns)
      "level1.level2.level3"
  """
  @spec to_string(t()) :: String.t()
  def to_string(%__MODULE__{parts: parts}) do
    Enum.join(parts, ".")
  end

  defimpl String.Chars do
    def to_string(namespace_ident) do
      ExIceberg.NamespaceIdent.to_string(namespace_ident)
    end
  end

  defimpl Inspect do
    def inspect(%ExIceberg.NamespaceIdent{parts: parts}, _opts) do
      "#NamespaceIdent<#{Enum.join(parts, ".")}>"
    end
  end
end
