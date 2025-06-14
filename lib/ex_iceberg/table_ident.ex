defmodule ExIceberg.TableIdent do
  @moduledoc """
  Represents a table identifier in Iceberg.

  A table identifier combines a namespace and a table name to uniquely
  identify a table within a catalog.
  """

  alias ExIceberg.NamespaceIdent

  @type t :: %__MODULE__{
          namespace: NamespaceIdent.t(),
          name: String.t()
        }

  defstruct [:namespace, :name]

  @doc """
  Creates a new table identifier.

  ## Examples

      iex> ns = ExIceberg.NamespaceIdent.new("my_namespace")
      iex> ExIceberg.TableIdent.new(ns, "my_table")
      %ExIceberg.TableIdent{
        namespace: %ExIceberg.NamespaceIdent{parts: ["my_namespace"]},
        name: "my_table"
      }
  """
  @spec new(NamespaceIdent.t(), String.t()) :: t()
  def new(%NamespaceIdent{} = namespace, name) when is_binary(name) do
    %__MODULE__{namespace: namespace, name: name}
  end

  @doc """
  Creates a table identifier from a dot-separated string.

  The last part becomes the table name, and all preceding parts
  form the namespace.

  ## Examples

      iex> ExIceberg.TableIdent.from_string("ns1.ns2.my_table")
      %ExIceberg.TableIdent{
        namespace: %ExIceberg.NamespaceIdent{parts: ["ns1", "ns2"]},
        name: "my_table"
      }

      iex> ExIceberg.TableIdent.from_string("my_table")
      %ExIceberg.TableIdent{
        namespace: %ExIceberg.NamespaceIdent{parts: []},
        name: "my_table"
      }
  """
  @spec from_string(String.t()) :: t()
  def from_string(table_path) when is_binary(table_path) do
    parts = String.split(table_path, ".")
    {namespace_parts, [table_name]} = Enum.split(parts, -1)

    namespace = NamespaceIdent.from_list(namespace_parts)
    %__MODULE__{namespace: namespace, name: table_name}
  end

  @doc """
  Creates a table identifier from a list of strings.

  The last element becomes the table name, and all preceding elements
  form the namespace.

  ## Examples

      iex> ExIceberg.TableIdent.from_list(["ns1", "ns2", "my_table"])
      %ExIceberg.TableIdent{
        namespace: %ExIceberg.NamespaceIdent{parts: ["ns1", "ns2"]},
        name: "my_table"
      }
  """
  @spec from_list([String.t()]) :: t()
  def from_list(parts) when is_list(parts) and length(parts) > 0 do
    {namespace_parts, [table_name]} = Enum.split(parts, -1)
    namespace = NamespaceIdent.from_list(namespace_parts)
    %__MODULE__{namespace: namespace, name: table_name}
  end

  @doc """
  Returns the table identifier as a dot-separated string.

  ## Examples

      iex> ns = ExIceberg.NamespaceIdent.from_list(["ns1", "ns2"])
      iex> table = ExIceberg.TableIdent.new(ns, "my_table")
      iex> ExIceberg.TableIdent.to_string(table)
      "ns1.ns2.my_table"
  """
  @spec to_string(t()) :: String.t()
  def to_string(%__MODULE__{namespace: namespace, name: name}) do
    case namespace.parts do
      [] -> name
      parts -> Enum.join(parts ++ [name], ".")
    end
  end

  defimpl String.Chars do
    def to_string(table_ident) do
      ExIceberg.TableIdent.to_string(table_ident)
    end
  end

  defimpl Inspect do
    def inspect(%ExIceberg.TableIdent{} = table_ident, _opts) do
      "#TableIdent<#{ExIceberg.TableIdent.to_string(table_ident)}>"
    end
  end
end
