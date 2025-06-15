defmodule ExIceberg.Schema do
  @moduledoc """
  Schema definition for Iceberg tables using a declarative API similar to Ecto.Schema.

  This module provides macros to define table schemas with proper type safety
  and a more Elixir-like API for working with Apache Iceberg tables.

  ## Example

      defmodule MyApp.UserSchema do
        use ExIceberg.Schema

        schema "users" do
          field :id, :long, required: true
          field :name, :string
          field :email, :string, required: true
          field :age, :int
          field :active, :boolean, default: true
          field :metadata, ExIceberg.Types.map(:string, :string)
          field :tags, ExIceberg.Types.list(:string)
          field :address, ExIceberg.Types.struct([
            ExIceberg.Types.field("street", :string),
            ExIceberg.Types.field("city", :string),
            ExIceberg.Types.field("zip", :int)
          ])
          field :balance, ExIceberg.Types.decimal(10, 2)
          field :created_at, :timestamp, required: true
          field :updated_at, :timestamptz
        end
      end

      # Usage:
      table_ident = ExIceberg.TableIdent.from_string("my_namespace.users")
      {:ok, catalog, _} = MyApp.UserSchema.create_table(catalog, table_ident)

  ## Supported Types

  ### Primitive Types
  - `:boolean` - True or false
  - `:int` - 32-bit signed integer
  - `:long` - 64-bit signed integer
  - `:float` - 32-bit IEEE 754 floating point
  - `:double` - 64-bit IEEE 754 floating point
  - `:string` - UTF-8 character sequences
  - `:uuid` - Universally unique identifiers
  - `:date` - Calendar date without timezone
  - `:time` - Time of day in microsecond precision
  - `:timestamp` - Timestamp in microsecond precision, without timezone
  - `:timestamptz` - Timestamp in microsecond precision, with timezone
  - `:binary` - Arbitrary-length byte array

  ### Complex Types
  Use the helper functions from `ExIceberg.Types`:
  - `ExIceberg.Types.decimal(precision, scale)` - Fixed point decimal
  - `ExIceberg.Types.fixed(length)` - Fixed-length byte array
  - `ExIceberg.Types.list(element_type, opts)` - List of elements
  - `ExIceberg.Types.map(key_type, value_type, opts)` - Map of key-value pairs
  - `ExIceberg.Types.struct(fields)` - Struct with named fields

  ## Field Options
  - `:required` - Whether the field is required (default: false)
  - `:field_id` - Explicit field ID (auto-assigned if not provided)
  """

  defmacro __using__(_opts) do
    quote do
      import ExIceberg.Schema
      Module.register_attribute(__MODULE__, :iceberg_fields, accumulate: true)
      Module.register_attribute(__MODULE__, :iceberg_table_name, [])
      @before_compile ExIceberg.Schema
    end
  end

  @doc """
  Defines the table schema with the given name and fields.

  ## Example

      schema "my_table" do
        field :id, :long, required: true
        field :name, :string
      end
  """
  defmacro schema(table_name, do: block) do
    quote do
      @iceberg_table_name unquote(table_name)
      unquote(block)
    end
  end

  @doc """
  Defines a field in the table schema.

  ## Parameters
  - `name` - The field name (atom)
  - `type` - The field type (atom or structured type)
  - `opts` - Field options (keyword list)

  ## Examples

      field :id, :long, required: true
      field :price, ExIceberg.Types.decimal(10, 2)
      field :tags, ExIceberg.Types.list(:string, element_required: false)
      field :metadata, ExIceberg.Types.map(:string, :string)
      field :address, ExIceberg.Types.struct([
        ExIceberg.Types.field("street", :string),
        ExIceberg.Types.field("city", :string)
      ])
  """
  defmacro field(name, type, opts \\ []) do
    quote do
      @iceberg_fields {unquote(name), unquote(type), unquote(opts)}
    end
  end

  defmacro __before_compile__(env) do
    fields = Module.get_attribute(env.module, :iceberg_fields) |> Enum.reverse()
    table_name = Module.get_attribute(env.module, :iceberg_table_name)

    field_structs =
      fields
      |> Enum.with_index(1)
      |> Enum.map(fn {{name, type, opts}, index} ->
        build_field_struct(name, type, opts, index)
      end)
      |> Macro.escape()

    quote do
      def __table_name__, do: unquote(table_name)
      def __fields__, do: unquote(field_structs)

      @doc """
      Creates the table in the specified catalog using the defined schema.

      ## Parameters
      - `catalog` - The catalog instance
      - `table_ident` - The TableIdent struct identifying the table
      - `properties` - Optional table properties (default: %{})

      ## Returns
      `{:ok, updated_catalog, response}` on success, `{:error, updated_catalog, reason}` on failure.

      ## Example
          table_ident = ExIceberg.TableIdent.from_string("my_namespace.my_table")
          {:ok, catalog, response} = MySchema.create_table(catalog, table_ident, %{"owner" => "team"})
      """
      def create_table(catalog, %ExIceberg.TableIdent{} = table_ident, properties \\ %{}) do
        ExIceberg.Rest.Catalog.create_table(
          catalog,
          table_ident,
          __fields__(),
          properties
        )
      end
    end
  end

  defp build_field_struct(name, type, opts, index) do
    %ExIceberg.Types.Field{
      name: to_string(name),
      field_type: type,
      required: Keyword.get(opts, :required, false),
      field_id: Keyword.get(opts, :field_id, index)
    }
  end
end
