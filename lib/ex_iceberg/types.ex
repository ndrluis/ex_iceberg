defmodule ExIceberg.Types do
  @moduledoc """
  Type definitions for Iceberg schemas.

  This module provides structured type definitions that correspond to the Rust NIF types,
  offering a more type-safe and idiomatic approach compared to string-based type definitions.
  """

  defmodule Field do
    @moduledoc """
    Represents a field in an Iceberg table schema.

    This struct is used to define table fields with proper type safety
    and corresponds directly to the Rust `IcebergField` struct.
    """

    defstruct [:name, :field_type, :required, :field_id]

    @type t :: %__MODULE__{
            name: String.t(),
            field_type: ExIceberg.Types.type(),
            required: boolean(),
            field_id: integer() | nil
          }
  end

  defmodule Decimal do
    @moduledoc "Represents a decimal type with precision and scale."
    defstruct [:precision, :scale]
    @type t :: %__MODULE__{precision: pos_integer(), scale: non_neg_integer()}
  end

  defmodule Fixed do
    @moduledoc "Represents a fixed-length binary type."
    defstruct [:length]
    @type t :: %__MODULE__{length: pos_integer()}
  end

  defmodule List do
    @moduledoc "Represents a list type with element type and requirements."
    defstruct [:element_type, :element_required]
    @type t :: %__MODULE__{element_type: ExIceberg.Types.type(), element_required: boolean()}
  end

  defmodule Map do
    @moduledoc "Represents a map type with key-value types and requirements."
    defstruct [:key_type, :value_type, :value_required]

    @type t :: %__MODULE__{
            key_type: ExIceberg.Types.type(),
            value_type: ExIceberg.Types.type(),
            value_required: boolean()
          }
  end

  defmodule Struct do
    @moduledoc "Represents a struct type with named fields."
    defstruct [:fields]
    @type t :: %__MODULE__{fields: [ExIceberg.Types.Field.t()]}
  end

  @type type ::
          :boolean
          | :int
          | :long
          | :float
          | :double
          | :string
          | :uuid
          | :date
          | :time
          | :timestamp
          | :timestamptz
          | :timestamp_ns
          | :timestamptz_ns
          | :binary
          | {:decimal, precision: pos_integer(), scale: non_neg_integer()}
          | {:fixed, length: pos_integer()}
          | {:list, element_type: type(), element_required: boolean()}
          | {:map, key_type: type(), value_type: type(), value_required: boolean()}
          | {:struct, fields: [Field.t()]}

  @doc """
  Creates a new field with the given name, type, and options.

  ## Examples

      iex> ExIceberg.Types.field("id", :long, required: true)
      %ExIceberg.Types.Field{name: "id", field_type: :long, required: true, field_id: nil}

      iex> ExIceberg.Types.field("price", {:decimal, precision: 10, scale: 2})
      %ExIceberg.Types.Field{name: "price", field_type: {:decimal, precision: 10, scale: 2}, required: false, field_id: nil}
  """
  def field(name, type, opts \\ []) do
    %Field{
      name: to_string(name),
      field_type: type,
      required: Keyword.get(opts, :required, false),
      field_id: Keyword.get(opts, :field_id)
    }
  end

  @doc """
  Creates a list type with the specified element type.

  ## Examples

      iex> ExIceberg.Types.list(:string)
      {:list, element_type: :string, element_required: false}

      iex> ExIceberg.Types.list(:int, element_required: true)
      {:list, element_type: :int, element_required: true}
  """
  def list(element_type, opts \\ []) do
    {:list,
     %{
       element_type: element_type,
       element_required: Keyword.get(opts, :element_required, false)
     }}
  end

  @doc """
  Creates a map type with the specified key and value types.

  ## Examples

      iex> ExIceberg.Types.map(:string, :int)
      {:map, key_type: :string, value_type: :int, value_required: false}

      iex> ExIceberg.Types.map(:string, :string, value_required: true)
      {:map, key_type: :string, value_type: :string, value_required: true}
  """
  def map(key_type, value_type, opts \\ []) do
    {:map,
     %{
       key_type: key_type,
       value_type: value_type,
       value_required: Keyword.get(opts, :value_required, false)
     }}
  end

  @doc """
  Creates a struct type with the specified fields.

  ## Examples

      iex> ExIceberg.Types.struct([
      ...>   ExIceberg.Types.field("street", :string),
      ...>   ExIceberg.Types.field("city", :string),
      ...>   ExIceberg.Types.field("zip", :int)
      ...> ])
      {:struct, fields: [
        %ExIceberg.Types.Field{name: "street", field_type: :string, required: false, field_id: nil},
        %ExIceberg.Types.Field{name: "city", field_type: :string, required: false, field_id: nil},
        %ExIceberg.Types.Field{name: "zip", field_type: :int, required: false, field_id: nil}
      ]}
  """
  def struct(fields) when is_list(fields) do
    {:struct, %{fields: fields}}
  end

  @doc """
  Creates a decimal type with the specified precision and scale.

  ## Examples

      iex> ExIceberg.Types.decimal(10, 2)
      {:decimal, precision: 10, scale: 2}
  """
  def decimal(precision, scale) do
    {:decimal, %{precision: precision, scale: scale}}
  end

  @doc """
  Creates a fixed-length binary type with the specified length.

  ## Examples

      iex> ExIceberg.Types.fixed(16)
      {:fixed, length: 16}
  """
  def fixed(length) do
    {:fixed, %{length: length}}
  end
end
