defmodule ExIceberg.Rest.Config do
  defstruct uri: nil, prefix: nil, token: nil

  def new(options) do
    struct(__MODULE__, options)
  end
end
