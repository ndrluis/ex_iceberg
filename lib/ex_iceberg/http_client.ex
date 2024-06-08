defmodule ExIceberg.HTTPClient do
  @callback request(atom(), String.t(), map(), list()) :: {:ok, any()} | {:error, any()}
end
