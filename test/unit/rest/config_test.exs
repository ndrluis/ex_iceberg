defmodule ExIceberg.Rest.ConfigTest do
  use ExUnit.Case, async: true

  alias ExIceberg.Rest.Config

  describe "split_credential/1" do
    test "should split the credentials by :" do
      assert Config.split_credential("foo:bar") == {"foo", "bar"}
      assert Config.split_credential("foo") == {"foo", nil}
    end
  end
end
