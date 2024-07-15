defmodule Unit.Rest.ConfigTest do
  use ExUnit.Case, async: true

  alias ExIceberg.Rest.Config

  describe "split_credential/1" do
    test "split the credentials by :" do
      assert Config.split_credential("foo:bar") == {"foo", "bar"}
      assert Config.split_credential("foo") == {"foo", nil}
    end
  end

  describe "parse/1" do
    test "parse the config" do
      config = %{
        "uri" => "http://localhost:8181",
        "prefix" => "prefix",
        "token" => "token",
        "credential" => "credential",
        "scope" => "scope",
        "s3.endpoint" => "s3.endpoint",
        "s3.access-key-id" => "s3.access-key-id"
      }

      assert Config.parse(config) == %Config{
               uri: "http://localhost:8181",
               prefix: "prefix",
               token: "token",
               credential: "credential",
               scope: "scope",
               grant_type: "client_credentials",
               s3: %ExIceberg.S3.Config{
                 endpoint: "s3.endpoint",
                 access_key_id: "s3.access-key-id"
               }
             }
    end
  end

  describe "merge/3" do
    test "merge the configs" do
      local_config = %Config{
        uri: "http://local_config:8181",
        prefix: "prefix-local",
        token: "token-local",
        s3: %ExIceberg.S3.Config{
          endpoint: "s3.endpoint.local",
          access_key_id: "s3.access-key-id.local"
        }
      }

      default_config = %Config{
        audience: "audience.default",
        s3: %ExIceberg.S3.Config{
          access_key_id: "s3.access-key-id.default",
          delete_enabled: "s3.delete-enabled.default"
        }
      }

      override_config = %Config{
        prefix: "prefix-override",
        resource: "resource-override",
        s3: %ExIceberg.S3.Config{
          endpoint: "s3.endpoint.override",
          delete_enabled: "s3.delete-enabled.override",
          region: "s3.region.override"
        }
      }

      assert Config.merge(local_config, default_config, override_config) ==
               %ExIceberg.Rest.Config{
                 audience: "audience.default",
                 prefix: "prefix-override",
                 resource: "resource-override",
                 s3: %ExIceberg.S3.Config{
                   access_key_id: "s3.access-key-id.local",
                   delete_enabled: "s3.delete-enabled.override",
                   endpoint: "s3.endpoint.override",
                   region: "s3.region.override"
                 },
                 scope: "catalog",
                 token: "token-local",
                 uri: "http://local_config:8181"
               }
    end
  end
end
