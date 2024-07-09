defmodule ExIceberg.MixProject do
  use Mix.Project

  @version "0.1.0"
  @description "ExIceberg is an Elixir library for interacting with Apache Iceberg."

  def project do
    [
      app: :ex_iceberg,
      name: "ExIceberg",
      version: @version,
      description: @description,
      elixir: "~> 1.16",
      start_permanent: Mix.env() == :prod,
      preferred_cli_env: [
        "test.integration": :test,
        "test.tabular": :test
      ],
      deps: deps(),
      aliases: aliases(),
      package: package()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  def aliases do
    [
      "test.integration": ["test --only integration"],
      "test.tabular": ["test --only tabular"]
    ]
  end

  defp deps do
    [
      {:req, "~> 0.5"},
      {:jason, "~> 1.2"},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false},
      {:plug, "~> 1.0"}
    ]
  end

  defp package do
    [
      licenses: ["Apache-2.0"],
      links: %{
        "GitHub" => "https://github.com/ndrluis/ex_iceberg"
      }
    ]
  end
end
