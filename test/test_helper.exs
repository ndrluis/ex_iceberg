ExUnit.start(exclude: [:integration])
Mox.defmock(ExIceberg.MockHTTPClient, for: ExIceberg.HTTPClient)
Application.ensure_all_started(:mox)
