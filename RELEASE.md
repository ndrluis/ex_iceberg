# How to release

Because we use
[`RustlerPrecompiled`](https://hexdocs.pm/rustler_precompiled/RustlerPrecompiled.html), releasing
is a bit more involved than it would be otherwise.

1. Open a PR with any changes needed for the release.

- This must include at least updating the `@version` in `mix.exs` and any other files that
  reference it, like `README.md`. It must also include updating `CHANGELOG.md` to reflect the
  release.

2. Once the PR is merged, cut a GitHub release with information from the changelog and tag the
   commit with the version number (format: `vx.x.x`).
3. This will kick off the "Precompile NIFs" GitHub Action. Wait for this to complete.
4. While the NIFs are compiling, ensure you have the latest version of `main` and don't have any
   intermediate builds by running `rm -rf native/ex_iceberg/target`.
5. Once the NIFs are built, use:

        EX_ICEBERG_BUILD=true mix rustler_precompiled.download ExIceberg.Nif --all --print

   to download all the artifacts and generate the checksum file.
6. Create the `checksum-Elixir.ExIceberg.Nif.exs` file with the SHA 256 contents from the previous step.
7. Commit and push the checksum file:

        git add checksum-Elixir.ExIceberg.Nif.exs
        git commit -m "Add checksums for vx.x.x"
        git push origin main

8. Run `mix hex.publish` - please double check the dependencies and files, and confirm.
9. Bump the version in the `mix.exs` and add the `-dev` flag to it (e.g., `0.3.1-dev`).

## Additional Notes

### Supported Platforms

The precompiled NIFs are built for the following platforms:

- **macOS**: `aarch64-apple-darwin` (Apple Silicon), `x86_64-apple-darwin` (Intel)
- **Linux**: `x86_64-unknown-linux-gnu`, `x86_64-unknown-linux-musl`, `aarch64-unknown-linux-gnu`, `aarch64-unknown-linux-musl`
- **Windows**: `x86_64-pc-windows-msvc`, `x86_64-pc-windows-gnu`

### NIF Versions

We support NIF versions: `2.15`, `2.16`, `2.17` to ensure compatibility across different Erlang/OTP versions.

### Troubleshooting

If the GitHub Action fails:
1. Check the logs in the Actions tab
2. Ensure all Rust dependencies support cross-compilation
3. Verify the `Cargo.toml` has the `rustler_precompiled` feature flag

If checksums don't match:
1. Clear local cache: `rm -rf ~/.cache/rustler_precompiled`
2. Re-download: `EX_ICEBERG_BUILD=true mix rustler_precompiled.download ExIceberg.Nif --all --print`
3. Verify the release artifacts on GitHub

### Development Builds

For development or testing, you can force local compilation:

    EX_ICEBERG_BUILD=true mix deps.compile ex_iceberg

This bypasses the precompiled binary download and compiles from source.
