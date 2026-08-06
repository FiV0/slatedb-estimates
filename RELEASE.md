For now there is no CI release process, so push the release manually.
Check that Cargo.toml corresponds to the tag we are actually releasing.
```bash
cargo fmt --check
cargo check --all-targets
cargo test
cargo doc --no-deps
cargo publish --dry-run
cargo publish
git tag v0.1.0
git push origin main v0.1.0
```
