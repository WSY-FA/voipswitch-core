# Contributing

[English](CONTRIBUTING.md) | [简体中文](CONTRIBUTING_zh-CN.md)

For bugs, include the commit, OS/kernel, Rust version, reproduction steps and expected/actual behavior. Remove credentials, telephone numbers and other personal data from logs or SIP traces before sharing them.

For architectural or feature changes, open an issue describing scope, process ownership, interfaces and failure behavior before implementation. Internal design documents and the private SIP adapter are not prerequisites for contributing to this public repository; request a public contract or reproduction when needed.

Core owns call/session lifecycle, routing, media and CDR. SIP protocol state stays in the adapter. AI provider execution stays in the gateway. Keep runtime routing independent of database reads and preserve domain boundaries.

## Checks

Follow the build requirements in [README.md](README.md), including the sibling `ai-protocol` dependency and BPF-capable Clang.

```bash
cargo fmt --all -- --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
# One test:
cargo test -p voipswitchd replacement_version_is_process_local_and_monotonic
```

Keep a pull request focused. Describe behavior changes, compatibility implications and checks actually run. Keep English/Chinese descriptions aligned. Never claim benchmark results without a reproducible test.

Contributions to this repository are accepted under its [MIT License](LICENSE).
