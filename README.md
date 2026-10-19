# VoIPSwitch Core

**A Rust IPPBX core for single-machine deployment: calls, configuration, media and AI integration.**

[English](README.md) | [简体中文](README_zh-CN.md) · [MIT License](LICENSE)

VoIPSwitch separates SIP protocol state from PBX business logic. This repository builds `voipswitchd` (core daemon) and `vs_cli` (local management client). Web management, SIP adaptation and AI processing run as separate processes on the same machine.

> Development-stage software. The SIP adapter uses a private SIP stack and is not included in this repository. Building Core alone does not provide a working SIP telephone service.

## What it does

- **Calling:** domain-local extension calls, inbound/outbound trunks, regex routing and sequential trunk hunt.
- **Configuration:** validated database writes and versioned runtime snapshots; active calls retain their resolved call configuration.
- **Media:** userspace RTP/RTCP relay, SDP rewriting, early media and optional IPv4 tc/eBPF forwarding with fallback.
- **Recording and diagnostics:** policy-based dual-channel G.711 WAV recording, per-domain CDR, SIP call traces and media statistics.
- **DTMF:** RFC4733 observation/collection and SIP INFO handling.
- **Management:** CLI commands; a separate Web service provides configuration and domain-authorized management.
- **AI integration:** a separate gateway supports post-call analysis, real-time assistance and voice agents. Providers and gateway configuration are required; Core does not call providers directly.

These are system capabilities, not features supplied by the Core binary alone. Domain isolation does not imply a complete multi-tenant operating platform.

## Architecture

![Architecture](docs/assets/architecture-overview.png)

More: [call flow](docs/assets/call-flow.png) · [config reload](docs/assets/config-reload.png) · [RTP fast path](docs/assets/rtp-fast-path.png)

- Each call leg has a session actor with bounded queues. The coordinator owns cross-leg decisions and cleanup.
- Configuration writes go through the command/configuration services. Call routing reads snapshots, not databases.
- Core owns media relay, recording and CDR. The adapter owns SIP authentication, registration, transactions and dialogs.
- AI provider execution belongs to the gateway; call actions remain subject to Core validation.

## Build and run

### Requirements

- Linux and a current stable Rust toolchain supporting edition 2024.
- Clang with the BPF target: Core's build script currently compiles an eBPF object even when forwarding will use userspace.
- The `voipswitch-ai` repository checked out next to `voipswitch-core`: `voipswitchd` currently has a local path dependency on `../voipswitch-ai/crates/ai-protocol`.

Do not assume this repository is a standalone checkout until that sibling dependency is available. No minimum supported Rust version or hardware capacity is claimed yet.

From this repository:

```bash
cargo build --workspace
cargo run -p voipswitchd
```

In another terminal, from the same repository:

```bash
cargo run -p vs_cli -- -x "show status"
cargo run -p vs_cli -- -x "vc config"
# Interactive CLI:
cargo run -p vs_cli
```

Data defaults to `data/`. Sockets default to `$XDG_RUNTIME_DIR/voipswitch/`, or `/tmp/voipswitch/` when `XDG_RUNTIME_DIR` is unset. Use each binary's `--help` for overrides.

For real calls, obtain and run the separate SIP adapter, configure a domain and two extensions, then register SIP endpoints. The optional `vs_web` service normally listens on port 18081; `vs_ai_gatewayd` provides its own management UI on 18082. Neither service is started by these commands. Change bootstrap passwords and secure management access before exposing a deployment.

## Development checks

```bash
cargo fmt --all -- --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
# Run a single test by name:
cargo test -p voipswitchd replacement_version_is_process_local_and_monotonic
```

[Contributing](CONTRIBUTING.md)

## Scope and limits

Current focus is a functional single-machine IPPBX. HA and cross-host deployment are deferred until required by an actual deployment.

Do not assume general SIP REFER transfer, ring groups, IVR, queues, voicemail, conferences, video, WebRTC, SRTP or transcoding are supported. AI-agent-to-extension transfer is not equivalent to general SIP transfer support. Advanced features require separately agreed scope and validation.

## Performance

**Benchmarks pending.** Publish concurrency, call setup latency, packet loss, CPU and memory measurements only with reproducible workloads, hardware/kernel details and userspace/fast-path/recording/AI modes stated separately. No capacity or latency figures are claimed here.

## License

This repository is licensed under the [MIT License](LICENSE). This does not grant rights to the private SIP adapter or its third-party stack. Other repositories and dependencies retain their own license terms.
