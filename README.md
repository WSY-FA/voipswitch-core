# VoIPSwitch Core

VoIPSwitch 的 Rust 核心服务，提供配置管理、命令处理和基础呼叫运行时。

## Workspace

- `voipswitch-core`：共享领域类型、命令模型和 IPC 基础能力。
- `voipswitchd`：核心守护进程。
- `vs_cli`：本地管理命令行客户端。

SIP 协议运行时通过独立适配器接入，Web 管理界面和 AI Gateway 也由独立进程提供。

## 构建

需要 Rust stable 工具链和 Linux 开发环境。

```bash
cargo build --workspace
```

## 运行

启动核心服务：

```bash
cargo run -p voipswitchd
```

默认运行数据写入 `data/`，Unix socket 位于
`$XDG_RUNTIME_DIR/voipswitch/`；未设置 `XDG_RUNTIME_DIR` 时使用 `/tmp/voipswitch/`。

另一个终端中启动交互式 CLI：

```bash
cargo run -p vs_cli
```

也可以执行单条命令：

```bash
cargo run -p vs_cli -- -x "show status"
```

使用 `--help` 查看完整运行参数。

## 验证

```bash
cargo fmt --all -- --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```
