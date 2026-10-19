# VoIPSwitch Core

**面向单机部署的 Rust IPPBX 核心：呼叫、配置、媒体和 AI 集成。**

[English](README.md) | [简体中文](README_zh-CN.md) · [MIT 许可证](LICENSE)

VoIPSwitch 将 SIP 协议状态与 PBX 业务逻辑分离。本仓库构建核心守护进程 `voipswitchd` 和本地管理客户端 `vs_cli`。Web 管理、SIP 适配和 AI 处理作为同机独立进程运行。

> 项目仍处于开发阶段。SIP 适配器使用私有 SIP 协议栈，不包含在本仓库中。仅构建 Core，不能获得可用的 SIP 电话服务。

## 功能

- **呼叫：** 域内分机互拨、入出局中继、正则路由、顺序中继重试。
- **配置：** 写库前校验、版本化运行时快照；活动呼叫保留创建时解析的配置。
- **媒体：** 用户态 RTP/RTCP 转发、SDP 改写、早期媒体，以及可选的 IPv4 tc/eBPF 转发与降级。
- **录音与诊断：** 策略驱动的 G.711 双声道 WAV 录音、按域话单、SIP 呼叫流和媒体统计。
- **DTMF：** RFC4733 观察/收号和 SIP INFO 处理。
- **管理：** CLI 命令；独立 Web 服务提供配置和按域授权管理。
- **AI 集成：** 独立网关支持通话后分析、实时辅助和语音机器人。需要配置网关和 Provider；Core 不直接调用 Provider。

以上为系统能力，不代表仅运行 Core 就具备全部功能。域隔离不等于完整多租户运营平台。

## 架构

![架构](docs/assets/architecture-overview.png)

更多：[呼叫流程](docs/assets/call-flow.png) · [配置热更新](docs/assets/config-reload.png) · [RTP 快速转发](docs/assets/rtp-fast-path.png)

- 每条呼叫 leg 使用独立 session actor 和有界队列；coordinator 负责跨 leg 决策与清理。
- 配置写入统一经过命令/配置服务；呼叫选路读取快照，不查数据库。
- Core 拥有媒体转发、录音和话单；适配器拥有 SIP 鉴权、注册、事务和对话。
- AI Provider 执行属于网关；呼叫动作仍由 Core 校验和执行。

## 构建与运行

### 环境要求

- Linux，以及支持 Rust edition 2024 的当前 stable 工具链。
- 支持 BPF target 的 Clang：即使运行时只用用户态转发，当前构建脚本也会编译 eBPF 对象。
- 将 `voipswitch-ai` 仓库放在 `voipswitch-core` 同级目录：`voipswitchd` 当前通过本地路径依赖 `../voipswitch-ai/crates/ai-protocol`。

取得同级依赖前，不能假定本仓库可独立构建。目前不承诺最低 Rust 版本和硬件容量。

在本仓库中运行：

```bash
cargo build --workspace
cargo run -p voipswitchd
```

另开终端，仍在本仓库中运行：

```bash
cargo run -p vs_cli -- -x "show status"
cargo run -p vs_cli -- -x "vc config"
# 交互式 CLI：
cargo run -p vs_cli
```

默认数据目录为 `data/`。Unix socket 默认位于 `$XDG_RUNTIME_DIR/voipswitch/`；未设置 `XDG_RUNTIME_DIR` 时使用 `/tmp/voipswitch/`。覆盖参数见各程序的 `--help`。

真实通话还需要取得并启动独立 SIP 适配器，配置域和两个分机，再注册 SIP 终端。可选的 `vs_web` 通常监听 18081；`vs_ai_gatewayd` 的独立管理界面监听 18082。以上命令不会启动这两个服务。对外部署前，应修改初始密码并保护管理入口。

## 开发检查

```bash
cargo fmt --all -- --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
# 按名称运行单个测试：
cargo test -p voipswitchd replacement_version_is_process_local_and_monotonic
```

[贡献说明](CONTRIBUTING_zh-CN.md)

## 范围与限制

当前重点是完善单机 IPPBX。HA 和跨主机部署暂缓，待实际项目需要时再设计。

不要假定已支持通用 SIP REFER 转接、振铃组、IVR、队列、语音信箱、会议、视频、WebRTC、SRTP 或转码。AI 机器人转分机不等于通用 SIP 转接。高级能力需要单独确认范围并验证。

## 性能

**待压测后补充。** 并发、呼叫建立延迟、丢包率、CPU 和内存数据必须附带可复现负载、硬件和内核信息，并分别说明用户态、快转、录音及 AI 模式。当前不发布容量或延迟指标。

## 许可证

本仓库使用 [MIT 许可证](LICENSE)。该授权不包含私有 SIP 适配器及其第三方协议栈；其他仓库与依赖遵循各自许可证。
