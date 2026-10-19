# 贡献说明

[English](CONTRIBUTING.md) | [简体中文](CONTRIBUTING_zh-CN.md)

报告问题时，提供提交版本、操作系统/内核、Rust 版本、复现步骤和预期/实际结果。分享日志或 SIP 报文前，删除凭据、电话号码和其他个人数据。

架构或功能变更先开 issue，说明范围、进程归属、接口和失败行为，再进入实现。内部设计文档和私有 SIP 适配器不是参与本公开仓库贡献的前提；需要时，请求公开接口约定或可复现案例。

Core 拥有呼叫/session 生命周期、路由、媒体和话单。SIP 协议状态留在适配器，AI Provider 执行留在网关。保持呼叫选路不查数据库，保留域隔离边界。

## 检查

先满足 [README_zh-CN.md](README_zh-CN.md) 中的构建条件，包括同级 `ai-protocol` 依赖和支持 BPF 的 Clang。

```bash
cargo fmt --all -- --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
# 单个测试：
cargo test -p voipswitchd replacement_version_is_process_local_and_monotonic
```

保持 PR 聚焦，说明行为变化、兼容性影响和实际执行的检查。同步维护中英文描述。没有可复现测试，不宣称性能结果。

本仓库贡献使用 [MIT 许可证](LICENSE)。
