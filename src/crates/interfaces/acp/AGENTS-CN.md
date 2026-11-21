**中文** | [English](AGENTS.md)

# ACP 协议入口指南

适用范围：`src/crates/interfaces/acp`。

`openbitfun-acp` 负责基于已组装产品 runtime 的 Agent Client Protocol 入口与 ACP client 行为。ACP protocol / client 细节留在这里或应用入口 adapter 中；跨层只共享稳定 capability facts。

本 crate 暴露两个可加性的角色 feature。`client` 负责 ACP 进程发现、配置、远程探测、会话传输和
tool-card 投影，选择 Core Agent Runtime 与具体 SSH 能力；`server` 负责 CLI 托管的 ACP 服务端，
通过 `DeliveryProfile::Acp` 投影 runtime，并精确选择该路径使用的工具、文档、订阅与外部来源 owner，
但不选择 SSH。兼容默认值必须精确等于 `client + server`；Desktop 只选择 `client`，CLI 选择两者。
两个角色保持加法语义，不得用 `product-full` 替代任一闭包。

## 护栏

- Remote ACP workspace 复用本地 ACP client 配置。修改 ACP client 行为时，必须保持 manager、remote shell probing、remote capability store 和 workspace menu availability 语义。
- ACP config persistence、remote probing、timeout policy 和 workspace surface selection 属于 ACP / app-surface 行为，不要移动到 `core-types`、`runtime-ports` 或 `agent-tools`。
- ACP external-agent tool 的命名、schema、validation、presentation 和 result shape 属于 `openbitfun-agent-tools` 的 portable contract；ACP 应调用这些 helper，不要在本层重复定义。
- ACP 标准输入输出、连接管理和协议通知投影留在本 crate。共享运行时事实可以经过 SDK 边界；ACP 协议请求、客户端选择和生命周期状态不得进入 SDK。
- 如果未来需要 contract，只表达观测事实：environment identity、capability facts、request / response DTO。

## 验证

```bash
cargo check -p openbitfun-acp --no-default-features --features client
cargo check -p openbitfun-acp --no-default-features --features server
cargo test -p openbitfun-acp
cargo test -p openbitfun-acp --no-default-features --features client,openbitfun-core/git --lib client::execution::tests
```

执行状态测试覆盖请求准入回执、按 Turn 绑定取消和实时事件与重放游标的一致性。
ACP owner 先记录执行事件，再由宿主发布；执行快照查询不获取长请求持有的协议会话锁。
Desktop 注入既有的会话事件日志，通过可选的 `executionProjection: "acp"` 恢复字段标明权威来源。

元数据并发验证：
`cargo test -p openbitfun-acp --no-default-features --features client,openbitfun-core/git --lib client::manager::tests::metadata_`。
协议接收器仍串行持有；按会话保存的 watch 快照支持长请求或变更期间的读取。
首次读取等待快照发布或初始化锁释放；模型、配置变更和流内元数据更新后发布新快照。
空闲读取仍排空待处理通知；重建连接不得复用旧快照，不能增加第二个协议流读取者来解决阻塞。

普通 ACP 审批保留 `backend-event-acppermissionrequest` 与
`submit_acp_permission_response`，兼容已有客户端及 Peer 控制端。
Native 审批只通过 runtime mailbox，旧响应命令不得转发到 native driver。
`client::manager::tests::legacy_permission_payloads` 覆盖旧请求与响应往返。

## Native agent drivers

Claude Code、Codex、Pi 驱动及共享外部 CLI 工具归
[`openbitfun-native-agents`](../native-agents/AGENTS.md)。`client::native`
继续重新导出该 crate；ACP manager 保留会话准备，运行与队列通过 external-session port
连接 scheduler。持久化标识和 session options 的 wire shape 保持兼容。
Session options 验证：`cargo test -p openbitfun-native-agents --lib session_options::tests`。
