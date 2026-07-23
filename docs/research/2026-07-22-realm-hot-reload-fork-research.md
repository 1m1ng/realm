# Realm 动态配置与 Tunnel2SS 热更新研究

> 日期：2026-07-22  
> 状态：研究结论，供后续 `$my-brainstorm` 使用  
> 范围：Tunnel2SS 当前 Realm 配置发布路径、Realm 上游能力、PR #160 的可复用价值，以及自行维护 fork 前需要明确的设计边界  
> 不包含：最终架构决策、实施拆分、工期估算

## 1. 研究背景

Tunnel2SS 当前把一个节点上的多条 Realm 转发 endpoint 聚合到同一个 Realm 配置和进程中。修改任意一条中转规则后，对应 peer 或 relay 节点会重新生成配置并重启整个 Realm 服务。

这会产生两个用户可见影响：

1. 未被修改的 endpoint 也会短暂停止接受新连接。
2. 该 Realm 进程承载的现有连接可能一起断开。

本次研究需要回答：

1. 官方 Realm 是否已经支持安全的配置热重载。
2. Realm PR #160 是否可以直接用于 Tunnel2SS。
3. 如果自行维护 fork，应保留哪些思路、避开哪些实现，并为后续 brainstorm 提供哪些决策输入。

## 2. 执行摘要

结论如下：

1. Tunnel2SS 当前发生整组重启是现有契约的必然结果，不是偶发现象。
2. Tunnel2SS 固定使用 Realm `v2.9.3`；截至本次研究，官方最新 release `v2.9.4` 仍没有 signal reload、配置 watcher、`ExecReload` 或动态 endpoint 管理 API。
3. PR #160 证明了“单个 Realm 进程内分别管理 endpoint task”的产品方向有价值，但不能直接合并或 cherry-pick 到生产 fork。
4. PR #160 最严重的问题是直接 `abort()` endpoint listener，而 Realm core 的连接子任务通过裸指针式 `Ref<T>` 借用 listener task 栈上的状态。这会破坏 `Ref<T>` 的生命周期约束，活跃 TCP 连接和 UDP association 存在悬空指针及未定义行为风险。
5. 正确方向不是在现有 `run_tcp` / `run_udp` 外简单包一层 HTTP CRUD，而是先重构 Realm core，使 endpoint、listener、TCP connection、UDP association 的所有权和生命周期可被安全管理，再增加本地控制面。
6. Tunnel2SS 更适合提交“带稳定 ID 和 generation 的完整期望状态”，由 Realm 原子 reconcile 差异，而不是调用随机 UUID 的逐条 CRUD API。
7. TCP 可以实现“已有连接继续使用旧配置，新连接使用新配置”；同端口替换若不做 FD handoff 或可靠的 socket reuse，仍会有很短的新连接接受空窗。
8. UDP 不是无连接 TCP，必须单独决定 association 的保留、迁移、超时和回包行为，不能笼统承诺无损热更新。

因此，自行维护 fork 是可行方向，但应从官方当前基线开始，移植 PR #160 的需求和少量控制面思路，而不是以它的实现作为代码基础。

## 3. Tunnel2SS 当前行为的证据链

### 3.1 版本固定

`backend/pkg/managedresources/specs.go:35` 将 Realm 固定为：

```text
zhboner/realm v2.9.3
```

这意味着节点并不会自动获得上游后续能力；采用 fork 后还需要同步修改 managed resource 的来源、版本和完整性信息。

### 3.2 systemd 服务只有启动，没有 reload

`backend/agent/internal/bootstrap/realm.go:31` 生成 Realm systemd unit。当前 unit 只有 `ExecStart`，没有 `ExecReload`，也没有为动态控制面声明 socket、认证信息或 readiness 探针。

即使将来 Realm 支持 reload，Tunnel2SS agent 也必须显式增加对应调用路径。

### 3.3 Realm 配置变更被归入重启组

`backend/agent/internal/configstore/types.go:184` 将以下配置种类映射到 `RestartGroupRealm`：

- `KindRealmTOML`
- `KindRelayRealmTOML`
- `KindExitRealmTOML`

`backend/agent/internal/configpoll/poller.go:565` 对 Realm restart group 执行 `Reloader.Restart`。现有 agent 没有 Realm 专用的 hot-reload executor。

### 3.4 多个 endpoint 被聚合到同一进程

以下 renderer 会把节点上的多条规则合并成一个 Realm 配置：

- `backend/backend/internal/config/files/realm_peer.go:52`
- `backend/backend/internal/config/files/realm_relay.go:33`

因此当前重启粒度天然是“节点上的 Realm 进程”，不是“单条转发规则”。

### 3.5 规则变更会扇出到受影响节点

`backend/backend/internal/config/affected_nodes.go:511` 会把转发规则关联 preset 所绑定的节点加入配置失效集合。后端重新生成节点快照后，agent 观察到 Realm 配置变化并重启对应 service。

完整因果链为：

```text
转发规则变更
  -> 计算受影响节点
  -> 重新生成聚合 Realm 配置
  -> agent 检测到 Realm restart group 变化
  -> systemctl restart realm
  -> 该进程内全部 endpoint 同时受影响
```

### 3.6 当前行为已由测试确认

本次研究执行了：

```bash
go test ./agent/internal/configpoll \
  -run 'TestRestartGroupMonotonicity_PBT_097|TestAccelPPPGroupHotReloadsNotRestart' \
  -count=1 -v
```

测试通过。它确认 Realm 使用 restart group，而项目中已经存在的热更新例外仅适用于其他明确支持 reload 的组件。

## 4. Realm 上游现状

### 4.1 官方版本没有动态重载入口

本次检查了 Tunnel2SS 使用的 `v2.9.3` 和官方 `v2.9.4` 源码，未发现：

- SIGHUP 或其他 reload signal handler
- 配置文件 watcher
- 动态 endpoint CRUD/reconcile API
- 可由 systemd `ExecReload` 调用的命令
- listener 生命周期管理器

Realm 当前的配置模型仍是进程启动时读取配置并启动 endpoint event loop。

### 4.2 上游方向尚未落地

相关上游 issue 仍未形成可供 Tunnel2SS 使用的稳定接口。维护者在 PR #160 的后期讨论中明确表示，倾向于先重构 Realm，暴露必要且低耦合的内部接口，再集成 Web API 或外部应用。

这与本次代码审查结论一致：控制面不是主要难点，核心难点是 listener 和活动流量的所有权、状态机及切换语义。

参考：

- PR #160：https://github.com/zhboner/realm/pull/160
- 维护者关于先重构 core interface 的评论：https://github.com/zhboner/realm/pull/160#issuecomment-3700720776
- 官方 `v2.9.4` release：https://github.com/zhboner/realm/releases/tag/v2.9.4

## 5. PR #160 概况

PR 信息：

- 标题：`add http api support for instance management and update documentation`
- 最终 head：`e9b5c05bea48e6c960517e7b07f16d5155c6b1aa`
- 规模：12 files，`+1813/-6`
- 主要新增：`src/api.rs` 696 行，`readme.api.md` 709 行
- 状态：2026-01-26 closed，未 merge
- 当前 fork 仓库已不可访问，但 GitHub PR head ref 仍可读取
- PR 当前没有可读取的 check runs/statuses
- 新增 API 代码没有对应单元测试或集成测试

PR 提供以下能力：

- 在一个 Realm 进程内创建、查询、更新、启动、停止、重启和删除 instance
- 每个 instance 保存 TCP/UDP `JoinHandle`
- 可选 API key
- instance JSON 持久化和 auto-start
- 继续复用原有 `run_tcp` / `run_udp`

这些能力覆盖了管理界面的表面需求，但没有闭合底层生命周期和一致性问题。

## 6. PR #160 详细审查发现

### 6.1 P0：`abort()` 破坏 Realm core 的内存生命周期约束

PR 更新 instance 时执行：

```rust
if let Some(tcp_handle) = data.tcp_handle.take() {
    tcp_handle.abort();
}
if let Some(udp_handle) = data.udp_handle.take() {
    udp_handle.abort();
}
```

来源：

https://github.com/zhboner/realm/blob/e9b5c05bea48e6c960517e7b07f16d5155c6b1aa/src/api.rs#L306-L322

Realm core 的 `Ref<T>` 实际是 `*const T` 包装器，并通过 unsafe 手工实现 `Send` 和 `Sync`。其安全注释明确要求 pointee 在 event loop 期间始终有效：

https://github.com/zhboner/realm/blob/e9b5c05bea48e6c960517e7b07f16d5155c6b1aa/realm_core/src/trick.rs#L5-L15

TCP listener task 在自身栈上创建 `raddr`、`conn_opts` 和 `extra_raddrs`，然后把它们包装成 `Ref<T>` 交给 detached connection tasks：

https://github.com/zhboner/realm/blob/e9b5c05bea48e6c960517e7b07f16d5155c6b1aa/realm_core/src/tcp/mod.rs#L33-L66

`abort()` listener task 会 drop listener future 及其栈上状态，但已经由 `tokio::spawn` 创建的 connection task 不会自动随父 task 结束。connection task 随后仍可能解引用已经失效的 `Ref<T>`。

UDP 同样把 listener socket、remote address、connection options 和 `SockMap` 以 `Ref<T>` 交给 detached `send_back` tasks：

- https://github.com/zhboner/realm/blob/e9b5c05bea48e6c960517e7b07f16d5155c6b1aa/realm_core/src/udp/mod.rs#L26-L35
- https://github.com/zhboner/realm/blob/e9b5c05bea48e6c960517e7b07f16d5155c6b1aa/realm_core/src/udp/middle.rs#L123-L133

影响：

- 不能声称现有 TCP 连接会被安全保留。
- UDP association 也不能被安全排空。
- stop、delete、restart、update 都可能触发同类问题。
- 这不是普通业务错误，而是 Rust unsafe 前提被破坏后的未定义行为风险。

这是 PR 不可直接采用的首要原因。

### 6.2 P0：启动结果是假同步，API 可能返回虚假的 `Running`

`start_realm_endpoint()` 的返回类型是 `std::io::Result`，但它只创建后台 task，然后无条件返回 `Ok`：

https://github.com/zhboner/realm/blob/e9b5c05bea48e6c960517e7b07f16d5155c6b1aa/src/api.rs#L532-L561

真正的 TCP/UDP bind 在 `run_tcp` / `run_udp` 的后台 task 中发生，而且 bind 失败使用 panic：

- TCP：https://github.com/zhboner/realm/blob/e9b5c05bea48e6c960517e7b07f16d5155c6b1aa/realm_core/src/tcp/mod.rs#L37
- UDP：https://github.com/zhboner/realm/blob/e9b5c05bea48e6c960517e7b07f16d5155c6b1aa/realm_core/src/udp/mod.rs#L28

可能出现：

1. API 返回成功并记录 `Running`。
2. 后台 task 随后因端口占用、地址错误或权限问题退出。
3. 状态表没有监视 `JoinHandle` 的完成结果，instance 长期显示为 `Running`，但没有 listener。

### 6.3 P0：更新过程不是事务性的

当前顺序为：

```text
移除旧 instance
  -> abort 旧 listener
  -> build 新配置
  -> spawn 新 listener
  -> 立即记录 Running 并返回成功
```

问题包括：

- 先终止已工作的旧 endpoint，再验证和启动新 endpoint。
- 没有等待旧 task 完成，旧 socket 是否已经释放不确定。
- 新 endpoint bind 失败时无法自动恢复旧 endpoint。
- 同一 listen address 的更新存在明显竞态。
- API acknowledgment 不代表配置已经生效。

期望顺序至少应满足：验证、准备、受控停止 accept、等待 listener 释放、bind 新 listener、确认 ready、发布新状态。失败时应尽可能保留或恢复旧配置。

### 6.4 P1：API 默认暴露到所有网络接口且允许无认证运行

鉴权中间件在未配置 API key 时直接放行：

https://github.com/zhboner/realm/blob/e9b5c05bea48e6c960517e7b07f16d5155c6b1aa/src/api.rs#L27-L47

API 固定绑定 `0.0.0.0:<port>`：

https://github.com/zhboner/realm/blob/e9b5c05bea48e6c960517e7b07f16d5155c6b1aa/src/api.rs#L686-L695

任何可访问该端口的主体都可能创建任意 listener、转发到任意 remote，或删除现有 endpoint。即使设置 API key，明文 HTTP 也会暴露凭证和控制流量。

Tunnel2SS 的 agent 与 Realm 位于同一节点，不需要公网 HTTP 控制面。Unix domain socket 配合文件权限是更小的攻击面；如保留 TCP，则必须 loopback-only、fail-closed，并提供显式认证和请求大小/速率限制。

### 6.5 P1：输入错误可能 panic，而不是返回 4xx

`EndpointConf::build()` 中存在 `expect()` / `unwrap()` 地址与端口解析。API 直接对用户提交的 JSON 调用 `build()`，没有先做完整验证或把解析失败转换为结构化客户端错误。

影响：

- 无效地址可能终止 request task。
- 与无认证、全接口监听组合后，可形成低成本拒绝服务入口。
- API contract 无法稳定区分验证失败、bind 失败和内部错误。

### 6.6 P1：持久化存在乱序、竞争和事实源冲突

每次 mutation 都克隆当前 snapshot，再独立 `tokio::spawn` 一个持久化任务。所有任务都写入相同的 `<path>.tmp`，然后 rename：

https://github.com/zhboner/realm/blob/e9b5c05bea48e6c960517e7b07f16d5155c6b1aa/src/api.rs#L180-L186

创建路径示例：

https://github.com/zhboner/realm/blob/e9b5c05bea48e6c960517e7b07f16d5155c6b1aa/src/api.rs#L284-L291

并发或连续变更时可能发生：

- 较旧 snapshot 最后写入，覆盖较新状态。
- 多个 writer 争用同一个 temp file。
- API 已成功返回，但持久化稍后失败。
- 进程重启后恢复出已删除或旧版本的 endpoint。

此外，Tunnel2SS backend 已经是配置事实源。如果 Realm fork 同时重写同一配置文件，将形成双 writer，增加配置漂移和恢复歧义。

### 6.7 P1：API 模型不适配 Tunnel2SS 的期望状态发布

PR 的 `POST /instances` 使用随机 UUID：

https://github.com/zhboner/realm/blob/e9b5c05bea48e6c960517e7b07f16d5155c6b1aa/src/api.rs#L237-L253

缺少：

- 调用者指定的稳定 endpoint ID
- idempotency key
- desired generation / resource version
- compare-and-swap 或前置条件
- 批量原子 reconcile
- 当前生效 generation 的查询

Tunnel2SS agent 在超时重试后可能重复创建 instance；逐条 CRUD 更新一组规则时还会产生部分成功、部分失败的中间状态。

### 6.8 P2：状态恢复和全局配置继承存在缺陷

失败状态被保存为 `Failed(<error>)`，但 auto-start 判断使用 `persisted.status != "Failed"`。因此 `Failed(...)` 仍满足 auto-start 条件，会在重启时再次启动：

https://github.com/zhboner/realm/blob/e9b5c05bea48e6c960517e7b07f16d5155c6b1aa/src/api.rs#L610-L650

另外，create 会继承 global network defaults，但 update 不执行同样的继承逻辑。相同的 endpoint payload 在 create 和 update 时可能产生不同的最终配置。

### 6.9 测试与维护性不足

- API、认证、生命周期、持久化和并发路径没有新增自动化测试。
- PR 自报 CI 成功，但当前没有可独立读取的 check results，原 fork 也已不可访问。
- 本次环境没有安装 Rust `cargo`，因此未能独立执行 `cargo test --all-features`。
- PR 分支与官方当前 `v2.9.4` 已发生明显依赖、DNS 和 workflow 漂移，直接 cherry-pick 会把功能移植与历史分叉混在一起。

## 7. PR #160 中值得保留的部分

虽然不能复用实现，但它验证了几个有价值的需求：

1. 一个 Realm 进程可以在概念上承载多个独立可管理的 endpoint。
2. endpoint 应有显式 identity、status 和 lifecycle operation。
3. 控制面可以作为 optional feature，保持 CLI/静态配置兼容。
4. 管理 API 不应直接控制子进程，而应复用 Realm core。
5. 外部控制面需要查询实际运行状态，而不仅是期望配置。

可以把 PR 当作 API 用例和反例集合，而不是实现起点。

## 8. Fork 的候选设计边界

本节只记录应在 brainstorm 中讨论的边界，不作最终架构决定。

### 8.1 Core 先于 API

需要先让 Realm core 暴露可测试的 endpoint lifecycle abstraction，例如具备以下能力：

- validate/build endpoint，不产生副作用
- bind/start listener，并在成功后返回 ready
- stop accepting new traffic
- 查询 running / draining / failed 状态
- 等待 listener 完成释放
- 对已接受的 TCP connection 执行 drain
- 对 UDP association 执行明确的保留或终止策略

HTTP、Unix socket 或 CLI reload 都应只是这个 core interface 的 adapter。

### 8.2 所有权必须替代裸生命周期假设

候选方向：

- listener 和子任务使用 `Arc` 持有 immutable endpoint runtime config。
- 每个 TCP connection 在 accept 时 clone 当前 generation 的 `Arc`。
- listener 更新不使旧 connection 的配置失效。
- UDP association 持有自身所需 socket、remote/config 和 generation，不借用 listener task 栈。
- 任务取消必须通过 cancellation token、owned state 和 join/drain 完成，不能仅调用 `abort()` 后遗弃结果。

是否保留 `Ref<T>` 作为永不退出 event loop 的内部优化，需要在 benchmark 与安全性之间单独评估；动态生命周期路径不能继续依赖该前提。

### 8.3 推荐研究 desired-state reconcile，而非逐条 CRUD

候选请求模型：

```json
{
  "generation": 42,
  "endpoints": {
    "forwarding-rule-123:peer": { "...": "..." },
    "forwarding-rule-456:relay": { "...": "..." }
  }
}
```

Realm 返回：

- accepted / active generation
- unchanged、created、updated、draining、deleted、failed 的 endpoint 集合
- 每个失败 endpoint 的验证或 bind 错误
- 是否完成整个 generation 的提交

需要 brainstorm 的核心选择是：

1. 整个 generation 是否必须全有或全无。
2. 是否允许不相干 endpoint 成功、单个 endpoint 失败。
3. agent 重试相同 generation 时如何保证幂等。
4. 新 generation 到达时，旧的尚在 draining endpoint 如何表示。

### 8.4 TCP 更新语义候选

期望用户语义：

| 操作 | 已有 TCP 连接 | 新连接 | 其他 endpoint |
| --- | --- | --- | --- |
| 新增 endpoint | 不受影响 | 新 listener ready 后接受 | 不受影响 |
| 删除 endpoint | 继续排空或按策略超时关闭 | 不再接受 | 不受影响 |
| 修改 remote/transport | 继续使用旧 generation | 使用新 generation | 不受影响 |
| 新配置验证失败 | 保持旧连接 | 旧 listener 继续接受 | 不受影响 |
| 新 listener bind 失败 | 保持旧连接 | 应尽量恢复旧 listener | 不受影响 |

同一 listen address 的替换有三种候选策略：

1. **短接受空窗**：停止旧 accept、等待 socket drop、bind 新 listener。简单可靠，已有连接不受影响，但新连接可能在毫秒窗口失败。
2. **`SO_REUSEPORT`**：新旧 listener 短暂并存。需要验证平台语义、流量分配、transport 状态和回滚，不能仅凭 socket option 假设安全。
3. **FD handoff**：listener socket 不关闭，只替换 accept 后使用的 endpoint generation。最接近无缝，但需要把 acceptor 与 route/transport runtime 解耦，设计复杂度最高。

brainstorm 应先明确产品接受的是“已有连接无损”还是“已有连接与新连接都零中断”。两者的实现成本不同。

### 8.5 UDP 更新语义必须单独定义

需要决定：

- 现有 client association 是否继续发往旧 remote。
- 回包应该通过旧还是新 listener socket 返回。
- remote、transport 或 DNS 变化时 association 是否立即失效。
- association 的 drain timeout 和最大保留时间。
- 删除 endpoint 时是否立即停止全部 UDP 流量。
- 更新期间如何避免旧、新 association 对同一 client key 冲突。

可接受的第一阶段可能是：TCP 保证已有连接 drain，UDP 明确定义为受控重建并暴露影响，而不是声称两者都无损。

### 8.6 控制面安全候选

Tunnel2SS 场景优先考虑：

- Unix domain socket
- root/realm group 文件权限
- request size limit
- generation 和 schema version
- 本地调用超时
- readiness 与 health query
- 审计日志不包含敏感 transport 参数

只有存在跨主机直接控制 Realm 的明确需求时，才考虑 TCP HTTP、TLS 和独立凭证体系。

### 8.7 持久化所有权

候选原则：

- Tunnel2SS backend 保持唯一 desired-state 事实源。
- agent 负责把节点期望快照提交给本机 Realm。
- Realm 只保存运行状态，或保存带 generation 的 last-known-good snapshot 用于进程自恢复。
- Realm 不应异步重写 Tunnel2SS 管理的静态配置文件。
- backend、agent、Realm 三方必须能判断 active generation，避免控制面成功但数据面未生效。

## 9. Tunnel2SS 集成面

fork 就绪后，Tunnel2SS 预计涉及以下变更面；具体方案留给 brainstorm 和 plan：

1. `backend/pkg/managedresources/specs.go`
   - 将 Realm artifact 切换到受控 fork。
   - 明确版本策略、上游同步策略、SHA/签名和回滚版本。

2. `backend/agent/internal/bootstrap/realm.go`
   - 配置本地控制 socket、runtime directory、权限和 systemd lifecycle。
   - 保留首次安装及进程异常时的 restart 能力。

3. `backend/agent/internal/configstore/types.go`
   - 区分 Realm 进程级配置与动态 endpoint desired state。
   - 不能简单把所有 Realm 变更继续映射到 `RestartGroupRealm`。

4. `backend/agent/internal/configpoll/poller.go`
   - 新增 Realm reconcile executor。
   - 需要 timeout、generation、错误分类、重试、回退和 capability detection。

5. Realm peer/relay/exit renderers
   - 研究从单一静态文件转为稳定 ID 的 endpoint snapshot。
   - 保证同一业务规则在多次渲染中 ID 稳定。

6. affected-node fanout
   - 仍可保留受影响节点计算，但节点收到快照后只变更实际 diff 的 endpoint。

7. 兼容与发布
   - agent 必须识别旧 Realm 和 fork Realm 的 capability。
   - API 不可用或 reconcile 失败时，是否回退到整进程 restart 需要显式策略。
   - rollout 应支持单节点 canary 和快速切回官方 Realm。

## 10. 建议的验收语义

以下内容适合作为 brainstorm 的初始验收基线：

1. 修改 endpoint A 不会重启 Realm 进程，也不会停止 endpoint B/C 接受连接。
2. 修改 endpoint A 时，A 的既有 TCP 会话不中断，并继续使用旧 generation 直至自然结束或达到 drain timeout。
3. 新连接只在新 endpoint bind 并 ready 后被视为切换成功。
4. 新配置无效或 bind 失败时，API 不得返回 `Running`，旧 endpoint 应继续服务或被可靠恢复。
5. 重复提交同一 generation 不会创建重复 endpoint。
6. 乱序提交旧 generation 不会覆盖新 generation。
7. agent 可以查询 Realm 当前 active generation 和逐 endpoint 实际状态。
8. 控制接口默认不暴露到公网。
9. Realm 或 agent 重启后能恢复到 backend 认可的 desired generation。
10. UDP 的更新影响被明确记录、测试和展示，不能隐式套用 TCP 承诺。

## 11. Fork 前建议先做的验证实验

这些实验用于降低 brainstorm 中的未知量，不代表最终实现顺序：

1. **TCP ownership prototype**
   - 将 connection task 所需状态替换为 owned `Arc`。
   - 停止 listener 后验证长连接持续传输。

2. **同端口替换实验**
   - 测量 stop-join-bind 的新连接失败窗口。
   - 分别测试 Linux IPv4、IPv6、TCP、不同 bind options。

3. **FD handoff 可行性实验**
   - 让 listener 长期存在，只热替换 accept 后使用的 route generation。
   - 验证 transport/listen-side 配置变化是否允许复用 listener。

4. **UDP association prototype**
   - 明确 listener 停止后 association task 的所有权。
   - 验证旧 association drain 与新 association 创建是否冲突。

5. **失败原子性实验**
   - 端口冲突、非法地址、DNS 失败、transport 参数错误时，确认旧 endpoint 不受影响。

6. **agent 幂等实验**
   - 模拟请求成功但响应丢失、重复请求、乱序 generation、Realm 进程重启。

7. **负载与泄漏实验**
   - 高频增删改 endpoint，同时保持大量长连接和 UDP association。
   - 检查 task、FD、socket、内存和 drain queue 是否泄漏。

## 12. 后续 brainstorm 应回答的问题

### 产品语义

1. 首要目标是保护已有连接，还是要求新连接也完全零失败？
2. TCP 和 UDP 是否允许分阶段交付？
3. endpoint 更新失败时，允许部分 generation 生效还是必须整体回滚？
4. drain timeout 到期后是否强制关闭旧连接？默认值由谁控制？

### Realm fork 架构

5. acceptor 是否应与 remote/transport runtime 解耦，以支持 FD 不变的 generation 切换？
6. `Ref<T>` 是全面移除，还是只在动态 endpoint 路径替换为 owned state？
7. lifecycle manager 放在 `realm_core` 还是二进制层？对上游未来合并有什么影响？
8. 控制协议选择 Unix HTTP、Unix 自定义协议、gRPC，还是 CLI + socket？

### Tunnel2SS 集成

9. 稳定 endpoint ID 应由 forwarding rule ID、节点角色和渲染位置如何组成？
10. backend 生成 generation，还是 agent 根据 snapshot hash 生成？
11. reconcile 失败时何时允许回退整进程 restart？回退是否会违反用户的连接保护预期？
12. rollout 如何在官方 Realm 和 fork Realm 之间进行 capability negotiation？

### 维护策略

13. fork 是长期产品依赖，还是准备回馈上游的临时分支？
14. 如何持续吸收上游 release、依赖更新和安全修复？
15. fork binary 的命名、版本、release、SBOM、签名和回滚策略是什么？
16. 哪些 core refactor 应优先拆成小 PR 提交上游，以降低长期分叉成本？

## 13. 风险清单

| 风险 | 影响 | 研究阶段建议 |
| --- | --- | --- |
| unsafe `Ref<T>` 生命周期被动态取消破坏 | 未定义行为、崩溃、数据错误 | 在任何热更新实现前解决所有权 |
| 状态显示与实际 listener 不一致 | 控制面误判成功、流量中断 | ready handshake + task supervision |
| 同端口替换竞态 | 新 listener bind 失败 | 明确 stop/join/bind 或 FD handoff |
| UDP association 语义不清 | 丢包、回包异常、旧 remote 泄漏 | 单独设计和测试 UDP 状态机 |
| backend 与 Realm 双持久化 | 配置漂移、重启后回退 | 单一 desired-state 事实源 |
| 随机 ID 和非幂等 CRUD | 重试后重复 endpoint | 稳定 ID + generation reconcile |
| 控制面公网暴露 | 任意转发、删除规则、凭证泄漏 | Unix socket、最小权限、fail-closed |
| 长期 fork 漂移 | 漏掉上游修复、升级成本上升 | 小范围 core 改动、持续 rebase、争取上游接口 |
| 回退整进程 restart | 在失败路径重新引入全量断线 | 将回退策略变为显式产品决策 |

## 14. 已验证与未验证事项

### 已验证

- Tunnel2SS Realm artifact 版本固定为 `v2.9.3`。
- Realm 配置种类属于 `RestartGroupRealm`。
- configpoll 对 Realm 执行 restart，而非 reload。
- peer/relay renderer 聚合多个 endpoint。
- 官方 `v2.9.3` / `v2.9.4` 没有可直接使用的动态 reload 接口。
- PR #160 最终 head 的 API、lifecycle、持久化和鉴权实现。
- `abort()` 与 Realm core 裸指针 `Ref<T>` 生命周期约束存在直接冲突。
- PR #160 已关闭且未合并。

### 尚未验证

- PR #160 最终 head 在现代 Rust toolchain 上能否完整构建；本环境缺少 `cargo`。
- 真实负载下 stop/join/bind 的连接接受空窗长度。
- 不同 Linux kernel 与 bind option 下 `SO_REUSEPORT` 是否满足所需语义。
- Realm transport 模块是否允许长期 listener 与每连接 generation 完全解耦。
- UDP association 安全 drain 所需的最小 core 改动。
- 上游后续 refactor 的时间表和最终接口形式。

## 15. 建议的下一步输入

后续可直接使用本文件启动 brainstorm，并把目标描述为：

```text
基于 /tmp/2026-07-22-realm-hot-reload-fork-research.md，brainstorm 一个由我们长期维护的 Realm fork 方案。重点确定 TCP/UDP 更新语义、core ownership 重构、desired-state reconcile API、Tunnel2SS agent 集成、兼容回退和上游同步策略。不要直接进入实施计划，先闭合产品语义和关键架构决策。
```

