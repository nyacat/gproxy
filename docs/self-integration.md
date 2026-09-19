# self 功能整合记录

本轮以 `main`（`2b56d98b452a0818c235a4d21528724300ceba9a`）为基底建立新 `self`。
原 `self` 已保存为 `self_bk`（`5bd1028195a7d5e1af50db718ac76e946e65a818`）。
`main` 已同步到上游 `LeenHawk/gproxy`，本地与 `origin/main` 使用同一基底。

没有在新 `self` 上创建机械合并或 rebase 历史。以共同祖先为参照检查自有提交：主干没有改变的实现复用，交界处按主干现有接口整合，主干新增功能保留。

## 逐提交取舍

| 原提交 | 功能/问题 | 决策 | 整合依据 |
| --- | --- | --- | --- |
| `a18816f2` | 滚动未使用窗口 | 保留 | 窗口只滑动时不误判重置；保留 main 的 provider/version 语义。 |
| `83f30f64` | 响应头配额写入 | 按 main 重写 | 将新增 quota entries 与旧窗口 observations 一起交给有限队列，按凭证版本隔离。 |
| `ec5132e3` | 配额重建与清理限界 | 整合 | 保留索引、分批重建和清理；迁移编号与 main v10 合并设计。 |
| `3660884d` | 跳过无用 tokenization | 保留 | 没有限额/用量需求时跳过计数，活动写入延后。 |
| `4118e812` | PostgreSQL 池与缓存过期 | 保留 | 主干缺少连接池及过期工作限界。 |
| `b6e4cf71` | 请求分类轻量解析 | 整合 | 保留结构化 hints；沿用 main 新会话规则。 |
| `1ffe475b` | 预编译语句缓存与连接恢复 | 保留 | 限制每连接语句资源，取消事务后释放锁并回收池容量。 |
| `e8d0985d` | Redis EVALSHA | 保留 | 脚本缓存与 NOSCRIPT 恢复。 |
| `618f94ac` | 请求头会话与 tier 快路径 | 整合 | 保留 main 的 OpenCode/指纹规则并跳过不必要的 JSON 树。 |
| `307163a2` | 单次 token 计数 | 保留 | 同一请求 body 的结果在 admission/settlement 复用。 |
| `53a2c56d` | 结算任务开销 | 整合 | 保留所有权与取消保障，避免只为立即 join 多开任务。 |
| `28c8f6ea` | 幂等费用与预留对账 | 保留 | durable ledger 与原子缓存状态共同去重。 |
| `61429fd7` | 取消 admission 退款 | 保留 | 取消后仍有退款所有者；失败任务可重放。 |
| `a209861b` | PostgreSQL OAuth 计数类型 | 保留 | 按 SQL 实际类型绑定计数器。 |
| `933480cf` | Rust 基线 | 保留 | Rust 1.98.1；保留 main 的包版本 3.0.13 及新增依赖。 |
| `99314481` | self Docker/SSH 部署 | 保留 | 仅保留构建/上传工具，本次未执行部署。 |
| `f40a0671` | 原子预留与 admission 状态 | 保留 | 费用预留和退款凭据同一原子状态转移，丢响应重试不重复扣费。 |
| `225ada6d` | 同配额窗口竞争 | 保留 | Store 层按窗口串行，独立窗口可并行。 |
| `40a5f6cc` | 用量汇总投影与溢出 | 整合 | 保留窄投影和 checked 累加，并接入 main 的历史指标归一化。 |
| `c908597c` | 结算重放持久化 | 按 main 重写迁移 | 保留 replay 状态；统一版本改为 v12 并兼容旧 self v11。 |
| `343d2f69` | 流尾用量与 capture | 保留 | 错误后恢复已经收到的用量，关闭捕获时不保留原始 body。 |
| `7d0e00ca` | 后台任务跟踪与 drain | 整合 | main 服务新增行为继续保留；关闭等待有限任务和嵌套清理。 |
| `e0337090` | 失败结算重放 | 保留 | 保留阶段进度和完成凭据，重放不重复结算。 |
| `954122d7` | Edge 取消与 inline 结算 | 保留 | 取消先唤醒阻塞 pull，再等待结算；组合流也保留所有者。 |
| `e1259914` | 自更新互斥/固定路径/drain | 整合 | 保留 main Store 安装保护，用排空后重启替代固定延时退出。 |
| `97793d5f` | TCP_NODELAY | 保留 | 接受连接时设置，减少 SSE 小帧缓冲延迟。 |
| `0cf59b7a` | 导入预检 | 整合 | 完整引用/密钥/记录先验证，写后单次 reload；保留 main 凭证编辑机制。 |
| `9b712ef5` | 取消旧查询与历史过滤 | 整合 | 移植到 main QuotaSnapshot/source UI 和新查询 hook。 |
| `fbcb70e0` | 集成与性能测试工具 | 保留 | 保留可重复 runner 与比较器，CI 增加 PostgreSQL/Redis job。 |
| `1d03e280` | 旧审查/跑分结果 | 仅保留在 self_bk | 旧构建的测量不是本轮整合证据；不把历史数字当作新分支性能结果。 |
| `294310ee` | Codex terminal error | 保留 | 流式 HTTP 200 不覆盖末端 failed/incomplete 结果。 |
| `5e36c19e` | 流完成后健康状态 | 整合 | 配额响应立即观察，凭证成功需等真正流完成。 |
| `36cec1e9` | 会话亲和性与健康 | 整合 | main 会话扩展继续保留，绑定不能跳过 credential health。 |
| `746f13b5` | 部分输出与结构化失败 | 按 main 重写交界 | 协议语义与 provider 状态规则统一分类；前缀帧/用量/终态跨 wrapper 保留。 |
| `763d9cc1` | 统计限界/attempt 生命周期 | 整合 | 保留按 attempt 记录及已结算状态；统一 migration v13 并兼容 self v12。 |
| `5fa3ddc0` | 用量免重复 count/keyset | 整合 | 保留索引分页、has_more 和可选总数，并保留主干指标归一化。 |
| `9489e0a3` | 前端限界与懒详情 | 按 main 重写 | 保留快照/授权/错误后旧余额；详情、历史、估算按需加载，窗口精确匹配。 |
| `5321cd7f` | CLI 默认指纹/连接隔离 | 整合 | channel 默认值统一导出，配置/传输池按有效 fingerprint 隔离。 |
| `5bd10281` | Codex CLI 环境 UA | 保留 | 由 CLI 环境生成默认 UA，而不是宿主机器环境。 |

## 主干实现继续保留

- 单一 routing table 与 `default_route`/`executable_routes`；不恢复旧的重复 `supports` 列表。
- quota source、额度/余额快照、凭证版本检查、查询授权，以及新管理界面。
- main 的每个 provider 的 HTTP 状态策略；仅明确的协议错误语义覆盖默认策略。
- main 的旧用量指标归一化、OpenCode 会话指纹、凭证密钥保留编辑、Microsoft Store 安装管理、Windows 发布和 UPX 修复。
- 整合期间新增的三个 Microsoft Store 发布修复（`6b8e416c`、`670af1c4`、`2b56d98b`）也已纳入基底：CLI 初始化顺序、手动重试来源校验、统一 secret 名称和 artifact 读取权限。
- Groq 的旧目录属于共同祖先功能，main 已删除。self 在该目录只有通用错误分类改动，不是 self 自有新增通道，因此沿用 main 的删除。

## 交界处的根因修正

- 响应配额沿用 main 的 source/entry 数据模型，并将 self 的合并队列按凭证版本隔离。周期和快照的写事务都校验版本；轮转、删除、配置变更使用相同的凭证行锁顺序，避免旧请求覆盖新凭证数据。
- self 的轻量 probe 仍会进入 repair 并扫描历史。本轮将轻量入口与历史修复拆开，保留当前快照与周期，只有完整详情或维护路径执行重查询。
- 错误分类先识别明确的协议语义，再保留 main 的 provider 默认策略；健康状态与重试使用同一份分类。流解析契约携带“已完成帧 + 后续错误”，跨 AWS event stream、Code Assist、ClaudeWeb continuation 保留前缀和用量。
- 完整并发测试发现旧配置读取可能晚于新 mutation 发布，覆盖路由或运行时限制。快照发布使用 generation 比较；App reload 将快照、transport/tokenizer、runtime watch 与 invalidation cursor 串行更新。回归通过 gate 确定性复现旧读暂停，没有以延迟或重试掩盖竞争。
- PostgreSQL parity 测试原先把 SQLite 的 `?` 参数占位符直接发给 PostgreSQL。夹具改用统一查询构造器生成方言对应的 SQL，保留全部业务断言。
- 内存原子预留先检查结果再写入，消除极值失败后取负回滚可能溢出的路径，并与 Redis/LibSQL 的状态转移保持一致。

release profile 保留 self 的 `opt-level=3`；需要体积优先时可显式使用 `release-size`。本轮没有进行新旧构建的性能对比，不据此承诺吞吐或延迟提升。

## 数据库升级

统一序列保留 main 的 `QuotaSnapshots=10`，追加 `QuotaRebuildIndex=11`、`SettlementRecovery=12`、`QuotaActivityLifecycle=13`。
旧 self 曾把这些功能放在 v8/v10/v11/v12；迁移通过实际 schema 对象补足缺失步骤，不能仅按数字假设对象已存在。
已有真实 parent/state 和 replay 数据需要保留；回填根据持久化用量事实幂等执行。

跨 schema 回退需要恢复对应数据库备份，不能仅换回旧二进制。此次没有操作生产数据库。

## 验证

本轮使用 Rust 1.98.1 与 Node.js 24.21.0。结果以本次工作树实际执行为准，不沿用 `self_bk:review-results/*` 的历史跑分。

| 检查 | 结果 |
| --- | --- |
| `cargo fmt --all -- --check` | 通过 |
| `cargo test --workspace --no-fail-fast` | 746 passed，0 failed，23 ignored |
| console `pnpm lint` | TypeScript、ESLint、i18n parity/unused 全部通过 |
| console `pnpm test` | 39 个 Vitest 文件、94 个测试通过；4 个模型目录脚本测试通过 |
| console `pnpm build` | 通过，embed 静态资源已同步 |
| docs `pnpm check` 与 `pnpm build` | 通过，57 个页面构建成功 |
| `cargo clippy --workspace --all-targets -- -D warnings` | 通过 |
| `cargo check --workspace --target wasm32-unknown-unknown` | 通过 |
| `python3 scripts/test-postgres-redis.py` | 19/19 通过（PostgreSQL 18.6、Redis 7.4.11，临时 loopback 容器） |
| `python3 scripts/perf/test_compare.py` | 12 个测试通过 |
| `bash -n deploy/self/build.sh deploy/self/upload.sh` | 通过 |

未连接生产数据库、MySQL、Upstash 或真实 Edge runtime；没有执行部署，也没有把历史跑分当作本轮性能结论。

---

# 2026-09-20：基底推进到 v3.0.17，历史按功能点重建

以上记录的是上一轮整合（基底 `2b56d98b`）的逐提交取舍，结论继续有效。本轮没有重做那些取舍，只做两件事：把基底推进到最新上游，并把上一轮糊成一个 401 文件压扁提交的自有功能拆成可 review 的功能点。

| 项 | 值 |
| --- | --- |
| 新基底 | `c29e5fdf`（上游 `LeenHawk/gproxy` v3.0.17） |
| 旧基底 | `2b56d98b`（v3.0.13），相距 17 个上游提交 |
| 重建前的 `self` | 备份为本地分支 `self_bk_20260920`（`faba3ed2`），按决定不推 origin |
| 功能边界参照 | tag `self_bk_prev`（`5bd10281`）固定上一轮那 40 个原始功能点提交，它们原先只靠 reflog 存活 |
| 重建后的 `self` | 25 个提交，base 在 `c29e5fdf` |

## 做法：先 rebase 定内容，再拆分定形状

两步分开，各有独立判据：

1. `git rebase --onto c29e5fdf 2b56d98b` 产出中间分支 `self-rebased`，只解决自有功能与上游新代码的冲突，并在此跑完整套验证。这是**内容的唯一真相**。
2. 在 `c29e5fdf` 上重新起 `self`，逐个功能点 `git checkout <阶段提交> -- <路径清单>` 落盘提交。拆分阶段不再碰上游冲突，只决定历史形状。

因此有一条可机械验证的不变量，贯穿始终：

```
git diff self-rebased self    # 空 —— 拆分没有增、删、改任何一行
```

重建分 7 个阶段，每个阶段对应 rebase 后的一个提交；阶段收尾都核对过 `git diff <该阶段提交> HEAD` 为空，最后核对 tip 等于 `self-rebased`。

文件归属是**整文件粒度**，不按 hunk 切：同一个文件被拆到两个提交里，会让两边都处于内部不自洽的状态（签名在一个提交、调用方在另一个）。判据是该文件被哪一组原始提交改得最多；只有两条例外需要人工定夺 —— `Cargo.toml`/`Cargo.lock` 必须与依赖变更同时落地，统一并入工具链提交；测试与实现文件跟随各自的功能点，而不是集中到测试工具提交。

## 上游 17 个提交的处理

`47d40363`..`c29e5fdf` 共 17 个提交、42 个文件，其中与 self 改动有交集的只有 16 个文件，所以本轮用 rebase 即可，不需要像上一轮那样从零重写。

上游新增功能一律保留，交界处按上游现有接口整合：

- `447b3e3a` 用量记录的确认式批量删除 —— 与 self 的 keyset 分页和统计限界同时保留；`usage-table.tsx` 是本轮唯一的 rebase 冲突，解法是保留上游的批量删除，套在 self 的内外层组件拆分与分页之上。
- `380377c7` provider 维度的 catalogue 路由 —— 保留，self 的健康分类接在其后。
- `12c7aeb1` Claude prefill 保留尾部 system 消息、`77d2353d` 保留可见 reasoning summary、`97987c3b` 接受 ToolSearch 引用 —— 全部保留。
- `a49abfc1` about 页与赞助链接、`be3cdccb` 审计 TPS 下的 token 用量、`47d40363` Sponsors 链接 —— 全部保留。
- 5 个依赖 bump 与 `59343b27` 的 workspace pin 对齐 —— 采用上游版本；self 侧只保留 Rust 1.98.1 基线和 release profile 的 `opt-level=3`。

## 拆分结果

| # | 提交 | 文件 | 单独 `cargo check` |
| --- | --- | --- | --- |
| 1 | `build(self)`: Rust 1.98.1 基线、self 部署工具与 CI | 31 | 通过 |
| 2 | `perf(store)`: PostgreSQL 连接池、语句缓存、Redis EVALSHA | 9 | 不通过 |
| 3 | `feat(quota)`: 周期重建限界与竞争窗口串行写 | 33 | 不通过 |
| 4 | `feat(admission)`: 原子预留费用与幂等重放结算 | 58 | 不通过 |
| 5 | `feat(routing)`: 轻量请求分类与 CLI 指纹隔离 | 45 | 不通过 |
| 6 | `fix(stream)`: 保留部分输出、流尾与结构化失败 | 77 | 不通过 |
| 7 | `fix(host)`: 后台任务跟踪与自更新重启前 drain | 18 | 不通过 |
| 8 | `fix(import)`: 批量写入前校验引用与记录 | 13 | 不通过 |
| 9 | `perf(usage)`: 统计读限界与按时间索引分页 | 38 | 不通过 |
| 10 | `feat(quota)`: 重建配额快照与迁移 branch history | 16 | 通过 |
| 11 | `perf(console)`: 渲染限界、取消过期查询、按需加载配额详情 | 63 | 通过 |
| 12 | `fix(stream)`: envelope 失败时保留 channel 流尾 | 34 | 通过 |
| 13 | `fix(credential)`: 刷新租约竞争时读权威行 | 3 | 不通过 |
| 14 | `fix(quota)`: 硬化配额状态转移与内存费用预留 | 47 | 通过 |
| 15 | `fix(console)`: 收尾用量与凭证界面；固定部署镜像基底 | 22 | 通过 |
| 16 | `build(self)`: 按 commit 打镜像 tag 并更新 latest | 1 | 通过 |
| 17 | `feat(health)`: 按模型探测并快照凭证健康 | 26 | 不通过 |
| 18 | `feat(admin)`: 按模型重置降级的凭证健康 | 12 | 不通过 |
| 19 | `fix(core)`: websocket 与 refusal 路径尊重按模型健康 | 48 | 通过 |
| 20 | `feat(channel-api)`: 凭证刷新契约与各 channel 适配 | 73 | 不通过 |
| 21 | `feat(admin)`: 暴露手动凭证刷新与 quota probe 来源 | 21 | 通过 |
| 22 | `feat(console)`: provider 界面的手动凭证刷新 | 16 | 通过 |
| 23 | `fix(core)`: 重试流起始处抛出的上游容量错误 | 12 | 不通过 |
| 24 | `fix(health)`: 凭证健康恢复需要真实成功 | 12 | 通过 |
| 25 | `feat(core)`: 流起始检查的内存预算 | 16 | 通过 |

上一轮的 `1d03e280`（旧审查/跑分结果）按既定决策不带入，仍只留在备份分支。

## 单独编译情况（如实记录）

25 个提交里 **12 个 `cargo check --workspace` 单独通过，13 个不通过**（上表最后一列）。tip 全绿，见下节。

不通过的根因不是归属算错，而是**上一轮整合时这些功能就是一起重写的**，跨 crate 的契约变更没有中间态：

- **trait 声明与实现分居两个提交**：#2 `CacheBackend::seed_counter`、#13 `SnapshotControl::credential_for_load`、#17/#18 `gproxy_core::host::CredentialHealthLease`、#20 `lease_refresh` 参数个数、#23 `health::record_success`。声明在 `gproxy-core`，实现在 `gproxy-app`/`gproxy-store`/`gproxy-channels`，属于不同功能点。
- **模块文件与 `mod` 声明分居**：#3、#5、#9 的 `file not found for module branch_history` / `failure`。一个 hunk 往往一次声明多个模块，声明无法单独搬走。
- **结构体字段与其初始化点分居**：#6、#7、#8 的 `missing field activity in initializer of FunnelCtx`。
- **跨功能点的 import**：#4 的 `unresolved import crate::funnel::inline`。

具体有两个不可约的依赖环横跨其中 6 个功能点：

1. `gproxy-store`（quota/usage/settlement）↔ `gproxy-app` 的 snapshot/host 插件 ↔ `gproxy-core` 的 host trait；
2. `gproxy-channel-api` 的流解码契约 ↔ `gproxy-channels` 11 个 channel 的实现 ↔ `gproxy-transform` 的 envelope ↔ `gproxy-core` 的 funnel。

不存在一种文件归属或提交顺序能让它们各自独立编译。可以靠把相关 crate 整个合进同一个提交换取逐提交可编译，但那样会把 quota、admission、stream 三个功能点糊回一起 —— 本轮明确选择**保功能点边界，放宽逐提交编译**。因此 bisect 应以 tip 与上表中可编译的提交为落点，不要假设任意中间提交可构建。

## 验证

本轮使用 Rust 1.98.1 与 Node.js 24.21.0，全部在重建后的 tip（`f87c32c9`）上实际执行。

| 检查 | 结果 |
| --- | --- |
| `git diff self-rebased self` | 空 —— 拆分与 rebase 内容逐字节一致 |
| `cargo fmt --all -- --check` | 通过 |
| `cargo clippy --workspace --all-targets -- -D warnings` | 通过 |
| `cargo test --workspace --no-fail-fast` | 941 passed，0 failed，28 ignored |
| `cargo check --workspace --target wasm32-unknown-unknown` | 通过 |
| console `pnpm lint` | TypeScript、ESLint、i18n parity/unused 全部通过 |
| console `pnpm test` | 42 个 Vitest 文件、130 个测试通过；4 个模型目录脚本测试通过 |
| console `pnpm build` | 通过 |
| docs `pnpm check` 与 `pnpm build` | 通过，57 个页面构建成功 |
| `python3 scripts/test-postgres-redis.py` | 23/23 通过（临时 loopback 容器） |
| `python3 scripts/perf/test_compare.py` | 12 个测试通过 |
| `bash -n deploy/self/build.sh deploy/self/upload.sh` | 通过 |

测试总数由上一轮的 746 升到 941，其中既有上游 v3.0.14–v3.0.17 新增的测试，也有本轮拆分过程中一并带入的 self 自有测试；重点是 0 failed。

未连接生产数据库、MySQL、Upstash 或真实 Edge runtime；没有执行部署，也没有把上一轮的跑分当作本轮性能结论。
