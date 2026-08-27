# DQL Engine Replay Design

## Scope

本设计覆盖 Engine 阶段 E02-E10，目标是让 TM 已创建的 DQL recovery batch 在运行中任务和暂停任务上都能从源节点边界按服务端固化顺序逐条回放。Web 页面不在本设计范围内；TM API、消息和回调契约沿用 `doc/TAP-12615-DLQ-controlled-reprocessing-detailed-design.md` 与已完成的 D01-D10。

## E02 event model

`iengine-common` 新增 `com.tapdata.entity.TapdataDqlRecoveryEvent`。事件复用 `TapdataEvent` 的 `tapEvent` 和 `eventId`：`eventId` 承载 TM DQL 主记录 ID，避免在子类重新声明同名字段破坏父类复制语义。事件自身保存 `batchId`、`attemptId`、`recoveryType`、`operatorId` 和 `taskVersion`。

DATA 事件由 `DqlPayloadSerializer` 从 `DqlPayloadSnapshot` 白名单反序列化，只接受完整的 I/U/D `TapRecordEvent`。构造时在原始 `TapRecordEvent.info` 中写入 `DQL_RECOVERY=true`、`DQL_EVENT_ID`、`DQL_BATCH_ID` 和 `DQL_ATTEMPT_ID`，并原样保留 table、before、after、time、referenceTime、info 与 `exactlyOnceId`。BEGIN/END 事件不携带 DML。

事件实现覆盖 DATA 判定、并发写入判定和 clone 复制，保证经过 Jet/Hazelcast 复制后 recovery 元数据和原始 DML 仍完整。

## E03-E10 execution boundary

协调器以明确的 `DqlRecoveryEventSource`、`DqlRecoveryEventSink`、`DqlRecoveryBarrier` 和 `DqlRecoveryReportClient` 接口隔离存储读取、源边界注入、屏障等待和 TM 回调。协调器只允许一个批次运行，严格遍历 `orderedEventIds`；每条事件执行 EVENT_STARTED → DATA 注入 → 屏障 → EVENT_RESULT，单条失败按配置继续或终止。

运行中任务使用 source read gate 暂停普通读取并在 finally 恢复；暂停任务使用不启动普通 source reader 的 recovery-only runner。所有 recovery event 都携带 DQL 标记，捕获切面遇到该标记时只更新原事件并追加 attempt，不创建新的 DQL 主记录。初始化失败、任务停止、版本不一致、Engine 重启、屏障超时和 gate 恢复失败均进入批次失败补偿与安全资源释放路径。

## Verification

每个步骤遵循 TDD：先添加一个能证明缺口的测试并观察失败，再实现最小行为、运行定向测试和模块编译，最后运行 `git diff --check`。每步同步 `doc/TAP-12615-DQL-development-progress/README.md`、开发计划和步骤总结，并以 `feat(TAP-12615): ...` 创建本地 commit。
