# TAP-12361-6：空闲 CDC 任务心跳停滞保护（首版）

## 适用场景与启用前提

针对低流量任务：源端心跳保证无 DML 时也推进位点；任务偶发停滞后，及时重启可恢复，但延迟发现可能导致源日志过期。

默认关闭。只支持显式启用的普通 SYNC / MIGRATE 任务，不自动覆盖共享 CDC、共享缓存消费、logCollector、预览或测试任务。启用前必须确认指定源→目标单元的 CDC 心跳能穿过实际处理/确认链路并推进可恢复检查点。源库有心跳表本身不构成此保证。

源路径将 `HeartbeatEvent` 包装成 `TapdataHeartbeatEvent`。目标路径经写入/并发确认处理后更新 `syncProgressMap`，再通过 `saveToSnapshot()` 写入 TM。首版的本地进展埋点放在检查点 HTTP 请求成功返回之后：源端心跳、内存 offset 更新、重复上传同一个检查点都不会单独刷新进展计时。

`offsetCallbackEnable`、并发写入和共享采集的具体 connector 语义需要分别验证，不能由普通路径的单元测试推断全部兼容。此版本不改变检查点推进或恢复读取语义。

## 启用方法

先通过正常任务操作停止目标任务，确认停止完成，再在任务 `attrs` 下设置以下配置，然后正常启动。`units` 必须填写 `attrs.syncProgress` 的完整原始 key（JSON 数组的字符串），不能用节点名称替代。

```json
{
  "heartbeatWatchdog": {
    "enabled": true,
    "units": ["[\"SOURCE_NODE_ID\",\"TARGET_NODE_ID\"]"],
    "timeoutMs": 180000,
    "graceMs": 180000
  }
}
```

配置模板另见 `docs/bash/TAP-12361-6/heartbeat-watchdog.example`。运行中修改配置不属于首版支持的热更新方式，修改后应正常停止/启动。

`timeoutMs` 必须至少 60000，`graceMs` 至少 15000；非法配置不启用检测。默认均为 180000ms。超时应大于源心跳最大正常间隔、检查点提交周期及网络/GC 余量。还必须满足：检测、扫描、停止启动、恢复读取和安全余量之和，小于源日志剩余有效窗口。首版不会自动查询 binlog/oplog 保留窗口。

## 检测与恢复

1. 引擎独立 daemon 线程每 15 秒扫描。每条指定 CDC 单元单独比较持久化 `streamOffset` 的 SHA-256 摘要，避免一个单元掩盖另一个单元。`sourceTime/eventTime` 变化不能代替恢复位点变化。首次观测仅种子初始化，每个运行实例有新宽限期。本地超时使用单调时钟。
2. 只有正常 CDC、存在 streamOffset 的进展参与检测。缺失、损坏、初始同步数据视为未知，不自动重启。源端收到心跳的时间仅用于诊断。
3. 超时后以 CAS 写入 `attrs.heartbeatRecovery`。TM 每 15 秒独立观察持久化 `syncProgress`，也只能写同一份恢复请求。CAS 同时检查 taskId、RUNNING、agentId、taskRecordId、lastStartDate、配置和旧恢复文档。
4. 引擎恢复线程使用现有任务启停锁，重新读取任务状态和停滞证据，原子认领请求。认领成功才消耗恢复额度；记录有限线程栈、源心跳时间和最后持久化时间，不记录 offset 内容。
5. 调用既有 `TaskClient.stop()`，对于尚未完成的异步取消，在恢复线程中以 250ms 间隔最多等待 60 秒。只有明确返回 true 后才清理旧客户端并通过既有 `startTask()` 启动。启动前再次检查用户状态、执行归属和恢复 CAS。普通出错重试也必须让出正在进行的 watchdog 恢复。单次 stop 调用本身若不返回，则仍由独立扫描的 120 秒截止时间阻止迟到启动。
6. 重启后进入 VERIFYING；需要重启后的成功持久化信号，且所有涉事单元的摘要都相对事件基线变化，才标记 RECOVERED。仅进程心跳、启动调用返回不算恢复成功。

恢复工作线程最多 2 个，排队最多 16 个，防止坏任务无限创建线程。同一任务本地仅一个在途动作，跨 TM/引擎以持久化 CAS 互斥。

## 超时、预算与安全边界

恢复请求/停止阶段超过 120 秒，独立引擎扫描或 TM 将请求标记 BLOCKED，输出需人工处理的监控日志。停止调用晚到返回时，旧 CAS 不能再授权启动。不会使用 `Thread.stop()`，也不会把中断当成停止成功。

每任务滚动 1 小时最多认领 3 次恢复，跨单元共享，短暂推进不清零。额度耗尽进入 CIRCUIT_OPEN；窗口释放后可再尝试。停止未确认的 BLOCKED 不会自动释放。

启用本能力的任务跳过原 `engineRestartNeedStartTask` 基于 pingTime 的直接重新调度，以免与原地恢复争用。云版也保持原 agent，无条件重新分配不会发生。

**引擎进程完全失联时，TM 可以检测检查点停滞并记录请求超时，但本版不在无法确认旧写入者退出的情况下换 agent 或强杀 JVM。需要运维/supervisor 确认进程退出后通过正常启停流程恢复。** 这是首版的明确边界；对要求无人值守进程失联切换的环境，应先完成外部隔离/fencing 集成再启用。

任务停止中/已停止、agent 或运行批次变化、用户重新启动都会使旧请求无法认领。显式启动取消旧事件但保留滚动预算；reset 清理健康/恢复状态并保留配置；复制及非保留状态导入会清理配置和运行状态，必须按新 DAG 重新启用。

## 可观测性

- `attrs.heartbeatHealth`：运行实例 UUID、扫描报告时间、最后成功持久化时间、源心跳时间、停滞单元。OBSERVING 只表示观测中，不承诺所有指定单元均健康。
- `attrs.heartbeatRecovery`：幂等请求 ID、来源 ENGINE/TM、状态、认领时间、尝试时间列表、涉事单元摘要。
- 恢复状态：REQUESTED → STOPPING → VERIFYING → RECOVERED / FAILED；停止超时进入 BLOCKED，额度耗尽进入 CIRCUIT_OPEN，过期证据或显式启动进入 CANCELLED。
- 引擎及 TM 输出 `TaskHeartbeat` 日志，同时写既有任务监控日志。首版未新增邮件/短信告警模板；需要将监控日志接入现有值班告警，不能依赖人工定时看页面防止日志窗口过期。

## 验证

代码入口（相对仓库根目录）：

- 检测契约、摘要与预算：`manager/tm-common/src/main/java/com/tapdata/tm/commons/task/heartbeat/HeartbeatWatchdog.java:10`。
- CAS/执行归属校验：`manager/tm-common/src/main/java/com/tapdata/tm/commons/task/heartbeat/HeartbeatRecoveryProtocol.java:10`。
- 源心跳诊断埋点：`iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/hazelcast/data/pdk/HazelcastSourcePdkBaseNode.java:1513`。
- 持久化成功埋点：`iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/hazelcast/data/pdk/HazelcastTargetPdkBaseNode.java:2106`。
- 引擎扫描/恢复：`iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/monitor/heartbeat/HeartbeatProgressWatchdog.java:29`。
- 任务启停锁与停止确认：`iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/schedule/TapdataTaskScheduler.java:1110`。
- TM 兜底：`manager/tm/src/main/java/com/tapdata/tm/schedule/TaskHeartbeatWatchdogSchedule.java:32`。

自动化测试覆盖无 DML 心跳推进、单元独立超时、宽限期、缺失/初始数据、重复上传、摘要长度、滚动预算、恢复验证、CAS 作用域、本地停止确认、用户停止优先、旧实例请求、TM 超时保护、真实目标持久化成功/失败埋点等。

2026-09-22 使用 Java 17 / Maven 3.9.8 完成下列干净构建回归：tm-common 13 项、TM 375 项、引擎 360 项，共 748 项，0 失败、0 错误，14 项既有跳过。该结果不替代真实 connector 的端到端故障注入验收。

回归命令（仓库根目录）：

```sh
mvn -o -pl manager/tm,iengine/iengine-app -am \
  -Dtest=HeartbeatWatchdogTest,TaskHeartbeatWatchdogScheduleTest,HeartbeatProgressRegistryTest,HeartbeatProgressWatchdogTest,HeartbeatCheckpointPersistenceTest,TapdataTaskSchedulerHeartbeatTest,TaskRestartScheduleTest,TaskPingTimeMonitorTest,HazelcastTaskClientTest,HazelcastTargetPdkBaseNodeTest,HazelcastSourcePdkBaseNodeTest,TapdataTaskSchedulerTest,TapdataTaskSchedulerEngineStartTest,TapdataTaskSchedulerStartPermitTest,TaskServiceImplTest \
  -Dsurefire.failIfNoSpecifiedTests=false clean test
```

## 残余待办

- 在有真实源心跳的低流量环境做端到端故障注入：阻塞读取、阻塞写入、检查点 HTTP 失败、停止不返回、TM 断连；验证重启位点无跳数、重复符合 connector 原有语义。
- 逐 connector 校准心跳最大间隔和确认边界，特别是并发目标及异步 offset callback；没有验证的任务不要启用。
- 对检查点缺失/损坏、心跳契约配置错误增加专门的 UNKNOWN 告警；当前不会因此自动重启。
- 接入邮件/短信告警与源日志有效窗口监测；目前仅监控日志与持久化恢复状态。
- 进程失联的 supervisor/fencing 自动恢复；本版只提供保守兜底，未实现跨 agent 自动切换。
- TM 重启或调度切换导致新的检测器首次观测时重新计时；已存在的恢复请求、超时和预算不会丢失。
