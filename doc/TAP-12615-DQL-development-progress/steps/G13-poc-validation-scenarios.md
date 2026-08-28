# G13 DQL POC 场景测试用例

## 1. 文档目的与代码实现分析

本文在现有“重复下单导致唯一索引冲突”用例基础上，补充可用于 POC 联调的 DQL 场景。所有场景都以真实任务、真实源/目标连接器和真实 TM/Engine 运行结果为准，不通过手工修改 `dql_events` 来制造成功结果。

当前实现的关键行为如下：

| DQL 能力 | 代码实现依据 | POC 观察点 |
| --- | --- | --- |
| 目标记录级异常捕获 | `SkipErrorEventAspectTask` + `DqlExceptionClassifier` | 单条唯一键、类型、长度、非空或 `SKIPPABLE_DATA` 错误进入 DQL，任务继续运行 |
| Processor/JavaScript 单条异常 | `skipErrorProcessAspectHandle` | `TRANSFORM_ERROR`、失败节点和原始 Payload 被记录 |
| Payload、预览和记录身份 | `DqlPayloadSerializer`、`DqlPayloadPreviewBuilder`、`DqlEventIdentityGenerator` | `payloadData`、安全预览、`eventKey`/`recordIdentity` 可查询，敏感字段不泄露 |
| 共享异常与未知异常保护 | `DqlExceptionClassifier`、`DlqStormGuard` | 网络/连接异常走任务级重试；未知异常超过窗口阈值后停止继续写入 DQL |
| 事件持久化与幂等 | `DqlEventService.report`、`DqlEventReporter` | 同一事件重复上报不新增主记录，首次落库触发 `TASK_DQL_EVENT` |
| 单条/批量恢复 | `DqlRecoveryBatchService`、`DqlRecoveryCoordinatorImpl` | 预览、排序、锁定、`dqlRecovery` 下发、attempt 和批次计数完整闭环 |
| 运行中/暂停任务恢复 | `DqlSourceReadGate`、`DqlRecoveryOnlyRunner` | 运行中任务暂停普通 Source 读取；暂停任务不启动普通 Source reader |
| 恢复失败防递归与补偿 | `DqlRecoveryCaptureGuard`、TM 超时扫描 | 恢复失败只结束当前 attempt，不生成新的 DQL 主记录；锁最终释放 |
| 后续成功覆盖风险 | `reportRecordSuccess` | 同记录后续普通写入成功后，原 DQL 事件标记 `overwriteRisk` |

其中，目标 MongoDB validator 或 MySQL 约束错误通常会被分类为 `TARGET_WRITE_ERROR`；只有目标连接器明确返回 PDK `SKIPPABLE_DATA` 错误码时，才应验证 `POISON_RECORD`。不能根据业务字段 `scenario=POISON` 直接修改或推断 DQL 类型。

## 2. 共通前置条件和证据

### 2.1 固定数据和变量

新增场景沿用现有用例中的环境：MongoDB 源集合 `customer_orders`，MySQL 目标表 `dql_poc_orders_1`，任务已开启 DQL/记录级异常处理并处于运行中。每次执行新场景使用不重复的 `user_id`、`order_no` 和 `op_seq`。

执行前记录以下动态变量：

```text
TASK_ID       = DQL 任务 ID
EVENT_ID      = DQL 事件 ID
BATCH_ID      = DQL 恢复批次 ID
ACCESS_TOKEN  = 当前登录 Web 的访问令牌（如需直接调用 API）
```

任务配置必须核对：

- `skipErrorEvent.errorMode` 为 `SkipData`；当前兼容代码也接受 `DQL`，但 POC 优先使用契约值 `SkipData`；
- `dql.event.enabled=true`；
- `limitMode` 和 `limit` 足以容纳当前场景，例如 `SkipByLimit`/`100`；
- 目标表存在主键或唯一业务键，且恢复场景使用支持幂等写入的配置。

### 2.2 共通查询

每个场景开始和结束时保存任务状态、目标表记录数和控制面 MongoDB 的 DQL 数量：

```javascript
// 在控制面 MongoDB 执行
db.dql_events.countDocuments({ task_id: TASK_ID })
db.dql_recovery_batches.countDocuments({ task_id: TASK_ID })
```

异常事件列表和详情使用以下 API：

```text
GET  /api/dql-events/summary?taskId={TASK_ID}
GET  /api/dql-events?taskId={TASK_ID}&skip=0&limit=20&order=-failedAt
GET  /api/dql-events/{EVENT_ID}
POST /api/dql-events/recovery/preview
POST /api/dql-events/recovery
GET  /api/dql-events/recovery-batches/{BATCH_ID}
```

可重处理事件的最低预期是：`status=PENDING` 或 `RECOVERY_FAILED`、`payloadComplete=true`、存在 `recordIdentity`/业务键。完整 Payload 缺失时状态为 `NOT_REPROCESSABLE`，不应强行提交恢复。

## 3. 场景 1：重复下单导致唯一索引冲突

这是现有场景，保留其业务背景和数据。它验证目标写入的单条唯一约束错误能被隔离并进入 DQL。

1. 使用 Mongo 作为源表，插入三条历史订单数据：

   ```javascript
   db.customer_orders.insertMany([{
     user_id: "1", order_no: "1", amount_text: "UID",
     event_time: new ISODate(), status: "PAYED", scenario: "i", op_seq: 1
   }, {
     user_id: "2", order_no: "2", amount_text: "CNX",
     event_time: 111, status: "OVER", scenario: "ei", op_seq: 2
   }, {
     user_id: "3", order_no: "3", amount_text: "IF",
     event_time: new ISODate(), status: "PAYED", scenario: "ie", op_seq: 3
   }])
   ```

2. 创建并启动订单表同步到 MySQL 表的 TapData 任务，任务开启 DQL 异常处理。
3. 在目标表为订单号增加唯一索引：

   ```sql
   CREATE UNIQUE INDEX idx_unique_order_no
     ON dql_poc_orders_1 (order_no);
   ```

4. 源端再次插入重复订单号，模拟用户重复点击下单：

   ```javascript
   db.customer_orders.insertOne({
     user_id: "1", order_no: "1", amount_text: "UID",
     event_time: new ISODate(), status: "PAYED", scenario: "i", op_seq: 2
   })
   ```

5. 查看任务状态和异常事件列表。

预期结果：

- 任务保持运行，不因单条记录失败而中断；
- 新增一条 DQL 主记录，`exceptionScope=RECORD`、`routeDecision=RECORD_DLQ`、`errorType=TARGET_WRITE_ERROR`；
- 事件详情能看到原始记录、错误信息、脱敏预览、事件身份和业务键；
- 触发 `TASK_DQL_EVENT` 告警，DQL 计数与跳过计数各增加 1；
- 如果历史数据中的 `event_time=111` 另行触发类型错误，应按另一条事件记录，不能把它与唯一索引冲突合并计算。

## 4. 场景 2：I/U/D 混合数据中的记录级异常隔离

本场景验证 DQL 对 Insert、Update、Delete 事件的 Payload 和身份处理，同时确认目标约束错误只影响失败记录。

1. 在源端插入两个正常订单，并保证目标端已同步：

   ```javascript
   db.customer_orders.insertMany([
     { user_id: "M-001", order_no: "M-001", amount_text: "10.00",
       event_time: new ISODate(), status: "CREATED", scenario: "MIX_I", op_seq: 1 },
     { user_id: "M-002", order_no: "M-002", amount_text: "20.00",
       event_time: new ISODate(), status: "CREATED", scenario: "MIX_I", op_seq: 1 }
   ])
   ```

2. 更新 `M-001` 的状态，验证 Update 正常同步：

   ```javascript
   db.customer_orders.updateOne(
     { user_id: "M-001" },
     { $set: { status: "PAYED", op_seq: 2 } }
   )
   ```

3. 删除 `M-002`，验证 Delete 正常同步：

   ```javascript
   db.customer_orders.deleteOne({ user_id: "M-002" })
   ```

4. 预先在目标端写入 `order_no=M-003`，再在源端插入另一条 `order_no=M-003` 的记录，制造 Insert 唯一键冲突。
5. 预先在目标端写入 `order_no=M-004`，再将源端 `M-001` 更新为 `order_no=M-004`，制造 Update 唯一键冲突。
6. 紧接着插入一条正常订单 `M-005`，验证异常后续记录仍可处理。

预期结果：

- `M-001` 的正常 Update 和 `M-002` 的 Delete 到达目标，不产生 DQL；
- Insert 冲突和 Update 冲突分别产生一条 DQL，事件的 `dmlType` 分别为 `I`、`U`（以实际 API 枚举格式为准）；
- 两条事件的 Payload、`eventKey`、`recordIdentity` 与对应源记录一致，不能把同一批数据错误合并成一条事件；
- `M-005` 正常写入目标，任务继续运行；
- 若目标连接器支持可控的 Delete 记录级约束错误，可额外对 Delete 注入同类错误，预期新增 `dmlType=D` 的事件；如果只能制造任务级/共享错误，则不把该错误归入 Delete DQL 场景。

## 5. 场景 3：JavaScript 转换异常与单条恢复

本场景覆盖 Processor 失败、错误分类、事件详情、TM 恢复预览、Engine 回放和恢复防递归。

1. 在源到目标之间增加 JavaScript Processor，脚本使用现有任务支持的处理函数格式：

   ```javascript
   function process(record) {
     var after = record.after || {};
     if (after.scenario === "SCRIPT_FAIL") {
       throw new Error("DQL_POC_SCRIPT_FAIL");
     }
     return record;
   }
   ```

2. 插入一条 `scenario=SCRIPT_FAIL` 的订单，随后立即插入一条正常订单。
3. 在事件详情中记录 `EVENT_ID`，查看任务日志、事件列表和目标表。
4. 将脚本修复为：

   ```javascript
   function process(record) {
     return record;
   }
   ```

5. 先预览恢复：

   ```bash
   curl -sS -X POST "http://localhost:3000/api/dql-events/recovery/preview?access_token=$ACCESS_TOKEN" \
     -H 'Content-Type: application/json' \
     --data '{"eventIds":["EVENT_ID"],"mode":"AUTO"}'
   ```

6. 确认 `canSubmit=true` 后提交恢复：

   ```bash
   curl -sS -X POST "http://localhost:3000/api/dql-events/recovery?access_token=$ACCESS_TOKEN" \
     -H 'Content-Type: application/json' \
     --data '{"eventIds":["EVENT_ID"],"confirm":true,"mode":"AUTO"}'
   ```

7. 轮询批次详情和事件详情，直到批次、attempt 和事件进入终态。

预期结果：

- 脚本错误产生一条 `TRANSFORM_ERROR`，`failedStage=PROCESSOR`、`routeDecision=RECORD_DLQ`，并包含 `DQL_POC_SCRIPT_FAIL`；
- 任务不停止，脚本失败后的正常订单成功到达目标；
- 预览返回该事件可恢复，提交后批次经过 `CREATED/DISPATCHED/RUNNING`，事件经过 `REPROCESSING` 最终变为 `RECOVERED`；
- `recovery_attempts` 新增一次 `SUCCESS`，目标端出现被恢复的订单；
- DQL 主记录数量不新增，恢复写入失败时也只能结束当前恢复事件为 `RECOVERY_FAILED`，不能递归创建新的 DQL 主记录；
- 运行中任务应能在恢复前暂停普通 Source 读取、恢复完成后重新开放；暂停任务变体应使用 recovery-only runner，且不启动普通 Source reader。

## 6. 场景 4：批量恢复顺序、幂等和无丢失

本场景使用三个已捕获且可恢复的 DQL 事件，验证 TM 固化顺序、批次锁、Engine 串行屏障和数量对账。

1. 准备三个不同事件时间的目标唯一键冲突，分别记录 `EVENT_A`、`EVENT_B`、`EVENT_C`。清理目标端原有冲突行，但不要修改 `dql_events`。
2. 以非时间顺序提交预览，例如：

   ```json
   {
     "eventIds": ["EVENT_C", "EVENT_A", "EVENT_B"],
     "mode": "AUTO"
   }
   ```

3. 检查预览中的 `orderedEvents`，再提交恢复：

   ```json
   {
     "eventIds": ["EVENT_C", "EVENT_A", "EVENT_B"],
     "confirm": true,
     "mode": "AUTO"
   }
   ```

4. 记录返回的 `BATCH_ID`，同时检查 `dql_recovery_batches.ordered_event_ids`、Engine 日志中的 `dqlRecovery.orderedEventIds` 和每个事件的 attempt 顺序。
5. 如果消息或回调测试工具支持，重复投递同一个批次消息和同一个 attempt 回调；否则至少保存 TM 的重复回调响应和批次详情作为证据。

预期结果：

- `orderedEvents` 和 `ordered_event_ids` 使用 `task_id ASC → event_time ASC → capture_seq ASC → event_id ASC`，与用户原始选择顺序无关；
- 同一任务同一时刻只能存在一个活动恢复批次，重复发起被拒绝；
- Engine 每次只推进一个事件，前一个事件收到成功、失败或超时结果后才处理下一个；
- 三条事件各只有一个有效终态 attempt，重复消息/回调不重复增加成功、失败或跳过计数；
- 目标唯一键无重复，DQL 主记录不新增；
- 对账公式成立：`输入记录数 = 首次成功数 + DQL 数`，`DQL 数 = 已恢复数 + 未恢复数`，`批次 selectedCount = successCount + failedCount + skippedCount`。

## 7. 场景 5：共享故障与未知异常 Storm Guard

### 7.1 目标短暂不可用：只走任务级重试

1. 在源端插入一条格式正常的订单。
2. 在目标写入窗口内，通过目标连接代理、防火墙或目标容器短暂停止制造连接拒绝/超时，持续时间短于任务最大重试时间。
3. 恢复目标连接，观察 Engine 日志、任务状态、目标数据和 DQL 数量。

预期：

- `SocketTimeoutException`、`ConnectException`、`SocketException`、`IOException` 或对应共享错误码被分类为 `TASK_RETRY`；
- 任务重试后从原进度继续，记录最终到达目标；
- 故障期间不把共享异常拆成逐条 DQL，DQL 数量不因积压数据批量增长。

### 7.2 未知单条异常风暴：触发保护而非无限写 DQL

1. 使用测试 Processor 或测试连接器制造“单条可定位、但不属于 `DqlExceptionClassifier` 已知错误码”的异常；不要使用普通 JavaScript 执行异常，因为它会优先被分类为 `TRANSFORM_ERROR`。
2. 在 60 秒窗口内，对同一任务/节点/表/错误码/归一化消息注入超过 20 条同类异常。
3. 检查事件数量、任务路由、日志和告警。

POC 默认阈值为：`windowSeconds=60`、`maxEvents=20`、`maxBatchRatio=0.2`、`decision=TASK_RETRY`。

预期：

- 阈值内的未知单条异常可产生有限的 `UNKNOWN_RECORD_ERROR` DQL；
- 第 21 条及之后同一 guard key 的事件不继续新增 DQL，转为 `TASK_RETRY`（若设置为 `TASK_ERROR`，按设置验证）；
- 触发 `TASK_DQL_STORM_GUARD` 告警，告警只包含安全摘要/guard key，不包含完整 Payload、原始异常堆栈或归一化敏感文本；
- 若没有可控未知异常注入点，应将本子场景标记为“待外部环境”，不得用已知脚本异常冒充 Storm Guard 端到端证据。

## 8. 场景 6：恢复失败、超时和补偿收敛

本场景验证 DQL 恢复链路的失败状态、告警、锁释放和防递归。

1. 选择一个 `PENDING` 或 `RECOVERY_FAILED` 事件，保持目标约束冲突不修复，提交单条恢复。
2. 观察 `EVENT_STARTED`、`EVENT_RESULT` 和批次详情。
3. 对另一个事件提交恢复后，在 Engine 回调前停止 Engine，或阻断目标写入使 barrier 超时。若需要缩短等待时间，只在隔离 POC 环境中设置较小的 `dql.recovery.eventTimeoutSeconds`/`dql.recovery.batchTimeoutSeconds`，并重新启动任务或创建新批次使配置快照生效。
4. 恢复 TM/Engine，等待回调或 TM 超时扫描完成。

预期结果：

- 目标约束仍存在时，当前事件变为 `RECOVERY_FAILED`，追加一个失败 attempt，触发 `TASK_DQL_RECOVERY_FAILED`；
- `continueOnEventFailure=true` 时，单事件失败不阻止批次按策略继续处理其他事件，批次最终为 `PARTIAL_FAILED` 或 `FAILED`；
- barrier 超时或 Engine 进程终止后，TM 超时扫描能把遗留事件和批次收敛到终态，并释放事件锁、任务锁；
- 重复回调不覆盖已有终态，不重复增加批次计数；
- 恢复过程中的目标/Processor 错误不新增 `dql_events` 主记录，原始失败原因保留在 recovery attempt/批次审计中。

## 9. 场景 7：事件查询、后续成功覆盖风险、权限和 TTL

本场景验证 DQL 事件的 Web/API 交接和生命周期证据。

1. 使用事件列表分别按 `taskId`、`sourceTable`、`targetTable`、`dmlType`、`errorType`、`status` 和 `keyword` 查询；再调用 summary 和 detail 接口。
2. 对有菜单权限但无目标任务权限的用户执行列表、详情、恢复预览；再用无菜单权限用户重复执行。
3. 对一个已经进入 DQL 的业务记录产生同一记录身份的后续普通成功写入，观察 Engine 到 TM 的 `record-success/report` 回调和事件详情。
4. 在控制面 MongoDB 检查初始化索引和 TTL 字段：

   ```javascript
   db.dql_events.getIndexes()
   db.dql_recovery_batches.getIndexes()
   db.dql_events.findOne({ event_id: EVENT_ID }, {
     event_id: 1, status: 1, ttl_at: 1, overwrite_risk: 1,
     recovery_count: 1, recovery_attempts: 1
   })
   ```

预期结果：

- 列表、summary、detail 的字段和状态计数一致；
- 无权限用户不能读取或操作其他任务事件，跨任务恢复预览被服务端拒绝；
- 后续同记录普通写入成功后，原事件 `overwriteRisk=true`，详情出现覆盖风险提示；恢复操作不因该回调自动生成新主记录；
- `dql_events` 和 `dql_recovery_batches` 均存在 `{ttl_at: 1}` 的 14 天 TTL 索引，索引名分别为 `idx_dql_event_ttl`、`idx_dql_batch_ttl`；
- 新建事件的 `ttl_at` 与 `created` 接近，恢复活动推进时 `ttl_at` 刷新；POC 不需要真实等待 14 天，只需保留索引、字段和刷新证据。

## 10. 覆盖矩阵和通过标准

| 场景 | 主要覆盖功能 | 关键证据 |
| --- | --- | --- |
| 场景 1 | 唯一键冲突、记录级路由、DQL 告警 | 1 条 `TARGET_WRITE_ERROR`，任务运行中 |
| 场景 2 | I/U/D、Payload、身份、异常后续处理 | DML 类型、事件身份、正常记录到达目标 |
| 场景 3 | Processor 捕获、单条恢复、Source gate、防递归 | `TRANSFORM_ERROR`、`RECOVERED`、无新增 DQL |
| 场景 4 | 批量恢复、顺序、锁、幂等、无丢失 | `ordered_event_ids`、批次计数和对账公式 |
| 场景 5 | 共享重试、Storm Guard、任务级路由 | 无共享故障批量 DQL，超过阈值后受保护 |
| 场景 6 | 恢复失败、超时、补偿、恢复告警 | attempt 终态、锁释放、`PARTIAL_FAILED/FAILED` |
| 场景 7 | 查询、详情、权限、覆盖风险、TTL | API 结果、权限边界、索引和 `ttl_at` |

POC 总体通过需要同时满足：

1. 记录级错误只影响对应记录，任务和后续正常记录按预期继续；
2. DQL 主记录、Payload、身份、状态、告警和事件数量可以相互对账；
3. 单条/批量恢复能按服务端顺序执行，成功、失败、超时和重复回调都能收敛；
4. 共享故障不被误判为记录级 DQL，未知异常风暴不会无限增长 DQL；
5. 目标表具备幂等能力时才对无重复作结论，不将 Exactly-Once 泛化到无幂等目标；
6. 不设置吞吐和延迟阈值，本文只验证功能、路由、状态、顺序、计数、告警、安全和恢复结果。
