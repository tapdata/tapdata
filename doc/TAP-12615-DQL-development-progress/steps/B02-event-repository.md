# B02 完成 dql_events Repository

## 步骤信息

- 状态：已完成
- 完成日期：2026-08-27
- 依赖：B01

## 完成内容

- 集合不存在时创建 `dql_events`，集合已存在时仍校验并创建 7 个普通查询/唯一索引。
- `uk_task_event_identity` 改为只约束非空字符串 `event_identity` 的 partial unique index；普通记录身份查询索引不再使用 sparse。
- `nextCaptureSeq()` 通过 `Task.attrs.dqlEventSeq` 的 `$inc` 和 `findAndModify(returnNew=true)` 原子分配任务内序号，并校验任务 ObjectId 和任务存在性。
- 唯一 upsert 的捕获字段统一使用 `$setOnInsert`。并发重复上报命中同一唯一身份时返回既有主记录，不覆盖原 `event_id`、Payload、状态、创建时间或 TTL。
- 分页保留筛选、统计和 camelCase 到 Mongo 字段的排序映射；未传正数 `limit` 时使用 20 条默认值。
- 唯一 upsert 的 insert-only 捕获字段补齐 `target_node_id`、`target_node_name`，避免上报后丢失详情所需的目标节点信息。
- 事件锁只允许 `PENDING`/`RECOVERY_FAILED` 且 `current_batch_id=null` 的事件进入 `REPROCESSING`。
- 恢复完成、恢复失败和批次锁释放均使用 `status + current_batch_id` 条件更新；attempt 通过 `$push` 追加，不覆盖历史。
- 后续成功写入仍按同任务、同记录身份和未完成状态选择最新事件并原子标记覆盖风险。

## 设计校正

详细设计原 partial index 示例使用 `$exists + $ne` 表示非空字段，但 MongoDB partial index 不支持 `$ne` 过滤。实现与设计统一改为 `$type: "string" + $gt: ""`，保留“只索引非空字符串身份”的业务语义。

## 验证

执行：

```text
mvn -o -pl tm -am -Dtest=DqlEventRepositoryTest \
  -Dsurefire.failIfNoSpecifiedTests=false -Djacoco.skip=true \
  -DsurefireArgLine='-javaagent:.../mockito-core-5.20.0.jar' test
```

结果：

- `DqlEventRepositoryTest`：11 个测试通过，0 失败，0 错误。
- Maven reactor 7 个模块全部构建成功。
- 测试覆盖集合/索引初始化、partial unique、原子序号、并发重复 upsert 的 insert-only 语义、分页排序和默认值、事件锁、成功/失败 attempt、锁释放及覆盖风险更新。
- 扩展 DQL 回归中 B01 枚举、Controller、Service 和 B02 Repository 共 37 个测试通过；基线中的 `DqlRecoveryBatchRepositoryTest` 有 1 个断言未按 Mongo `$in` BSON 结构读取，归属 B03，未混入本步骤修改。

## 后续影响

- B07 可通过比较 upsert 入参与返回事件 ID，完善并发重复上报的 `duplicate=true` 和告警去重语义。
- B09 可在 Repository 现有分页/统计能力上补齐安全排序白名单和任务权限范围过滤。
- B04 的 TTL 集成验证可直接复用本步骤已覆盖的条件更新和同次 TTL 刷新断言。
