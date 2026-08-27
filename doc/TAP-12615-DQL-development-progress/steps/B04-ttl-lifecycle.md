# B04 TTL 字段生命周期

## 步骤信息

- 状态：已完成
- 完成日期：2026-08-27
- 依赖：B02、B03

## 完成内容

- `dql_events` 和 `dql_recovery_batches` 创建时统一由 Repository 将 `ttl_at` 设置为最终 `created`；即使调用方传入其他 `ttlAt`，也不能改变初始过期基准时间。
- 事件进入 `REPROCESSING`、恢复成功、恢复失败和批次锁释放时，在同一个条件更新中使用同一 `Date` 刷新 `updated` 与 `ttl_at`。
- 批次进入 `DISPATCHED`、进入 `RUNNING`、成功/失败/跳过计数变化以及进入终态时，在同一个条件更新中使用同一 `Date` 刷新 `updated` 与 `ttl_at`。
- 单元测试明确校验 TTL 值为 `java.util.Date`，对应 MongoDB BSON Date，而不是数字时间戳或字符串。
- Repository 初始化仅保留普通查询索引和唯一索引，并通过测试确认不包含 `expireAfterSeconds`；TTL 索引仍由 B05 的 iDaaS 初始化脚本统一管理。

## 设计决策

- `ttl_at` 是服务端维护的生命周期字段，不属于调用方可覆盖的业务输入。创建态以 Repository 最终采用的 `created` 为唯一时间基准，避免调用方延长或缩短保存时间。
- 重处理活动只在合法状态推进、锁释放和批次进度更新实际执行时刷新 TTL，并与该次 `updated` 共享同一个时间对象，保持原子更新语义。
- B04 只负责字段生命周期；索引安装、升级兼容和真实过期行为分别留给 B05、F06 和 G09 验证，避免 Repository 与初始化脚本重复管理 TTL 索引。

## 验证

按测试驱动方式先加入冲突输入：为事件和批次传入晚于 `created` 的 `ttlAt`。修复前两项断言均按预期失败，证明现有实现会接受调用方覆盖；改为从 `created` 强制派生后转绿。

执行：

```text
mvn -o -pl tm -am \
  -Dtest=DqlEventRepositoryTest,DqlRecoveryBatchRepositoryTest \
  -Dsurefire.failIfNoSpecifiedTests=false -Djacoco.skip=true \
  -DsurefireArgLine='-javaagent:.../mockito-core-5.20.0.jar' test
```

结果：

- `DqlEventRepositoryTest`：11 个测试通过，0 失败，0 错误。
- `DqlRecoveryBatchRepositoryTest`：11 个测试通过，0 失败，0 错误。
- 共 22 个测试通过；Maven reactor 7 个模块全部构建成功。
- 覆盖创建态强制 `ttl_at=created`、事件锁定/成功/失败/解锁、批次下发/运行/计数/结束、BSON Date 类型以及 Repository 不创建 TTL 索引。

## 后续依赖

- B05 继续负责通过 `init/idaas/4.22-7.json` 创建两个集合的 14 天 TTL 索引。
- F06 在真实初始化和升级流程中验证脚本可重复执行、已有集合兼容及历史数据策略。
- G09 使用可控时间数据验证 MongoDB 实际清理行为，以及重处理活动刷新后过期时间相应顺延。
