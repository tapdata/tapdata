# F06 数据库初始化和升级兼容

## 步骤信息

- 状态：已完成（待集成验证）
- 完成日期：2026-08-28
- 依赖：B05、F01-F05

## 结论

F06 已完成代码和脚本级兼容验证。`4.22-7.json` 可从 `4.22-6` 被升级器扫描，空集合会创建 DQL 两个 TTL 索引；已有完全匹配索引时，`JsonFilePatch` 不会重复创建或删除；14 项 DQL Settings 使用 `upsert` 与 `$setOnInsert.value`，重复执行不会覆盖已有运行时值。

当前仓库没有可供本次执行的真实 MongoDB 实例，因此“MongoDB 实际建索引、TTL Monitor 清理时序和真实历史数据”仍由 G09/集成环境验收。该限制不影响初始化脚本的结构和执行器幂等测试结论。

## 场景分析

| 场景 | 处理结论 |
| --- | --- |
| 空库或集合不存在 | `createIndexes` 命令创建 `dql_events.idx_dql_event_ttl` 和 `dql_recovery_batches.idx_dql_batch_ttl`；Settings、权限和告警均按现有升级脚本写入。 |
| 已有集合但没有 DQL 索引 | 执行两个单字段 `{ttl_at: 1}` TTL 索引，过期时间为 `1,209,600` 秒（14 天）。 |
| 索引已存在且配置完全匹配 | `JsonFilePatch` 按索引名和完整定义跳过创建，不产生 `dropIndexes` 或 `createIndexes`。 |
| 索引同名但配置不匹配 | 执行器先删除同名非 `_id_` 索引，再创建脚本定义，确保过期字段和保留时长收敛。 |
| 重复执行 Settings 初始化 | `$set` 只更新元数据，默认运行时值只通过 `$setOnInsert` 写入，已有 `value` 不被覆盖。 |
| 历史数据缺少 `ttl_at` | 不做回填。MongoDB TTL 索引不会删除缺少索引字段的文档；若业务未来要清理这类历史数据，应另行设计有明确时间基准的一次性迁移，不能用初始化脚本猜测过期时间。 |

## 代码产出

- 新增 `DqlInitializationCompatibilityTest`，解析正式 `4.22-7.json`，验证两个集合的索引集合完整、TTL 字段/时长/名称正确，且没有针对 DQL 历史文档的 `update`/`delete` 回填命令。
- 扩展 `DqlTtlIndexPatchTest`，通过 `PatchesRunner -> ScriptPatchScanner -> JsonFilePatch` 验证从 `4.22-6` 执行脚本，以及在两个匹配索引已存在时不重复发出索引命令。
- 修正 F05 配置生效说明，区分 Engine 回放快照和 TM 按操作读取当前配置的语义，避免文档将两者误写为相同生效策略。

## 验证

通过：

```text
mvn -o -pl manager/tm -am \
  -Dtest=DqlInitializationCompatibilityTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

结果：2/2，TM reactor 编译通过；覆盖索引定义、无历史回填和 Settings 重复执行语义。

通过：

```text
mvn -o -pl manager/tm -am \
  -Dtest=DqlTtlIndexPatchTest \
  -Dsurefire.failIfNoSpecifiedTests=false -Djacoco.skip=true \
  -DsurefireArgLine=-javaagent:/Users/gavinxiao/.m2/repository/org/mockito/mockito-core/5.20.0/mockito-core-5.20.0.jar test
```

结果：2/2；分别覆盖无索引升级创建和已有匹配索引的重复执行。测试需要显式 Mockito agent，是因为当前 JetBrains JDK 17 环境无法让 Mockito inline 自动 self-attach。

## 后续依赖

- F07 继续验证 DQL 关闭、SkipData、SkipTable、迁移快照和旧任务告警流程不回归。
- G09 在真实 MongoDB 上验证 TTL Monitor 的实际删除和 `ttl_at` 刷新后的延期。
- G12 在发布环境补做真实空库、既有集合、同名异配置索引和历史数据检查。
