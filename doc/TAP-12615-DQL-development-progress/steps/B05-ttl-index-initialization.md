# B05 TTL 索引初始化

## 步骤信息

- 状态：已完成
- 完成日期：2026-08-27
- 依赖：B04

## 完成内容

- 确认 `manager/tm/src/main/resources/init/idaas/4.22-7.json` 通过两条 `createIndexes` 命令分别为 `dql_events` 和 `dql_recovery_batches` 创建 TTL 索引。
- 两个索引均为 `{ "ttl_at": 1 }` 单字段索引，`expireAfterSeconds=1209600`，即从 `ttl_at` 起保留 14 天。
- 索引名固定为 `idx_dql_event_ttl` 和 `idx_dql_batch_ttl`，便于升级检查和运维诊断。
- 确认 `manager/tm/src/main/resources/init/idaas/version` 已推进至 `4.22-7`，升级器能够从 `4.22-6` 扫描并执行该脚本。
- 新增 `DqlTtlIndexPatchTest`，通过真实 `PatchesRunner -> ScriptPatchScanner -> JsonFilePatch` 链路加载版本文件和脚本，并在 MongoDB 命令边界校验最终行为。
- 复用 B04 Repository 测试确认两个 Repository 初始化时均不创建带 `expireAfterSeconds` 的索引，TTL 索引只有 iDaaS 初始化脚本一个管理入口。

## 设计决策

- 进入 B05 时脚本和版本文件已经具备正确内容，因此本步骤不对正确配置做无意义改写，而是补齐可执行的升级链路回归测试并据此完成验收。
- 测试断言初始化执行器最终提交给 MongoDB 的命令，而不是只读取或搜索 JSON 文本；版本文件未推进、脚本未被扫描、集合名错误、索引非单字段、过期秒数错误或索引名错误都会导致测试失败。
- B05 验证升级链路和 MongoDB 命令契约；空库、已有集合、重复执行、历史数据兼容和真实 MongoDB 索引安装由 F06 集成验证，后台实际清理及调度延迟由 G09 验证。

## 验证

基线执行：

```text
mvn -o -pl tm -am \
  -Dtest=PatchesRunnerTest,DqlEventRepositoryTest,DqlRecoveryBatchRepositoryTest \
  -Dsurefire.failIfNoSpecifiedTests=false -Djacoco.skip=true \
  -DsurefireArgLine='-javaagent:.../mockito-core-5.20.0.jar' test
```

结果：27 个测试通过，0 失败，0 错误。

B05 链路测试执行：

```text
mvn -o -pl tm -am -Dtest=DqlTtlIndexPatchTest \
  -Dsurefire.failIfNoSpecifiedTests=false -Djacoco.skip=true \
  -DsurefireArgLine='-javaagent:.../mockito-core-5.20.0.jar' test
```

结果：

- `DqlTtlIndexPatchTest`：1 个测试通过，0 失败，0 错误。
- 实际扫描范围为 `4.22-6 -> 4.22-7`，且只发现一个 DAAS 补丁。
- `JsonFilePatch` 成功提交两条索引命令，Maven reactor 7 个模块全部构建成功。
- A01-B05 最终定向回归中，TM 52 个测试和 `tm-common` 4 个枚举契约测试全部通过，共 56 个测试，0 失败，0 错误。
- 由于配置在本步骤开始前已经存在，按确认的设计采用真实链路特征测试；验证未发现需要修改初始化脚本的偏差。

## 后续依赖

- F06 使用真实 MongoDB 环境验证空库、已有集合、脚本重复执行、同名异配置索引及历史无 `ttl_at` 文档等升级场景。
- G09 使用可控时间数据验证 MongoDB TTL Monitor 的实际清理行为，以及 B04 刷新 `ttl_at` 后过期时间顺延。
- B06 及后续功能不在本步骤范围内，本步骤提交后暂停等待 review。
