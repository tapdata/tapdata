# B07 事件身份与去重

## 步骤信息

- 状态：已完成
- 完成日期：2026-08-27
- 依赖：B02、B06

## 完成内容

- 新增 `DqlEventIdentityService`，统一异常事件上报和后续成功写入回调的记录身份兜底逻辑，Engine 显式上报的 `recordIdentity`、类型、字段和 `eventIdentity` 始终优先。
- TM 在 `payloadHash` 缺失时，先对完整 Payload 的规范化 JSON 计算 SHA-256；该计算发生在 B06 超限处理移除 `payloadData` 之前，因此超限事件仍保留可用于诊断和去重的 Payload 指纹。
- 规范化 hash 对对象属性和 Map key 排序，对数组顺序保持不变，避免 Java Map 插入顺序或嵌套对象字段顺序造成身份漂移。
- `recordIdentity` 兜底优先使用 `eventKey`，生成 `key:{table}:sha256:{hash}`、`PRIMARY_KEY` 和排序后的字段名；没有业务键时使用 `payloadHash` 生成 `hash:{table}:{payloadHash}` 和 `FULL_FIELD_HASH`；无可用输入时类型归一为 `UNKNOWN`。
- `eventIdentity` 按详细设计优先级生成：原事件 `exactlyOnceId`、事件 info 中的 source offset/LSN/oplog position、业务键身份、Payload 身份；source offset、业务键和 Payload 均使用规范化 SHA-256。
- 普通流后续成功写入回调复用相同的 `recordIdentity` 算法，确保 DQL 主记录和覆盖风险回调在 Engine 未显式提供身份时仍能匹配。
- 保留 B02 的部分唯一索引和 `$setOnInsert` 原子 upsert。顺序重复在 upsert 前直接返回已有主记录；并发重复由 upsert 返回的已有 `eventId` 识别，响应 `duplicate=true`，不覆盖首次捕获快照、不刷新 TTL，也不重复触发告警。

## 设计决策

- 身份算法集中到独立 Service，避免异常上报与后续成功回调形成两套字符串拼接规则，也避免继续扩张 `DqlEventService`。
- Engine 对主键、唯一索引和全字段 hash 拥有更完整的模型信息，因此显式值不做重算或格式改写；TM 生成值只作为缺失情况下的稳定兜底。
- TM 的 `eventKey` 兜底使用规范化 hash，而不是直接拼接 `field=value`。这既保留字段集合用于审计，又避免字段值包含分隔符或嵌套对象时产生碰撞。
- `eventIdentity` 不包含 `taskId`，因为唯一索引已经以 `task_id` 作为第一维；其余组成保持详细设计 8.7 节约定。
- 并发重复判定按 B02 约定比较本次候选 `eventId` 与 upsert 返回的 `eventId`。竞争中分配但未落库的 `captureSeq` 允许产生间隙，仍保持任务内单调递增语义。

## TDD 与验证

- 红灯阶段先新增 `DqlEventIdentityServiceTest`，测试因目标 Service 尚不存在而按预期编译失败。
- 新增 6 个身份测试，覆盖 Engine 显式值优先、`exactlyOnceId` 优先级、source offset 规范化、不同 Map 插入顺序生成同一身份、Payload 变化生成不同身份，以及成功回调复用记录身份。
- `DqlReportValidationServiceTest` 新增超限 Payload 在移除前生成指纹的测试。
- `DqlEventServiceTest` 新增原子 upsert 竞争返回已有主记录时 `duplicate=true` 且不触发告警的测试；既有顺序重复用例继续验证不再次持久化和告警。
- B02 Repository 回归继续验证唯一条件使用 `task_id`、`task_record_id`、`table_id`、`event_identity`、`failed_node_id`，且 upsert 更新只包含 `$setOnInsert`。

执行：

```text
mvn -o -pl tm -am \
  -Dtest=DqlEnumContractTest,LogAOPTest,DqlEventControllerTest,DqlEventIdentityServiceTest,DqlEventServiceTest,DqlReportValidationServiceTest,DqlEventRepositoryTest,DqlRecoveryBatchRepositoryTest,DqlTtlIndexPatchTest,PatchesRunnerTest \
  -Dsurefire.failIfNoSpecifiedTests=false -Djacoco.skip=true \
  -DsurefireArgLine='-javaagent:.../mockito-core-5.20.0.jar' test
```

结果：

- `tm-common`：`DqlEnumContractTest` 4 个测试通过。
- `tm`：身份、上报校验、日志保护、Service、Controller、两个 Repository、TTL 初始化和补丁执行链路共 74 个测试通过。
- A01-B07 合计 78 个测试通过，0 失败，0 错误；Maven reactor 7 个模块全部构建成功。
- `git diff --check` 通过。

## 后续依赖

- B08 在本步骤身份和去重语义上补齐 Engine 上报 API 的其余契约测试与保存失败边界。
- C03 Engine 侧仍应优先生成准确的 `recordIdentity`、`payloadHash` 和 `eventIdentity`；TM 本步骤逻辑只提供可信兜底。
- F04 复用“仅新主记录触发告警”的结果，不改变唯一 upsert 和重复响应语义。
- B08 及后续功能不在本步骤范围内，本步骤提交后暂停等待 review。
