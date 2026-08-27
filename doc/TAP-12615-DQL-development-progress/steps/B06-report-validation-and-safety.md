# B06 上报校验和安全处理

## 步骤信息

- 状态：已完成
- 完成日期：2026-08-27
- 依赖：A03、A05、B01

## 完成内容

- 新增 `DqlReportValidationService`，作为 Engine DQL 上报进入身份生成、去重和持久化之前的 TM 侧信任边界。
- 校验 `taskId` 是合法 ObjectId，并通过 `TaskRepository` 确认任务存在；任务不存在时返回 `Task.NotFound`，即使 Engine 已显式提供 `captureSeq` 也不能绕过任务校验。
- `exceptionScope` 和 `routeDecision` 为空时分别归一化为 `RECORD`、`RECORD_DLQ`；显式上报其他异常范围或路由决策时返回 `DqlEvent.InvalidRouteDecision`。
- `errorDetails` 在 TM 侧先对 JSON、`key=value`/`key: value` 和 Authorization 等文本中的敏感字段值执行二次脱敏，再限制为 4000 字符，并把截断结果写入 `error_details_truncated`；敏感对象/数组按平衡结构整体遮蔽，未加引号的敏感值遮蔽到行尾，结构或引号未闭合时保守遮蔽到输入末尾。
- 使用 Jackson 将 `payloadData` 序列化为 UTF-8 JSON 字节，实际大小与 Engine 上报的 `payloadSize` 取较大值，避免通过低报大小绕过 1 MiB 上限，也保留 Engine 已知的原始 Payload 总大小。
- Payload 超过 1048576 字节时不保存完整 `payload_data`，将 `payload_complete=false`，由事件服务保存为 `NOT_REPROCESSABLE`；安全预览及 Payload 元数据仍可用于查询和诊断。
- 对 `payloadPreview` 执行递归二次安全处理：敏感字段统一替换为 `******`，字符串最多 512 字符，Map/List/数组每层最多 50 项，最大嵌套深度为 4，发生任何结构或内容截断时设置 `payload_preview_truncated=true`。
- 敏感字段名按大小写不敏感方式匹配 `password`、`passwd`、`secret`、`token`、`access_token`、`authorization`、`credential` 和 `apikey`。
- 新增通用 `IgnoreRequestBodyLog` 标记，并应用到三个 Engine DQL 回调；通用控制器日志仅记录参数已省略，不在校验执行前序列化完整 Payload、事件键或错误详情。
- 列表和详情服务继续在返回前清除 `payloadData`；B06 回归测试确认外部查询接口不会返回完整 Payload。

## 设计决策

- 安全处理集中在独立 Service，而不是继续堆叠在 `DqlEventService`，与详细设计中的组件划分一致，也为 F05 后续接入动态系统配置保留单一入口。
- 当前使用 A05 已冻结的默认值；读取系统设置、非法配置保护和环境覆盖仍由 F05 完成，本步骤不提前扩大配置开发范围。
- TM 只对 Engine 提供的预览执行二次安全处理，不在 B06 生成 Engine 侧完整预览、Payload hash 或事件身份；这些职责分别留给 C03 和 B07。
- 超限 Payload 的主记录仍然保存，但完整 `payloadData` 被移除，状态为 `NOT_REPROCESSABLE`；这满足“摘要可查询但不可恢复”的设计，并避免 Mongo 文档继续承载超限内容。
- `payloadPreviewTruncated` 保留 Engine 已上报的 true，并与 TM 本次处理产生的截断结果做逻辑或，避免二次处理错误清除既有截断信息。

## TDD 与验证

- 红灯阶段先新增 `DqlReportValidationServiceTest`；测试因目标 Service 尚不存在而按预期编译失败。
- 最小实现后，首次运行暴露本机 JetBrains JDK 无法让 Mockito 自附加。按既有测试方式显式加载 `mockito-core-5.20.0.jar` javaagent 后，确认该问题只属于测试 JVM 环境。
- 新增 11 个校验与安全处理测试，覆盖任务不存在、空白/非法任务 ID、路由缺省与非法显式值、标量/结构化/畸形错误详情脱敏、Digest Authorization 与复合 token、错误详情截断、UTF-8 实际字节计算、超限 Payload 摘要化、完整敏感字段集合，以及字符串/深度/Map/List 条目限制。
- `DqlEventServiceTest` 新增上报链路集成用例，确认安全处理结果最终按 `payloadData=null`、`payloadComplete=false`、`NOT_REPROCESSABLE`、脱敏预览、脱敏错误详情和 `errorDetailsTruncated=true` 持久化。
- `LogAOPTest` 验证受保护方法只记录参数已省略；`DqlEventControllerTest` 验证异常上报、正常写入回调和恢复结果回调均带请求日志保护标记。
- 独立代码审查先后检出错误详情未脱敏、通用 AOP 在校验前记录请求体、结构化/畸形敏感值提前终止及 Digest/复合 token 逗号分隔绕过；均先补复现用例再修复，并由后续复审确认前序问题闭环。

执行：

```text
mvn -o -pl tm -am \
  -Dtest=DqlEnumContractTest,LogAOPTest,DqlEventControllerTest,DqlEventServiceTest,DqlReportValidationServiceTest,DqlEventRepositoryTest,DqlRecoveryBatchRepositoryTest,DqlTtlIndexPatchTest,PatchesRunnerTest \
  -Dsurefire.failIfNoSpecifiedTests=false -Djacoco.skip=true \
  -DsurefireArgLine='-javaagent:.../mockito-core-5.20.0.jar' test
```

结果：

- `tm-common`：`DqlEnumContractTest` 4 个测试通过。
- `tm`：上报校验、日志保护、Service、Controller、两个 Repository、TTL 初始化和补丁执行链路共 66 个测试通过。
- A01-B06 合计 70 个测试通过，0 失败，0 错误；Maven reactor 7 个模块全部构建成功。
- `git diff --check` 通过。

## 后续依赖

- B07 继续处理 Engine 显式身份优先、TM 身份兜底和唯一 upsert 去重，不改变本步骤的安全处理顺序。
- B08 完成 Engine 上报 API 的其余回调契约测试和保存失败边界。
- B09 完成列表、详情和统计的专用 Web DTO 映射；必须继续保证任何外部查询响应不包含完整 `payload_data`。
- F05 将本步骤中的冻结默认值接入系统配置读取和环境覆盖。
- B07 及后续功能不在本步骤范围内，本步骤提交后暂停等待 review。
