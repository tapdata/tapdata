# F03 DQL 多语言告警模板

## 结论

F03 已完成，标记为“待集成验证”。简体中文、繁体中文和英文告警资源均已补齐以下四项模板：

- `TASK_DQL_EVENT`
- `TASK_DQL_SAVE_FAILED`
- `TASK_DQL_RECOVERY_FAILED`
- `TASK_DQL_STORM_GUARD`

模板参数统一采用任务、事件、批次和时间等安全摘要字段；错误信息由后续服务在进入告警前截断/脱敏。模板中明确不出现完整 Payload、`payload_data`、`recordIdentity` 或 `stackTrace`。

## 代码产出

- `manager/tm/src/main/resources/alarmTemplate_zh_CN.properties`
- `manager/tm/src/main/resources/alarmTemplate_zh_TW.properties`
- `manager/tm/src/main/resources/alarmTemplate_en_US.properties`
- `manager/tm/src/test/java/com/tapdata/tm/init/DqlAlarmTemplateTest.java`

四种模板使用的字段集合已在测试中冻结：事件创建包含任务、事件、源/目标表、DML、错误分类/错误码和时间；保存失败包含任务/事件、错误码和安全错误摘要；恢复失败包含批次、状态、计数和操作人；Storm Guard 包含保护键、窗口、阈值、抑制估算量和路由决策。所有模板还包含统一的详情地址和告警时间。

## 验证记录

通过：

```text
mvn -o -pl manager/tm -am -Dtest=DqlAlarmTemplateTest -Dsurefire.failIfNoSpecifiedTests=false test
```

结果为 1 项测试通过，TM reactor 编译成功。`git diff --check` 在本步骤提交前通过。

## 后续依赖

F04 负责将模板对应的四个 key 接入 `AlarmService`，并确保重复上报/重复回调不重复产生告警；实际通知渠道、模板渲染和 Storm Guard 端到端联调仍需 TM/Engine 运行环境。
