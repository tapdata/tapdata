# F02 DQL 告警 Key 与默认设置

## 结论

F02 已完成，标记为“待集成验证”。本步骤冻结并接入四个 DQL 任务告警 key，同时保持任务告警 schema 对前端的追加顺序：

1. `TASK_DQL_EVENT`：新 DQL 事件创建。
2. `TASK_DQL_SAVE_FAILED`：DQL 主记录保存失败。
3. `TASK_DQL_RECOVERY_FAILED`：单事件或批次恢复失败。
4. `TASK_DQL_STORM_GUARD`：未知异常风暴保护切换到任务级处理。

四个 key 均使用 `TYPE_EVENT`，并位于 `AlarmKeyEnum.getTaskAlarmKeys()` 的末尾。新任务初始化时追加全部四项；历史任务补齐缺失项时也只追加到末尾，不改变既有 key 的相对顺序。

## 代码与初始化产出

- `manager/tm-common/.../AlarmKeyEnum.java` 增加 `TASK_DQL_STORM_GUARD`，完成四项 DQL key 的连续定义。
- `manager/tm/.../TaskSaveServiceImpl.java` 在新任务和历史任务两条补齐路径中追加四项 DQL 告警设置。
- `manager/tm/src/main/resources/init/idaas/4.22-7.json` 增加 `Settings_Alarm` 的四项幂等 upsert，默认开启、通知方式为 `SYSTEM`，并提供任务/事件/批次定位所需的安全标题和正文模板。
- 扩展 `TaskSaveServiceImplTest` 覆盖新任务追加顺序；增加 `DqlAlarmKeyTest` 覆盖枚举类型及末尾顺序；扩展 `DqlInitializationPatchTest` 覆盖四项默认设置。

初始化脚本只写告警 schema 和安全摘要模板，不写入完整 Payload、recordIdentity 或原始错误对象。

## 验证记录

已通过：

- `mvn -o -pl manager/tm-common -Dtest=DqlAlarmKeyTest -Dsurefire.failIfNoSpecifiedTests=false test`：1 项通过。
- `mvn -o -pl manager/tm -am -Dtest=DqlInitializationPatchTest -Dsurefire.failIfNoSpecifiedTests=false test`：1 项通过，包含 TM reactor 编译。
- `git diff --check`：通过。

`TaskSaveServiceImplTest` 已补充业务断言，但当前本机 JVM 无法完成 Mockito inline 的 Byte Buddy agent attach（`Could not self-attach to current VM using external process`），该问题也影响该类原有测试，未执行到断言。生产代码通过编译，顺序契约由无 Mockito 的 `DqlAlarmKeyTest` 和初始化契约测试覆盖；待具备正常 Mockito agent attach 的 CI 或集成环境后补跑该类。

## 后续依赖

F03 继续补齐简中、繁中和英文告警模板；F04 将把已有 DQL 事件/批次触发点接入真实 `AlarmService`，并在 TM/Engine 联调中验证重复上报、失败回调和告警抑制语义。
