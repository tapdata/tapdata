# Task DQL Impact Check Design

## Scope

本设计为 TAP-12615 DQL 增加任务删除/重置前的影响提示能力。它只提供只读预检查，不改变任务删除、重置和 DQL 重处理的既有状态机语义。

纳入范围：

- 数据复制列表、数据转换列表的单条删除和单条重置。
- 数据复制列表、数据转换列表的批量删除和批量重置。
- 任务详情页的任务重置。

不纳入范围：

- 任务详情页画布节点删除。
- 空画布返回时的特殊任务清理。
- 共享缓存、共享挖掘、心跳任务等非本需求列表的额外生命周期入口。

## Existing behavior and gap

TM 当前已经保存 DQL 事件的 `task_id`、`task_version` 和 `status`。任务重置会在发送新的重置消息前递增任务版本；因此旧版本 DQL 事件即使仍保留在 `dql_events` 中，也无法按当前任务配置再次重处理。

当前 `DqlEventRepository.countPendingByTaskId` 只按任务 ID统计 `PENDING` 和 `RECOVERY_FAILED`，没有按任务当前版本过滤，也没有包含 `REPROCESSING`。TM 没有任务生命周期影响检查 API。Web 列表和任务详情的重置流程也直接调用原有删除/重置 API。

## API

新增 TM 接口：

```http
POST /api/Task/dql-event-impact
Content-Type: application/json
```

请求体：

```json
{
  "taskIds": ["task-id-1", "task-id-2"]
}
```

响应数据为按请求任务返回的结果数组：

```json
[
  { "taskId": "task-id-1", "exists": true, "count": 3 },
  { "taskId": "task-id-2", "exists": false, "count": 0 }
]
```

`exists` 表示当前用户数据范围内任务是否存在。不存在、已逻辑删除或不可见的任务返回 `exists=false` 且 `count=0`，不泄露 DQL 数据。

## Counting rule

对每一个可见任务，使用任务当前 `version` 查询 `dql_events`：

```text
task_id = current task id
AND task_version = current task version
AND status IN (PENDING, REPROCESSING, RECOVERY_FAILED)
```

`RECOVERY_FAILED` 纳入统计，是因为现有 DQL 设计允许该状态再次重处理。`RECOVERED` 和 `NOT_REPROCESSABLE` 不计入，因为它们不再属于可继续重处理的记录。任务版本缺失时不进行版本补写，计数为零；DQL 上报本身仍由现有版本校验负责。

查询使用批量任务 ID和任务数据权限范围，不能通过 DQL 菜单权限替代任务操作权限。该接口不锁定任务、事件或恢复批次，也不修改任何数据。

## Backend layering

在 TM DQL 域增加专用影响检查服务，复用 `DqlEventRepository` 的 Mongo 查询能力和 `TaskService` 的任务数据范围查询。任务控制器暴露接口并返回统一 `ResponseMessage`。删除、重置、批量删除、批量重置接口不调用该服务，不因 DQL 结果而失败或阻塞。

接口输入为空时返回空数组；非法任务 ID按现有任务 API 参数校验处理。返回结果保持请求顺序，便于 Web 端将结果和选择行稳定匹配。

## Frontend interaction

Web 在 `packages/business/src/views/task/List.vue` 中集中封装预检查流程，因此数据复制列表和数据转换列表共用同一实现。单条和批量删除/重置均沿用现有任务确认框，并在用户确认原操作后进行 DQL 影响检查：

1. 原有确认框取消，流程结束。
2. 原有确认框确认后调用影响检查接口。
3. 所有任务 `count=0`，直接执行原有删除/重置 API。
4. 任一任务 `count>0`，按任务名称和记录数展示 DQL 影响二次确认。
5. 二次确认取消，不执行任务删除/重置；确认后执行原有 API。

影响提示说明任务删除或重置会使这些 DQL 记录未来无法重处理。它不提供自动删除 DQL 记录的动作，也不改变 DQL TTL 清理策略。

任务详情的重置入口在执行 `resetTask` 前复用同一 API 和二次确认逻辑，覆盖当前新旧详情实现使用的重置入口。画布节点删除不调用该接口。

影响检查是提示性质。请求失败、超时或响应无法解析时，Web 继续使用原有确认结果执行任务操作，不把辅助接口故障变成任务操作阻塞。原有删除/重置接口的错误处理保持不变。

## Permissions and concurrency

接口只返回任务是否存在和数量，不返回事件明细或事件 ID。任务查询必须使用当前用户的数据权限范围。任务删除/重置本身继续由既有任务接口执行权限控制。

检查结果是操作时刻的提示快照。检查完成后可能有新 DQL 事件产生，或其他请求可能先完成任务重置；后端不承诺计数与最终操作时刻完全一致，也不增加锁或版本条件。任务操作仍按既有状态机和错误语义执行。

## Verification

TM 测试覆盖：

- 任务不存在、任务版本缺失和任务可见性。
- 任务 ID、当前版本及三种非终态的组合查询。
- 多任务返回顺序和独立计数。
- Controller 请求映射、用户传递和统一响应封装。

Web 测试/静态验证覆盖：

- 单条和批量删除/重置均使用预检查。
- 有影响时必须二次确认，无影响时保持原确认流程。
- 预检查失败仍执行原操作。
- 详情重置入口接入，节点删除不接入。

