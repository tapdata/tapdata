# DQL 异常事件从失败节点回放设计

## 目标

异常事件重处理必须先停止对应任务，再使用原始任务的节点处理逻辑执行回放。回放完成后，任务恢复到重处理开始前的状态。回放不得修改 MySQL 或其他业务数据源之外的任何业务数据，也不得把临时节点状态写回持久化任务配置。

## 已确认的约束

- 任务原始状态由 Engine 在收到重处理命令时读取并保存；当前只接受 `running` 和 `stop` 两种可恢复状态。
- Engine 通过正式任务调度链路停止任务，不能只关闭 `DqlSourceReadGate`。
- 回放使用原任务 DAG 的深拷贝，不直接修改任务 Mongo 文档中的 DAG。
- 以 DQL 事件的 `failedNodeId` 作为回放起点；回放入口向该节点投递 Payload 表示的输入事件。
- 临时 DAG 保留失败节点以及从该节点可达的下游处理节点和目标节点；其他节点在临时 DAG 中设置 `attrs.disabled=true`。
- 被临时隐藏的节点必须记录节点 ID、名称、原始 disabled 状态和恢复结果。
- 回放完成、失败、超时或初始化异常时，临时节点状态都要在 finally 路径恢复，并销毁临时回放 Job。
- 原始状态为 `running` 时回放结束后重新启动正式任务；原始状态为 `stop` 时保持停止。
- Join、Merge 或其他依赖任务运行时内存状态的多输入/有状态失败节点，如果 Payload 不足以重建状态，Engine 必须拒绝回放并恢复任务，不得产生不确定结果。

## 运行流程

1. TM 创建并锁定 DQL recovery batch，消息携带任务 ID、任务版本和事件 ID 顺序。
2. Engine 校验任务版本、Agent 归属和当前状态，并获取每个 DQL 事件的失败节点元数据与完整 Payload。
3. Engine 在任务级本地锁下停止正式任务，等待任务 Job 终止且任务状态为 `stop`。
4. Engine 深拷贝原始 TaskDto/DAG，计算每个事件失败节点到下游目标的保留节点集合。批次内事件必须使用同一条可回放路径；路径不一致时批次失败。
5. Engine 在副本 DAG 中隐藏保留集合之外的节点，保存节点状态快照，并在失败节点输入边前插入唯一的 DQL replay source。正式 Source reader 不启动。
6. Engine 创建临时 Jet Job。除 replay source 外，失败节点及其下游节点全部复用正式节点实现，因此字段转换、脚本、过滤、目标写入等逻辑不被绕过。
7. Engine 按 TM 固化顺序逐条构造带 recovery 标记的 TapdataEvent，注入失败节点，等待目标完成屏障，然后回调事件结果。
8. Engine 关闭临时 Job，恢复副本节点状态并写入批次审计；副本随后丢弃。
9. 如果原始状态是 `running`，Engine 通过正式调度器重新启动任务并确认接管；如果原始状态是 `stop`，不启动任务。
10. 任一步骤失败都回调批次失败；恢复任务失败时错误信息必须同时包含回放错误和任务恢复错误。TM 超时扫描继续作为 Engine 崩溃时的最终批次收敛机制。

## 协议变化

### Engine 获取的 Payload

现有 `DqlRecoveryPayloadVo` 只返回 Payload，无法计算失败节点路径。内部 recovery-payload 接口增加：

- `failedNodeId`、`failedNodeName`
- `sourceNodeId`、`sourceNodeName`
- `targetNodeId`、`targetNodeName`
- 原有完整 Payload 字段

### 批次审计

在 recovery report 增加节点准备/恢复信息。TM 批次详情至少可看到：

- 本次临时隐藏的节点列表及其原始 disabled 状态；
- 节点隐藏是否成功；
- 节点恢复是否成功；
- 任务停止、回放、任务恢复的失败原因。

节点审计使用结构化字段，不把节点列表只拼接进普通 message。

## 临时 DAG 规则

对每个 DQL 事件：

- `keep = failedNode + descendants(failedNode)`，必要时限制到事件对应目标节点；
- 所有 `keep` 外节点设置 `attrs.disabled=true` 和 `disabled=true`；
- failedNode 的原始入边改由临时 replay source 提供，避免 disabled 的上游节点重新执行；
- failedNode 到目标节点的边和节点实现保持原样；
- 已经持久化为 disabled 的节点仍保持 disabled，恢复时只能恢复临时修改的节点；
- 批次内 failedNode、目标节点或路径无法统一时直接失败，不猜测路径。

禁用节点的恢复必须基于快照逐节点恢复，而不是统一设置为 false，避免污染任务原有配置。

## TDD 与验证

- 先测试临时 DAG 深拷贝不会修改原 DAG。
- 测试只隐藏失败节点上游及无关分支，保留失败节点到目标的全部处理节点。
- 测试 replay source 能向失败节点输入事件，且正式 Source reader 未启动。
- 测试 running/stop 两种任务状态的停止与恢复，以及恢复失败的错误收敛。
- 测试批次中途异常、临时节点恢复和临时 Job 关闭均执行。
- 测试 Join/Merge 等无法重建状态的场景安全失败。
- 运行 Engine、TM Common、TM 定向测试和相关模块构建；确认 `git diff --check` 通过。
