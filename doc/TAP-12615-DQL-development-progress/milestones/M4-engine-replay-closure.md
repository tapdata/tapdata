# M4 Engine 回放闭环

## 1. 里程碑结论

M4 已完成（待集成验证）。E01-E10 已形成从 TM `dqlRecovery` 消息接收，到安全 Payload 快照重建、源边界注入、逐事件屏障、结果回调、恢复防递归捕获和失败补偿的 Engine 回放闭环。

## 2. 能力证据

| 能力 | 对应步骤 | 当前证据 |
| --- | --- | --- |
| 消息校验与批次幂等 | E01、E10 | 任务/版本/Agent/顺序校验；跨 Handler 重复消息只启动一次 |
| I/U/D 安全重建 | E02、E10 | `tap-record-event-json-v1` 快照重建测试及运行中 I/U/D 回归 |
| 串行回放 | E03、E07、E10 | 每个事件独立 attempt；完成屏障未确定前不注入下一事件 |
| 运行中任务 | E04、E06、E10 | source gate 暂停普通读取，恢复事件从 source boundary 进入 |
| 暂停任务 | E05、E10 | recovery-only runner 不启动普通 source reader，任务状态保持 `STOP` |
| 防递归捕获 | E08、E10 | recovery 标记失败回传原 attempt，不创建新的 DQL 主记录 |
| 失败补偿 | E09、E10 | 逆序清理、BATCH_FAILED best effort、TM 超时兜底 |

## 3. 退出条件判断

- 运行中和暂停任务的 Engine 回放路径均有自动化证据。
- 服务端事件顺序、I/U/D 类型、失败继续/停止、超时结果和 Gate 恢复均有覆盖。
- 已知的 Engine 进程重启、真实 TM 回调和现有任务控制消息兼容性，需要在 G08/G10 的联调环境验证。

因此本地代码阶段可关闭 M4，但不将单元/契约测试等同于真实集群验收。

## 4. 后续依赖

M5 的告警、配置、权限和初始化仍需完成；M6 的 TM/Engine 联调、故障注入、POC 证据和发布前检查仍未开始。
