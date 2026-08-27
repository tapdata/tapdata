# E10 Engine 回放回归

## 1. 步骤结论

E10 已完成（待集成验证）。本步骤新增跨边界回归矩阵，串联 E01-E09 的消息处理、快照重建、协调器、source boundary、屏障、捕获守卫和失败补偿契约。5 项测试全部通过，证明当前 Engine 侧在单元/契约层面能够保持服务端顺序、暂停任务状态和 source gate 生命周期。

Engine 进程重启本身不在单进程单元测试中伪造；E09 已明确进程终止后的责任边界由 TM 批次超时扫描、事件锁释放和批次补偿承接，真实重启故障注入留给 G08 联调验收。

## 2. 回归矩阵

| 场景 | 验证内容 | 结果 |
| --- | --- | --- |
| 运行中任务 I/U/D | 三种 DML 从 live source boundary 注入，按服务端 `orderedEventIds` 顺序执行；普通 sink 不被调用 | 通过 |
| 暂停任务 | recovery-only runner 只发送恢复事件，任务仍为 `STOP`，不启动普通 source reader/normal sink | 通过 |
| 事件结果策略 | 按顺序验证 `SUCCESS`、`FAILED`、`TIMEOUT`，continue 策略下仍保持后续事件顺序 | 通过 |
| 停止与恢复 | stop 策略在首个失败后停止后续注入，并恢复 `DqlSourceReadGate.OPEN`；普通事件恢复放行 | 通过 |
| 重复消息 | 不同 Handler 实例共享 batch registry，重复批次只启动一个 coordinator | 通过 |

运行中回放测试同时断言恢复流程只产生 `EVENT_STARTED`、`EVENT_RESULT` 和批次终态回报，不通过普通 DQL 上报器创建新的 DQL 主记录。E08 的目标/处理节点捕获测试进一步验证恢复失败只回传原 attempt。

## 3. 代码产出

新增：

- `iengine/iengine-app/src/test/java/io/tapdata/dql/recovery/DqlRecoveryReplayRegressionTest.java`

本步骤没有新增生产代码；E09 的 source 生命周期钩子和失败补偿已足以支撑矩阵中的停止、恢复和回调异常路径。

## 4. 验证结果

E10 定向命令：

```bash
mvn -o -pl iengine/iengine-app -am \
  -Dtest=DqlRecoveryReplayRegressionTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

结果：`DqlRecoveryReplayRegressionTest` 5/5 通过，`iengine-app` 及依赖模块编译成功，`BUILD SUCCESS`。

E08/C12 相关回归中，C12 3/3 和恢复捕获 2/2 通过。既有 `SkipErrorEventAspectTaskTest` 的日志测试夹具和线程池 spy 问题，以及 `DataSyncEventHandlerTest` 的 Mockito static mock 配置限制，均未由 E10 引入，详见 E08/E09 记录。

## 5. 里程碑判断与后续依赖

M4 的 Engine 回放实现和自动化回归证据已具备，状态更新为“已完成（待集成验证）”。仍需 G08 在真实 TM/Engine 环境验证 Engine 重启、重复回调、回放超时、source gate 恢复和任务 start/stop/reset/delete 消息兼容；该验证不能由本地单元测试替代。
