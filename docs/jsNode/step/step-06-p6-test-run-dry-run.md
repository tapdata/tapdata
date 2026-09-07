# TAP-12832 开发步骤总结：试运行参数传递与强制 dry-run

## 本步骤完成内容

- `TestRunDto` 增加 `scriptParams`，试运行请求可以携带未保存的通用参数列表。
- `TaskNodeServiceImpl.testRunJsNode` 和 `testRunJsNodeRPC`：
  - 普通 JS 与迁移 JS 分支都把 DTO 参数写入测试 DAG 中对应脚本节点。
  - 只有参数非 null 时覆盖临时 DAG，旧试运行请求保持原行为。
- `FileScriptExecutor` 增加 `forceDryRun` 构造选项；当任务不是正式运行任务时，`dryRun=false` 也会被 Java facade 覆盖为 true。
- `HazelcastJavaScriptProcessorNode` 创建文件 facade 时根据当前任务类型传入强制 dry-run 标志，避免试运行脚本误写目标 FTP。

## 测试与自审

新增/更新测试：

- `TestRunDtoTest`：验证 DTO 可携带 `scriptParams`。
- `FileScriptExecutorTest`：验证强制 dry-run 覆盖脚本显式 `false`，共享 service 收到的 request 为 dry-run。

执行结果：

- 先运行强制 dry-run 测试，因 facade 没有三参数构造器而编译失败，确认红灯。
- `mvn -q -Dtest=TestRunDtoTest,JsNodeConfigParamTest test`（`manager/tm-common`）：通过。
- `mvn -q -Dtest=FileScriptExecutorTest,JsNodeConfigAccessorTest test`（`iengine-common`）：通过。
- `TaskNodeServiceImplTest` 尝试执行时被当前工作区既有 SSO 源码缺失阻塞（`com.tapdata.tm.sso.dto/entity/service` 多个类无法解析），错误不来自本次试运行改造。

## 当前边界

- dry-run 目前由 facade 强制请求标志实现；共享 service 的 dry-run 路径只读取源和检查参数，不执行目标写入/rename。
- JS 试运行线程仍使用现有 10 秒引擎总超时；后续需要把该总超时与文件 request `timeoutMs`、取消和 socket 关闭统一起来。
- 试运行仍通过完整测试任务和 RPC 执行，参数字段只进入临时测试 DAG，不回写正式任务。
- 本步骤没有放宽加密参数权限：非正式任务不会创建 AES secret resolver，读取加密 key 仍返回 `JS_NODE_CONFIG_SECRET_FORBIDDEN`。

## 下一步

执行 P7：在前端 JS 节点表单增加通用参数编辑器、加密掩码、模板和试运行请求中的 `scriptParams` 字段；同时补充后端字段级校验错误展示。
