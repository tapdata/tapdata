# TAP-12832 开发步骤总结：P5 引擎配置访问器与文件 facade

## 本步骤完成内容

- 在 `iengine-common` 增加 `JsNodeConfigAccessor`、`DefaultJsNodeConfigAccessor` 和 `JsNodeConfigScriptFacade`。
  - `get`、`has`、`getOrDefault` 只按精确 key 访问。
  - 缺失 key、无 secret 权限、解密失败和类型错误返回稳定 code。
  - GraalJS 只拿到窄 facade，不暴露参数 Map、导出接口、反射或 secret resolver。
- 增加 `Aes256JsNodeConfigSecretResolver`，复用 engine 已有任务配置解密格式；解密后的值只在当前运行时内存中使用。
- 增加 `FileScriptExecutor`：
  - `copy(Map)`、`copyByConfig(Map)`、`copyBatch(List)`、`exists(Map)`。
  - `copyByConfig` 根据 `sourcePrefix`/`targetPrefix` 在 Java 侧读取配置，脚本不需要遍历或导出全部参数。
  - 返回只包含状态、路径、字节数、校验摘要、重试次数和耗时等安全字段。
  - 所有实际操作委托 `TapFileOperationService`，不接触 `FTPClient`、`TapFileStorage` 或输入流。
- 在 `file-connector-core` 增加 `DefaultFileOperationServiceProvider` 和 ServiceLoader 资源，engine 通过 API provider 复用 connector 侧共享服务。
- `HazelcastJavaScriptProcessorNode`：
  - 初始化时读取普通/迁移 JS 节点的 `scriptParams`。
  - 注入 `jsNodeConfig`；检测到 endpoint protocol 参数时加载共享服务并注入 `ftp`。
  - 文件操作节点关闭记录级并发，关闭时释放 facade/service。
  - 文件错误包装时保留 `FileOperationErrorCode`，不把参数和密码写入 JS 错误消息。

## 测试与自审

新增测试：

- `JsNodeConfigAccessorTest`：精确 key、默认值、JSON 转换、secret 权限和 resolver。
- `FileScriptExecutorTest`：显式 endpoint、prefix endpoint、结果脱敏和共享 API 委托。
- `FileOperationServiceLoaderTest`：缺少 provider 返回 `FILE_SERVICE_UNAVAILABLE`。
- `DefaultFileOperationServiceProviderTest`：connector provider 可被 ServiceLoader 发现并只暴露公共 service API。

执行结果：

- 先运行 facade 测试，因 `FileScriptExecutor` 不存在而失败，确认红灯。
- `iengine-common` 定向测试：通过。
- `iengine-common` 完整 `mvn -q test`：通过。
- `file-connector-core` provider 与 P3 回归测试：通过。
- `iengine-app` 编译已进入新增代码之后，剩余失败来自当前工作区既有 API/模块版本不一致（`ObsLogger.alert`、ShareCDC 构造器和错误码），新增 JS 注入代码未产生编译错误；需在全仓依赖版本统一后再做最终模块编译门禁。

## 当前边界

- provider 依赖运行时 connector classloader 提供 `file-connector-core` ServiceLoader 资源；缺失时在节点初始化返回明确不可用错误。
- 当前 AES resolver 复用平台既有 AES 配置格式；后续可替换为统一 secret service，不改变 `JsNodeConfigSecretResolver` 接口。
- 试运行任务目前不给加密参数 resolver，避免调试脚本直接获取明文；dry-run service 和试运行请求改造属于 P6。
- facade 的 `copyBatch` 顺序执行并受共享 service 的 100 文件上限约束，不提供跨文件分布式回滚。

## 下一步

执行 P6：把 `scriptParams` 传入 JS test-run，接入 dry-run 文件 service、统一超时和取消清理，确保试运行不会写入目标 FTP。
