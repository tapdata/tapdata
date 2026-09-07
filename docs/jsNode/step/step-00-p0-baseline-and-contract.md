# TAP-12832 开发步骤总结：P0 基线与契约冻结

## 本步骤完成内容

- 确认四个代码仓库当前分支和工作区状态均干净：`tapdata`、`tapdata-web`、`tapdata-connectors`、`tapdata-common-lib`。
- 确认实现边界：公共文件操作 API 放在 `tapdata-api`，共享实现放在 `file-connector-core`，协议行为继续由 `file-storages/*` 提供，引擎只依赖接口和 JS facade。
- 冻结持久化字段名为 `scriptParams`，脚本运行时参数访问器名称为 `jsNodeConfig`，文件 facade 名称为 `ftp`。
- 冻结一期 JS API：`copy`、`copyByConfig`、`copyBatch`、`exists`；不开放删除源文件的脚本 API。
- 冻结一期状态：`COPIED`、`REUSED`、`DRY_RUN`；文件能力启用时关闭 JS 节点记录级并发。
- 确认试运行必须使用 dry-run，不允许向目标 FTP 写入。

## 代码基线核对

| 位置 | 已确认事实 |
| --- | --- |
| `HazelcastJavaScriptProcessorNode` | 每线程创建 JS engine，当前通过 `ScriptEngine.put` 注入宿主对象；`supportConcurrentProcess()` 当前返回 `true` |
| `ScriptUtil` | GraalJS 已禁止 `File`、`Runtime`、`ProcessBuilder`、`ClassLoader` 等高风险宿主能力 |
| `ScriptProcessNode` | 当前只有 `script`、`declareScript`、`jsType`，需要增加 `scriptParams` |
| `FileConnector` | 已使用 `FileConfig -> FileProtocolEnum -> TapFileStorageBuilder -> TapFileStorage` 链路 |
| `FtpFileStorage` | 已有连接、读写和目录扫描，但 `move` 未实现，`storeFile`/`completePendingCommand` 结果检查不完整 |
| `JSProcessNodeTestRunService` | 通过完整测试任务运行脚本，需要在相同任务参数中传递 `scriptParams` 并注入 dry-run facade |

## 本步骤验收

- [x] 四个仓库分支和工作区状态已核对。
- [x] 开发计划中的模块依赖顺序已确定。
- [x] 持久化字段与运行时注入对象名称已区分。
- [x] 后续实现将遵循“先测试、再生产代码、再回归验证”的开发流程。

## 下一步

执行 P1：先为 `tapdata-api` 的文件操作 DTO、错误模型和 provider 编写失败测试，再实现最小公共 API。
