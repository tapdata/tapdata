# TAP-12832 开发步骤总结：P3 共享文件服务

## 本步骤完成内容

在 `connectors-common/file-connector-core` 增加共享文件操作链路：

- `FilePathPolicy`：规范化相对路径、拒绝路径越界、协议 URL 和控制字符。
- `FileServiceConfigMapper` / `DefaultFileServiceConfigMapper`：把通用 `host/port/username/password` 映射为 FTP/SFTP 等 adapter 使用的协议字段，同时保留已有字段。
- `FileStorageSession`、`FileStorageSessionManager`、`DefaultFileStorageSessionManager`：按线程、协议、rootPath 和参数指纹复用 storage，支持 retain/release/invalidate/close。
- `FileStorageFactory`：集中复用 `FileProtocolEnum -> TapFileStorageBuilder -> TapFileStorage` 装配链。
- `DefaultFileOperationService`：实现单文件 copy、临时路径、原子 rename、size 校验、目标 `REUSED`、冲突检查、dry-run、batch、stat、exists、list 和 validate。
- `FileConnector.buildStorage()` 和 `FileTest` 改为调用 `FileStorageFactory`，数据源侧与 JS 侧不再各自维护 storage builder。
- `file-connector-core` 显式依赖本地 `tapdata-api:2.0.9-SNAPSHOT`，使用 P1/P2 新增公共接口。

## 测试与自审

新增测试：

- `FilePathPolicyTest`：路径规范化、路径穿越和 rootPath 拼接。
- `DefaultFileOperationServiceTest`：复制发布、目标复用、目标冲突和 dry-run 不写入。
- `FileServiceConfigMapperTest`：通用 endpoint 字段映射为 FTP storage 字段。
- `FileStorageFactoryTest`：未知协议在 builder 前返回 `FILE_CONFIG_INVALID`。

执行结果：

- 先运行失败测试，确认共享 service、path policy 和 factory 类不存在。
- 实现后运行四个定向测试：通过。
- 运行 `file-connector-core` 完整 `mvn -q test`：通过。

## 当前边界

- 当前 service 已使用 size 校验；checksum、有限重试和更严格的总超时在后续引擎接入前继续补齐。
- `copyBatch` 已限制 100 个文件，但跨文件不提供分布式回滚。
- session 以线程为隔离单位，release 后保留可复用 session，close/invalidate 时销毁。
- 未声明 `ATOMIC_RENAME` 的协议会被 service 拒绝，避免半成品写入正式路径。

## 下一步

执行 P4：增加 `ScriptProcessNode.scriptParams`、参数 DTO、节点校验、密文/secret 权限和导入导出兼容测试。
