# TAP-12832 开发步骤总结：P2 存储接口与 FTP 适配器

## 本步骤完成内容

### 公共存储接口

- 在 `TapFileStorage` 增加 `capabilities()` 默认方法，默认返回空能力集合。
- 在 `TapFileStorage` 增加 `makeDirectory(path)` 默认方法，旧 adapter 不实现时返回 `false`。
- 使用 `FileStorageCapability` 表达 `ATOMIC_RENAME`、`MAKE_DIRECTORY`、`CHECKSUM`、`APPEND`。

### FTP adapter

- `FtpFileStorage.capabilities()` 声明 `ATOMIC_RENAME` 和 `MAKE_DIRECTORY`。
- `saveFile` 检查 `changeWorkingDirectory` 和 `storeFile` 返回值，失败抛 `IOException`，不返回伪成功。
- `move` 使用 FTP `rename`，并保留协议返回值。
- `makeDirectory` 使用 FTP `makeDirectory`，目录已存在时返回成功。
- `openFileOutputStream` 关闭时检查 `completePendingCommand`，失败抛 `IOException`。
- FTP 模块显式依赖本地 `tapdata-api:2.0.9-SNAPSHOT`，确保 connector 使用新增公共接口。

## 测试与自审

新增 `FtpFileStorageContractTest`，通过反射注入测试 FTP client，覆盖：

- capability 声明。
- `storeFile=false` 时写入失败。
- rename 参数和返回值。

执行结果：

- 先运行失败测试，确认 capability 为空、`move` 抛 `UnsupportedOperationException`、`storeFile=false` 未抛异常。
- 实现后 FTP 定向测试通过。
- `tapdata-api` 完整回归通过；现有测试会输出大量历史日志，但 Maven 退出码为 0。

## 边界说明

- SFTP、SMB 等 adapter 当前没有强行声明 rename capability；共享服务遇到未声明能力时必须明确返回 unsupported，不能直接写正式路径。
- 本步骤没有实现共享 copy service；临时文件、校验、重试和 session 管理在 P3 完成。

## 下一步

执行 P3：在 `file-connector-core` 实现参数映射、session 管理、路径策略、流式 copy、校验、重试和批量服务，并让 `FileConnector/FileTest` 复用这条链路。
