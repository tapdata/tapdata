# TAP-12832：增强 JS 节点通过 `storage` 操作文件连接概要设计

> Jira：[TAP-12832](https://tapdata.atlassian.net/browse/TAP-12832)
>
> 本文档与当前提交代码保持一致；详细设计见同目录 `TAP-12832-ftp-storage-detailed-design.md`。

## 1. 方案结论

用户先在连接管理中创建 FTP 文件连接，enhanced JS 脚本按连接名称调用注入的 `storage` 对象。FTP 连接不要求出现在当前任务 DAG 中，脚本可以按事件逻辑写文件，也可以把 source 文件连接的文件复制到 target 文件连接。

本方案复用现有 `TapFileStorage`，不新增 `tapdata-common-lib` 文件操作公共 API，不新增 `FileStorageFunction` 或 `TapFileStorage.capabilities()`。

```text
storage.update("target-ftp", data, options)
  -> StorageFacade
  -> StorageExecutorsManager
  -> PdkStorageExecutor
  -> TapFileStorage 既有方法
  -> FTP/SFTP/SMB/S3FS/OSS/Local/NFS 实现
```

数据库脚本仍走原有链路：

```text
ScriptExecutorsManager.getScriptExecutor("mongo-test").aggregate(query)
  -> ScriptExecutor
  -> ExecuteCommandFunction
```

所以 `ScriptExecutorsManager.getScriptExecutor("target-ftp")` 不能直接提供 `readFile/saveFile/delete`。FTP 文件操作必须使用 `storage`。

## 2. 本期能力

| API | 作用 |
| --- | --- |
| `storage.update(name, data, options)` | `write` 写文件，`copy` 从 source 连接复制到 target 连接 |
| `storage.find(name, {path}, options)` | 查询文件元数据 |
| `storage.exists(name, path)` | 判断文件是否存在 |
| `storage.delete(name, {path}, options)` | 删除文件 |

`overwrite` 支持 `skip`、`overwrite`、`fail`，默认 `skip`。返回值是普通 Map，写入/复制会返回 `status`、`targetPath`，若底层返回元数据则包含 `bytes`。

示例：

```javascript
function process(record) {
  storage.update("target-ftp", {
    action: "write",
    target: { path: "out/" + record.id + ".json" },
    content: JSON.stringify(record)
  }, { overwrite: "overwrite" });
  return record;
}
```

FTP 到 FTP：

```javascript
function process(record) {
  if (record.type !== "ready") return record;
  return storage.update("target-ftp", {
    action: "copy",
    source: { connection: "source-ftp", path: "in/a.csv" },
    target: { path: "archive/a.csv" }
  }, { overwrite: "skip" });
}
```

## 3. 连接创建与复用

`StorageExecutorsManager` 的缓存作用域是一个 JS processor 实例，缓存键是 trim 后的连接名称：

```text
connectionName -> CompletableFuture<StorageExecutor>
```

首次访问时按连接名查询 `Connections`，用 `ConnectionUtil.getDatabaseType` 和 `PdkUtil.createNode` 获取对应 PDK classloader，再用现有 `TapFileStorageBuilder` 创建一个 storage。`build()` 会执行 storage 的实际 `init(params)`，因此是实际连接初始化点。

同一连接并发首次访问共享一个 future，只有一个调用执行创建；后续事件复用同一个 executor，不会每条事件重新创建 PDK/FTP 连接。远端 I/O 异常会使 executor 失效并关闭，下一次调用重新创建。JS processor 关闭时关闭所有 executor、storage 和 PDK 运行时资源。

`PdkUtil.createNode` 在这条路径中只用于加载 PDK/classloader，不执行 `connectorInit`，也不获取或注册文件能力函数，因此一个 executor 只持有一个实际 `TapFileStorage`。

## 4. 协议范围

当前引擎内部已有协议映射：

| protocol | storage 实现 |
| --- | --- |
| `local` | `LocalFileStorage` |
| `ftp` | `FtpFileStorage` |
| `sftp` | `SftpFileStorage` |
| `smb` | `SmbFileStorage` |
| `s3fs` | `S3fsFileStorage` |
| `nfs` | `NfsFileStorage` |
| `oss` | `OssFileStorage` |

TAP-12832 首先验收 FTP。其他协议是否可用还取决于对应 PDK 是否包含 storage class、配置是否完整和连接器实现是否通过真实环境验证；不能仅因存在映射就宣称全部协议已完成生产验收。

## 5. 文件连接器资源修复

本需求同时修复了已经从源码确认的资源问题：

- FTP 初始化失败清理半初始化 client，`destroy` 可重复调用；受管输入/输出流关闭时完成 FTP pending command 并释放 I/O 边界。
- SFTP 初始化失败关闭 channel/session，输入/输出流关闭时释放操作锁。
- FileConnector 的既有停止清理保持有效，writer、merge executor、storage 分别进入清理路径。
- Local、SMB 的 `saveFile` 输出流改为异常安全关闭。
- OSS、S3FS 的对象流关闭，存在性判断返回真实结果，不再使用 `InputStream.available()` 作为远端文件总长度。

这些修复属于文件连接器/存储实现自身，不要求新增 common-lib operation API。

## 6. 明确不做

- 不通过 `ScriptExecutorsManager.getScriptExecutor("target-ftp")` 执行文件 API。
- 不在 JS 节点为每条 Tapdata 事件写 `file_operation_ledger`。
- 不自动实现业务幂等、业务重试、跨重启恢复或“临时文件后发布”的原子语义。
- 不把 PDK 节点、FTP client、凭据或 Java stream 暴露给 JS。
- 不为本需求新增公共 DTO、能力枚举、错误码或 `FileStorageFunction`。

缓存、失效和资源释放属于连接运行时管理，不是业务幂等。是否重复调用、失败后如何处理由用户 JS 或上层任务错误策略决定。
