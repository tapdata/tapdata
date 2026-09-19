# TAP-12832 文件连接 JS 操作研发计划（调整版）

## 1. 目标和调整结论

目标是在增强 JS 节点的 `process(record)` 中，允许用户按连接名称调用文件操作：

- 写入当前连接的文件；
- 从一个文件连接读取文件并写入另一个文件连接；
- 查询文件、判断文件存在、删除文件；
- 连接不要求出现在当前任务 DAG 中。

本次按代码分析后的关键调整如下：

1. 不修改 `tapdata-common-lib` 的公共文件 API，不新增文件操作 DTO、能力枚举、错误码、`TapFileStorage.capabilities()` 或 `FileStorageFunction`。
2. 不在文件 Connector 的 `ConnectorFunctions` 中注册新的文件能力。
3. 引擎直接复用既有 `TapFileStorage` 方法：`getFile`、`readFile`、`isFileExist`、`saveFile`、`delete`、`move`。
4. 引擎内部新增的只是 `StorageFacade`、`StorageExecutor`、`StorageExecutorsManager` 和引擎本地异常；这些不是 Connector/PDM 公共 API。
5. 按连接名缓存一个 `StorageExecutor`。事件级调用只做缓存命中，不会每条事件重新创建 PDK 或 FTP 连接。
6. 不增加 `file_operation_ledger`，不在 JS 节点维护跨事件幂等、任务状态或业务校验。

## 2. 代码依据和最终模块边界

| 模块 | 最终职责 | 本次状态 |
| --- | --- | --- |
| `tapdata-common-lib` | 保持既有 `TapFileStorage`/`TapFileStorageBuilder` API | 不新增 TAP-12832 公共 API；已撤销本需求之前增加的 operation API |
| `tapdata-connectors/file-storages` | 提供 FTP、SFTP、SMB、S3FS、OSS、Local 等 `TapFileStorage` 实现 | 保留既有 FTP/SFTP 生命周期修复；补充实际发现的 I/O 资源修复 |
| `connectors-common/file-connector-core` | 文件 Connector 自身的 storage 生命周期和读写 | 移除本需求新增的 `FileStorageFunction` 注册，不改变原有 Connector 读取/写入能力 |
| `iengine-app` | JS 文件操作门面、连接按名解析、缓存、PDK classloader 获取、生命周期 | 已实现 |
| `iengine-common` | 既有引擎公共代码 | 移除不再使用的 operation service/session manager 实现 |
| `docs/jsNode` | 设计、研发计划、验收说明 | 本文档同步最终实现 |

关键源码：

- [StorageFacade.java](/Users/gavinxiao/kit/tapdata/tapdata/iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/script/storage/StorageFacade.java)
- [StorageExecutorsManager.java](/Users/gavinxiao/kit/tapdata/tapdata/iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/script/storage/StorageExecutorsManager.java)
- [PdkStorageExecutor.java](/Users/gavinxiao/kit/tapdata/tapdata/iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/script/storage/PdkStorageExecutor.java)
- [TapFileStorage.java](/Users/gavinxiao/kit/tapdata/tapdata-common-lib/plugin-kit/tapdata-api/src/main/java/io/tapdata/file/TapFileStorage.java)
- [FtpFileStorage.java](/Users/gavinxiao/kit/tapdata/tapdata-connectors/file-storages/ftp-file/src/main/java/io/tapdata/storage/ftp/FtpFileStorage.java)

## 3. 研发任务拆分和完成情况

| 步骤 | 任务 | 主要内容 | 交付状态/提交 |
| --- | --- | --- | --- |
| 1 | 现有能力和问题核对 | 核对 `TapFileStorage`、`TapFileStorageBuilder`、FTP/SFTP 实现、JS 注入和缓存边界；确认不依赖 DAG 节点 | 已完成，依据形成于详细设计 |
| 2 | 引擎内部文件执行器 | 引入 `StorageExecutor`，直接持有 `TapFileStorage`，提供 root path 解析 | 已完成，`ceffa91a1f` |
| 3 | 按连接缓存和生命周期 | 连接名解析、并发首次访问单飞、缓存、关闭、失效和创建失败退避 | 已完成，`ceffa91a1f` |
| 4 | JS 文件操作门面 | 实现 write/copy/find/exists/delete；跨存储流式复制；同存储临时文件复制 | 已完成，`ceffa91a1f` |
| 5 | PDK 建连路径调整 | PDK 只用于获得 Connector classloader；由 `TapFileStorageBuilder` 创建并初始化一个 storage 实例；不调用 `FileStorageFunction` | 已完成，`ceffa91a1f` |
| 6 | 文件 Connector 资源修复 | 保留 FTP/SFTP 受管 stream 和连接清理修复；补充 Local/SMB 输出流、OSS/S3 对象流关闭和存在性判断 | 已完成，`a9d8db19`、`44d1dfaa` |
| 7 | 移除错误的公共 API方案 | 清理 `FileStorageFunction` 注册、`capabilities()` 和 operation DTO/service/session manager | 已完成，`a9d8db19`、`ae88d80` |
| 8 | 单元测试和编译验证 | 缓存复用、并发单飞、失效重建、写入、跨存储复制、同存储复制、查询/删除 | 已完成，11 项测试通过 |
| 9 | 真实环境验收 | 使用 FTP/SFTP 测试环境验证权限、断连、Unicode 路径、大文件和吞吐 | 待验收环境执行，不在本地模拟结果中宣称完成 |

## 4. 最终技术方案

### 4.1 调用链

```text
JS process(record)
    -> storage.update/find/exists/delete
    -> StorageFacade
    -> StorageExecutorsManager.getStorageExecutor(connectionName)
    -> 缓存的 StorageExecutor
    -> TapFileStorage 既有方法
    -> FTP/SFTP/SMB/S3FS/OSS/Local 实现
```

数据库聚合调用保持原链路：

```text
ScriptExecutorsManager.getScriptExecutor("mongo-test").aggregate(...)
    -> ScriptExecutor
    -> ExecuteCommandFunction
```

`ScriptExecutorsManager.getScriptExecutor("target-ftp")` 仍然只代表数据库命令执行器。它不会因为连接协议是 FTP 就自动获得文件 API；文件 API 必须使用注入的 `storage` 对象。

### 4.2 连接创建和缓存策略

`StorageExecutorsManager` 以当前 JS processor 实例为作用域，缓存：

```text
trim(connectionName) -> CompletableFuture<StorageExecutor>
```

第一次访问的流程：

1. 按连接名称查询 `Connections`。
2. 使用 `ConnectionUtil.getDatabaseType` 确认 PDK 类型。
3. 使用 `PdkUtil.createNode` 加载 PDK，并取得 `connectorNode.getConnectorClassLoader()`。
4. 按协议映射选择 storage class。
5. 通过现有 `TapFileStorageBuilder.withClassLoader(...).withParams(...).build()` 创建实例。`build()` 会反射 storage class 并调用 `storage.init(params)`，这一处才是文件连接真正初始化的位置。
6. 将该实例包装为 `PdkManagedFileStorage`，放入缓存。

`PdkUtil.createNode` 在该流程中不调用 `connectorInit`，也不获取 `FileStorageFunction`，因此不会额外创建一个已经初始化但不使用的文件 Connector storage。每个连接 executor 只创建一个实际 `TapFileStorage`。

并发和异常规则：

- 同一连接并发首次访问共享同一个 `CompletableFuture`，只允许一个工厂调用；
- 创建成功后后续事件直接命中缓存；
- 创建失败移除 future，并在短退避窗口内返回同一失败，避免故障时高频建连；
- 文件 I/O 抛出远端异常时使对应 executor 失效并关闭，后续事件重新创建；
- JS 节点关闭时关闭全部 executor，storage 的 `destroy()`、PDK associateId、state map、table map 都会清理；
- `StorageExecutorsManagerTest.repeatedEventLevelAccessReusesOneExecutor` 验证 100 次事件级访问只创建 1 个 executor。

### 4.3 协议映射

引擎当前按文件连接配置的 `protocol` 或 `file_source_protocol` 选择已有 storage class：

| protocol | storage class |
| --- | --- |
| `local` | `io.tapdata.storage.local.LocalFileStorage` |
| `ftp` | `io.tapdata.storage.ftp.FtpFileStorage` |
| `sftp` | `io.tapdata.storage.sftp.SftpFileStorage` |
| `smb` | `io.tapdata.storage.smb.SmbFileStorage` |
| `s3fs` | `io.tapdata.storage.s3fs.S3fsFileStorage` |
| `nfs` | `io.tapdata.storage.nfs.NfsFileStorage` |
| `oss` | `io.tapdata.storage.oss.OssFileStorage` |

因此实现不是只能操作 FTP；FTP 是 TAP-12832 的首要验收协议，其他协议能否成功还取决于对应 PDK 中是否包含 class、连接参数是否正确以及该实现自身的能力。新增协议只需要在引擎映射和对应 storage 实现验证后再开放，不需要新增 common-lib operation API。

### 4.4 路径解析

连接配置中的 `rootPath` 优先于 `filePathString`。脚本传入的路径会：

- 统一 `/`；
- 去掉开头 `/` 后拼到 root path；
- 拒绝 `..`、控制字符和 `://`；
- root 为空时按 `/` 作为 storage 路径根。

这只是防止把协议 URL/越界路径直接交给 storage，不是对用户 JS 业务逻辑做额外校验，也不维护任何事件状态。

## 5. JS 调用契约

### 5.1 写文件

```javascript
function process(record) {
  return storage.update("target-ftp", {
    action: "write",
    target: { path: "out/" + record.id + ".json" },
    content: JSON.stringify(record)
  }, { overwrite: "overwrite" });
}
```

`content` 支持字符串和 `byte[]` 形态；本次实现不把大文件 Java stream 暴露给 JS。

### 5.2 FTP 到另一个 FTP 复制

```javascript
function process(record) {
  if (record.type !== "ready") return record;
  storage.update("target-ftp", {
    action: "copy",
    source: { connection: "source-ftp", path: "in/a.csv" },
    target: { path: "archive/a.csv" }
  }, { overwrite: "skip" });
  return record;
}
```

跨 storage 使用 `source.readFile(path, consumer)` 将输入流直接交给目标 `saveFile`；同一个 storage 实例内复制会先写本地临时文件，再读取临时文件写回目标，避免 FTP/SFTP 单连接读锁和写锁相互等待。

### 5.3 查询、存在性、删除

```javascript
var metadata = storage.find("target-ftp", { path: "out/a.json" }, null);
var exists = storage.exists("target-ftp", "out/a.json");
var removed = storage.delete("target-ftp", { path: "out/a.json" }, null);
```

### 5.4 覆盖策略和返回值

`options.overwrite`：

- `skip`（默认）：目标存在时不写，返回 `status: "reused"`；
- `overwrite`：覆盖目标，返回 `status: "written"` 或 `"copied"`；
- `fail`：目标存在时抛出引擎本地 `StorageOperationException`。

实现不提供公共 operation DTO，也不承诺旧设计中的 `retryTimes`、`verify`、`dryRun`、checksum、批量复制或临时文件发布字段。

## 6. 文件连接器代码优化和资源修复

以下修复来自实际源码，而非推测：

### 6.1 FTP

- `init` 对 connect/login/编码/模式/二进制类型失败做 partial client 清理；
- `destroy` 处理 logout/disconnect，且可重复调用；
- `readFile(path)` 返回受管输入流，关闭时完成一次 `completePendingCommand` 并释放串行 I/O 锁；
- `openFileOutputStream` 的 close 幂等，并在关闭时完成 pending command；
- storage 内部用串行边界保护 FTPClient 的有状态操作；
- FTPS 配置未被伪装成普通 FTP，仍按现有实现边界处理。

### 6.2 SFTP 和 Connector 生命周期

- SFTP 输入/输出流关闭时释放操作锁；初始化失败会断开 channel/session；
- FileConnector 的停止流程保证 writer、merge executor、storage 分别进入清理路径；
- CSV/JSON/Excel discovery 的 storage 在异常和提前返回路径释放；
- 文件 Connector 的既有生命周期修复保留，不因移除 `FileStorageFunction` 注册而回退。

### 6.3 Local/SMB/OSS/S3FS 本次补充

- Local、SMB 的 `saveFile` 输出流改为 try-with-resources，读入异常时也能关闭输出流；
- OSS、S3FS `getFile` 查询对象元数据后关闭对象流，避免只查询元数据却留下远端对象流；
- OSS、S3FS `isFileExist` 返回实际存在性，不再“调用成功即固定返回 true”；
- OSS、S3FS `saveFile` 移除 `InputStream.available()` 作为远端文件长度的错误依据，避免 FTP/SFTP 流式复制时因 available 为 0 或部分可用而写出错误大小。

## 7. 明确不做的事情

- 不在 JS 节点为每条 Tapdata 事件写 Mongo `file_operation_ledger`；
- 不在 Java 层猜测用户脚本是否应该执行操作；脚本决定是否调用、调用哪个连接和路径；
- 不把幂等、业务重试、任务发布、跨重启恢复落到本功能；
- 不通过 `ScriptExecutorsManager.getScriptExecutor("target-ftp")` 暴露 FTP 文件操作；
- 不新增 `tapdata-common-lib` 公共 API；
- 不把 PDK/FTPClient/凭据/原始 Java stream 暴露给 JS。

本功能中的“缓存、失效、资源释放”是连接运行时管理，不是业务幂等。一次事件失败后是否跳过、重试业务逻辑或记录结果，由用户脚本和任务错误处理机制决定。

## 8. 验证和验收

### 8.1 已完成的自动化验证

```text
StorageExecutorsManagerTest: 6 passed
StorageFacadeTest: 5 passed
总计：11 passed, 0 failed
```

验证内容包括：

- 并发首次访问只创建一个 executor；
- 100 次事件级访问复用同一个 executor；
- executor 失效后允许重建；
- manager 关闭后关闭 executor 并拒绝新访问；
- write、find、exists、delete；
- 不同 storage 实例之间复制；
- 同一 storage 实例内复制不会持有读锁写入；
- overwrite 冲突和不支持 action 的异常传播。

已验证编译：

- 引擎 `mvn -o -pl iengine/iengine-app -am ... test`；
- 文件 storage 和文件 Connector 相关模块离线 compile；
- `tapdata-api` 恢复后离线 compile。

### 8.2 真实环境验收场景

1. 创建 FTP 连接但不把 FTP 节点放入 DAG，JS 中执行 write/find/exists/delete。
2. 两个 FTP 连接不在 DAG 中，按事件条件执行 FTP 到 FTP copy。
3. 同一连接连续处理 1000 条事件，观察 FTP 登录/PDK 创建次数应接近连接数，而不是事件数。
4. 目标存在时分别验证 skip、overwrite、fail。
5. 复制大文件，确认内存不会随文件大小线性增长，输入/输出流最终关闭。
6. 复制过程中断开控制连接，确认当前调用抛错、executor 被关闭，下一次调用可以新建连接。
7. 验证 Unicode、空格和 rootPath 下的相对路径。
8. 任务停止或 JS 节点异常退出后确认 FTP 控制连接、PDK associateId 和临时文件释放。
9. 运行既有 Mongo aggregate JS，确认 `ScriptExecutorsManager` 行为不变。

真实 FTP 服务、权限、断连和吞吐属于部署环境验收，不以本地 fake storage 单测代替。
