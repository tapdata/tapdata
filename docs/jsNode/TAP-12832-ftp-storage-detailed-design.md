# TAP-12832：JS 节点文件连接操作详细设计（最终落地版）

## 1. 设计结论

TAP-12832 的目标是让增强 JS 节点能够按连接名称操作文件连接。任务 DAG 不需要包含对应的 FTP 文件节点，用户可以在 `process(record)` 中根据事件逻辑决定是否写文件，或把一个文件连接的文件复制到另一个文件连接。

最终采用“引擎内部适配现有 `TapFileStorage`”方案：

- JS 通过注入的 `storage` 门面调用文件操作；
- `StorageExecutorsManager` 按连接名称缓存 executor；
- `PdkStorageExecutor` 通过 PDK classloader 找到已有的 storage 实现，再用已有 `TapFileStorageBuilder` 初始化；
- 文件操作直接调用现有 `TapFileStorage` 方法；
- common-lib 不新增文件 operation API，不增加 `FileStorageFunction`，不增加 `TapFileStorage.capabilities()`；
- 文件操作的业务条件、是否执行、失败后的业务处理由用户 JS 决定；
- 不维护按 Tapdata 事件增长的 Mongo ledger。

## 2. 代码分析结果

### 2.1 数据库 aggregate 与文件操作不是同一条能力链

当前数据库 JS 能力在 `ScriptExecutorsManager.ScriptExecutor` 中实现：

```text
ScriptExecutorsManager.getScriptExecutor(name)
  -> ScriptExecutor
  -> PDK connectorInit
  -> ConnectorFunctions.ExecuteCommandFunction
  -> aggregate/execute/executeQuery/count/call
```

因此：

```javascript
ScriptExecutorsManager.getScriptExecutor("mongo-test").aggregate(query);
```

对数据库连接成立。

但 FTP 文件 Connector 的现有能力是 `TapFileStorage`，不是数据库 `ExecuteCommandFunction`。因此：

```javascript
ScriptExecutorsManager.getScriptExecutor("target-ftp");
```

可以按名称创建一个普通 PDK `ScriptExecutor`，但不能因此获得 `readFile/saveFile/delete`；FTP 文件操作必须使用：

```javascript
storage.update("target-ftp", data, options);
```

这两个对象保持职责隔离，不修改 `ScriptExecutor` 的数据库语义。

### 2.2 现有文件 API 足够支持本需求

`TapFileStorage` 已经提供本需求所需的底层操作：

| 需求 | 复用的方法 |
| --- | --- |
| 查询文件/目录 | `getFile(path)` |
| 判断文件存在 | `isFileExist(path)` |
| 读取文件 | `readFile(path, Consumer<InputStream>)` 或 `readFile(path)` |
| 上传/覆盖 | `saveFile(path, InputStream, boolean)` |
| 删除 | `delete(path)` |
| 服务端移动 | `move(sourcePath, destPath)` |
| 资源关闭 | `destroy()` |

所以不需要在 API 层增加 DTO、能力枚举或第二套 operation service。JS 门面只负责把用户的 Map 参数翻译成上述现有方法调用。

### 2.3 真实建连路径

本次代码的实际建连路径是：

```text
StorageExecutorsManager
  -> ConnectionResolver 查询 Connections
  -> ConnectionUtil.getDatabaseType
  -> PdkUtil.createNode
  -> ConnectorNode.getConnectorClassLoader
  -> TapFileStorageBuilder
       .withClassLoader(classLoader)
       .withStorageClassName(storageClass)
       .withParams(connectionConfig)
       .build()
  -> storage.init(connectionConfig)
```

`TapFileStorageBuilder.build()` 会反射创建 storage class 并调用 `init(params)`，因此它是一次真实连接初始化，不是轻量对象获取。实现中故意不调用 `connectorInit`，也不从 `ConnectorFunctions` 获取 `FileStorageFunction`，避免同时创建一个未使用的 Connector storage。

## 3. 总体架构

```text
enhanced JS process(record)
       |
       v
storage: StorageFacade
       |
       v
StorageExecutorsManager
       |  connectionName -> CompletableFuture<StorageExecutor>
       v
PdkStorageExecutor
       |  one executor -> one TapFileStorage
       v
TapFileStorage existing API
       |
       v
FTP / SFTP / SMB / S3FS / OSS / Local / NFS storage implementation
```

JS 节点初始化时注入：

```java
this.storageExecutorsManager = new StorageExecutorsManager(...);
this.storageFacade = new StorageFacade(storageExecutorsManager);
((ScriptEngine) engine).put("storage", storageFacade);
```

这与现有 `ScriptExecutorsManager`、`source`、`target` 注入并列，不要求 DAG 中存在 FTP 节点。

## 4. 引擎内部组件设计

### 4.1 `StorageExecutor`

引擎内部接口只暴露：

```java
String getConnectionName();
TapFileStorage getStorage();
String resolvePath(String path);
void close();
```

它不暴露 `FTPClient`、PDK `ConnectorNode`、连接凭据或 common-lib 新增类型。

`resolvePath` 使用连接的 `rootPath`，没有时回退到 `filePathString`；统一 `/`，拒绝 `..`、控制字符和协议 URL，最后把相对路径交给现有 storage 实现。

### 4.2 `StorageExecutorsManager`

#### 缓存结构

```text
ConcurrentMap<String, CompletableFuture<StorageExecutor>> executors
ConcurrentMap<String, Failure> failures
```

作用域是一个 JS processor 节点实例。连接名 trim 后作为 key。

#### 首次访问

1. 参数为空直接抛 `StorageOperationException`。
2. 查询连接；连接不存在抛异常。
3. 使用 `putIfAbsent` 写入未完成 future。
4. 只有 future 创建者执行 storage 工厂；其他事件等待同一个 future。
5. 成功后 future 完成，所有后续事件命中同一 executor。
6. 创建失败移除 future，并记录短时间 failure backoff，避免故障时每个事件都建一次 PDK。

#### 失效与关闭

- 远端 I/O 异常时由 `StorageFacade` 调用 `invalidate(connectionName, cause)`；
- 失效会从缓存移除并调用 `StorageExecutor.close()`；
- 创建中的 executor 若在 manager 关闭期间完成，会被立即关闭而不会进入缓存；
- manager 关闭时遍历并关闭所有已完成 executor；
- 关闭/失效不掩盖原始文件操作异常。

这里的缓存只解决连接复用和坏连接淘汰，不是业务幂等，也不记录事件处理结果。

### 4.3 `PdkStorageExecutor`

#### 协议映射

```java
local -> io.tapdata.storage.local.LocalFileStorage
ftp   -> io.tapdata.storage.ftp.FtpFileStorage
sftp  -> io.tapdata.storage.sftp.SftpFileStorage
smb   -> io.tapdata.storage.smb.SmbFileStorage
s3fs  -> io.tapdata.storage.s3fs.S3fsFileStorage
nfs   -> io.tapdata.storage.nfs.NfsFileStorage
oss   -> io.tapdata.storage.oss.OssFileStorage
```

映射位于引擎内部，不是 common-lib API。协议能否成功还取决于连接对应 PDK 的 classloader 中是否存在该实现及其配置是否完整。

#### 生命周期

`PdkManagedFileStorage` 持有：

- `TapFileStorage delegate`；
- PDK associateId；
- PDK state map/table map；
- 幂等 destroy 标志。

关闭顺序：

```text
delegate.destroy()
  -> releaseAssociateId
  -> stateMap.reset
  -> tapTableMap.reset
```

若 builder 初始化失败，则释放 associateId 和 map；因为没有调用 `connectorInit`，失败路径不调用 `connectorStop`。

## 5. `StorageFacade` 操作设计

### 5.1 `update`

签名：

```java
Map<String, Object> update(String connectionName,
                           Map<String, Object> data,
                           Map<String, Object> options)
```

`data.action` 默认是 `write`，当前支持 `write` 和 `copy`。

#### write

执行顺序：

1. 解析 `target.path`；
2. 读取 `content`；支持 String、byte[]、InputStream；
3. 查询目标是否存在；
4. 按 `overwrite` 处理：skip/fail/overwrite；
5. 用 `ByteArrayInputStream` 或用户提供的输入流调用 `saveFile`；
6. 关闭输入流；
7. 返回 `status`、`targetPath`、文件大小（若 storage 返回元数据）。

#### copy

请求形态：

```javascript
{
  action: "copy",
  source: { connection: "source-ftp", path: "in/a.csv" },
  target: { path: "archive/a.csv" }
}
```

执行顺序：

1. 获取 target executor；
2. 获取 source executor；source 可以与 target 相同；
3. 按 overwrite 处理目标文件；
4. 用 source `getFile` 确认源文件存在；
5. 不同 storage 实例：`source.readFile(callback)` 中直接调用 `target.saveFile`，不把完整文件载入 JS 或 Java byte[]；
6. 同一 storage 实例：先读到 `Files.createTempFile`，关闭远端输入流后再从临时文件调用 `saveFile`；
7. finally 删除本地临时文件；
8. 返回 copied/reused 结果。

同一 FTP storage 不能在持有 `readFile` 的受管输入流时立即调用同一实例的写操作，否则会与 FTP 单连接 I/O 锁发生等待。因此同实例复制必须采用临时文件路径。

### 5.2 `find`、`exists`、`delete`

- `find(name, {path}, options)`：调用 `getFile`，返回 path、length/size、lastModified、directory；不存在返回 null；
- `exists(name, path)`：调用 `isFileExist`；目录不是文件；
- `delete(name, {path}, options)`：调用 `delete`，返回底层布尔结果。

### 5.3 错误和失效

引擎输入结构错误、action 不支持、overwrite 非法、源文件不存在等抛 `StorageOperationException`，不会因此失效连接。

底层 storage 抛出的连接/I/O 异常会使相关 executor 失效；本次调用仍将异常返回给 JS，不在同一次调用内隐式重放用户文件操作。下一次事件重新建立连接。

## 6. 文件数据源代码审查和修复点

### 6.1 FTP：已确认并已修复

根据当前 `FtpFileStorage` 源码，已处理：

| 代码问题 | 修复 |
| --- | --- |
| 初始化中 connect/login/模式设置失败时 client 可能半初始化 | 使用局部 client 和统一清理，失败时 disconnect |
| destroy 重复调用或 logout 异常处理不完整 | closeClient 统一 logout/disconnect，destroy 可重复调用 |
| `retrieveFileStream` 读完只 close 不能保证 FTP pending command 完成 | 回调式读取 finally close + completePendingCommand；raw stream 用受管 `ManagedInputStream` 在 close 中完成一次 pending command |
| `storeFileStream/appendFileStream` 输出流关闭边界不明确 | `ManagedOutputStream` 负责幂等 close、pending command 和锁释放 |
| FTPClient 包含 current working directory 等共享状态 | storage 内部用公平 Semaphore 串行化有状态 I/O |
| 编码、timeout、host/port 等配置缺省或非法 | 在 `FtpConfig`/初始化路径归一化和校验 |

### 6.2 SFTP 和文件 Connector：已保留的修复

- SFTP 用受管输入/输出流关闭时释放 `ReentrantLock`；
- 初始化失败时关闭 channel/session；
- `FileConnector.onStop` 分别清理 merge executor、writer 和 storage，清理异常不阻断其他资源；
- CSV/JSON/Excel discovery 的异常、空文件和提前返回路径执行 storage destroy；
- 这些修复与 JS storage 共用同一批文件存储实现，因此保留在 Connector 分支。

### 6.3 Local/SMB/OSS/S3FS：本次补充的明确修复

#### Local 和 SMB 输出流

原代码在 `saveFile` 中手动 `close()` 输出流；如果读入或远端写入过程中抛异常，close 语句可能无法执行。现改为 try-with-resources，保证异常路径也关闭输出流。

#### OSS 和 S3FS 元数据查询

原 `getFile` 会打开远端对象来获取元数据，却没有关闭对象流。现用 try-with-resources 关闭 `OSSObject`/`S3Object`。

#### OSS/S3FS 存在性判断

OSS 原实现调用 `doesObjectExist` 后无条件返回 true；S3FS 原实现通过 `getObject` 判断存在但不关闭对象。现分别返回实际 `doesObjectExist` 结果，避免错误存在判断和对象流泄露。

#### OSS/S3FS 流式写入长度

原实现用 `InputStream.available()` 作为远端对象长度。`available()` 只代表当前无需阻塞即可读取的字节数，不代表文件总长度；对 FTP/SFTP 流式复制经常会得到 0 或部分长度。现不再把 `available()` 写入对象元数据，避免产生截断/零长度文件。

## 7. common-lib 和 Connector API 的处理边界

### 7.1 不新增的 API

以下类型不属于最终实现：

- `io.tapdata.file.operation.*` 下本需求之前增加的 DTO、错误码、状态、服务接口；
- `FileStorageCapability`；
- `TapFileStorage.capabilities()`；
- `FileStorageFunction`；
- `ConnectorFunctions.supportFileStorageFunction/getFileStorageFunction`；
- `DefaultFileOperationService`、`DefaultFileStorageSessionManager`、`FileStorageSession`。

这些类型的删除是为了恢复 common-lib 和 Connector 到既有公共 API边界，不是让 JS 直接访问 Connector 私有类。

### 7.2 为什么不需要这些 API

当前要求是“支持 JS 调用文件操作”，而不是“建立一套新的跨模块文件服务协议”。已有 `TapFileStorage` 已能完成文件读写；引擎只需要连接解析、storage 实例缓存、参数适配和资源生命周期。把每条操作抽象成 common-lib DTO 会扩大 API 耦合面，并要求所有文件 Connector 同步升级。

## 8. 幂等、重试与发布的边界

本最终实现不增加这三类业务机制：

- 幂等：不写 `file_operation_ledger`，不按 eventId 判断是否执行；
- 重试：不在一次 `storage.update` 中自动重放写入/复制；底层异常使 executor 失效，下一次调用重新建连；
- 发布：不先写远端临时文件再 move 发布，也不声明原子发布语义；同 storage copy 的临时文件仅用于释放读锁，finally 删除。

这样可以保持事件级脚本的自由度：用户可以按事件把一个文件写入 FTP、从 FTP A 读入 FTP B、或完全不执行操作。用户需要业务幂等、重试或发布语义时，应在 JS 或上层任务策略中显式实现，而不是由 JS 节点暗中维护一套事件账本。

## 9. 测试设计和验收

### 9.1 已完成单测

引擎模块：

```text
StorageExecutorsManagerTest: 6 passed
StorageFacadeTest: 5 passed
总计：11 passed, 0 failed
```

覆盖：

- 并发首次访问单飞；
- 事件级重复访问只创建一次 executor；
- 失败退避和失效重建；
- manager close 与创建竞态；
- write/find/exists/delete；
- 不同 storage 实例复制；
- 同一 storage 实例复制不发生读写锁等待；
- overwrite 冲突和非法 action。

Connector/file storage 模块已完成离线编译，FTP/SFTP 既有生命周期测试和资源修复保留。

### 9.2 真实 FTP 验收

1. 建立 FTP 连接但不在 DAG 放置 FTP 节点。
2. enhanced JS 中按下例写入：

   ```javascript
   storage.update("target-ftp", {
     action: "write",
     target: { path: "out/" + record.id + ".json" },
     content: JSON.stringify(record)
   }, { overwrite: "overwrite" });
   ```

3. 配置两个不在 DAG 中的 FTP 连接，按事件条件执行 copy。
4. 发送 1000 条事件，确认 PDK/storage 初始化次数按连接数而不是事件数增长。
5. 验证 skip/overwrite/fail；验证中文、空格和 rootPath。
6. 中断 FTP 控制连接，确认当前调用报错、缓存 executor 关闭，下一次调用能重建。
7. 检查大文件复制期间 JavaScript 堆不承载完整文件内容。
8. 停止任务，确认 FTP client、输入输出流、PDK associateId、state map、table map 清理。
9. 运行既有 Mongo aggregate 脚本，确认数据库脚本行为不变。

真实 FTP 服务和吞吐数据需要在验收环境执行，不能用本地 fake storage 测试替代。
