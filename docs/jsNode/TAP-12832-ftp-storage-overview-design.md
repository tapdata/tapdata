# TAP-12832：增强 JS 节点通过 `storage` 操作 FTP 方案概要设计

> 文档类型：方案概要设计  
> Jira：[TAP-12832](https://tapdata.atlassian.net/browse/TAP-12832)
> 适用范围：enhanced JS 节点、FTP 文件连接、文件存储公共能力
> 更新时间：2026-09-08

## 1. 方案结论

用户先在连接管理中创建 FTP 连接，enhanced JS 脚本按连接名称调用 `storage`，不要求 FTP 连接出现在当前任务 DAG 中。文件连接的 PDK、FTP session、流和临时文件均由 Java/Connector 层管理；脚本只接触普通 JSON、字符串和布尔值。

文件操作不复用数据库 `ScriptExecutor` 的方法集合：

```text
ScriptExecutorsManager.getScriptExecutor('mongo-test').aggregate(...)
  -> ExecuteCommandFunction -> Mongo 命令

storage.update('target-ftp', ...)
  -> StorageFacade
  -> StorageExecutorsManager
  -> PDK FileStorageFunction
  -> DefaultFileOperationService
  -> FTP TapFileStorage
```

`ScriptExecutorsManager.getScriptExecutor('target-ftp')` 可以按连接名初始化连接级 PDK，但它仍只提供数据库 command 语义，不能直接调用 `update` 完成 FTP 文件操作。

## 2. 本期交付边界

已实现：

- enhanced JS 注入 `storage`；standard JS 不注入。
- `storage.update(connectionName, data, options)` 支持 `write` 和单文件 `copy`。
- `storage.find(connectionName, {path}, options)` 返回单个文件/目录 metadata。
- `storage.exists(connectionName, path)` 检查文件存在性。
- `storage.delete(connectionName, {path}, options)` 删除单个文件。
- source 和 target 可以是两个不同的连接，目标连接作为 `update` 第一个参数。
- FTP 连接不要求存在 DAG；连接名称由引擎查询 `Connections`。
- PDK executor 及文件 session 在一个 JS processor node 实例内复用；并发首次访问使用 single-flight，避免重复创建 PDK。
- FTP 复制使用 Java 临时文件、size 校验、目标临时文件和最终发布；远端可恢复错误支持有限重试并使坏 session 失效。
- enhanced JS 节点关闭时关闭 storage executor、session、底层 storage 和 PDK associate id。

明确不属于本期：

- `beforeTask` 生命周期。
- `file_operation_ledger`、逐 Tapdata 事件持久化状态、跨重启业务幂等。
- 批量 list、pattern/recursive/preservePath、maxFiles/maxBytes 计划接口。
- checksum 校验的通用承诺；只有 connector 声明能力时底层 copy 才接受 checksum。
- FTPS、SFTP、SMB、S3、NFS、OSS 的 JS storage 开放。
- JS 直接获取 FTPClient、PDK 节点、连接配置、InputStream 或 OutputStream。

## 3. 运行架构

```text
Tapdata 事件
    |
    v
enhanced JS: process(record)
    |
    +-- storage.update/find/exists/delete
            |
            v
    StorageFacade
            |
            v
    StorageExecutorsManager
      connectionName -> StorageExecutor
            |
            v
    PdkStorageExecutor
      PDK node + FileStorageFunction
            |
            v
    PdkFileStorageSessionManager
      DefaultFileStorageSessionManager
            |
            v
    DefaultFileOperationService
            |
            v
    FTP TapFileStorage
```

同一个 `StorageExecutorsManager` 只为同名连接保留一个 `CompletableFuture<StorageExecutor>`。第一个调用创建 PDK，竞争调用等待相同 future；后续事件直接复用 executor。executor 内部的 session manager 使用 endpoint 的协议、rootPath 和参数 SHA-256 fingerprint 复用底层 storage，最多 10 个 session，空闲 10 分钟回收。

当前连接配置更新没有动态版本监听。修复配置后，应重启任务/节点，或由上层显式调用 `invalidate(connectionName, cause)` 使旧 executor 关闭并重新创建；本期不在每条事件查询配置版本。

## 4. JS API

### 4.1 直接写入 FTP

适用于小型文本或 JSON。Java 文件服务会把字符串转为 UTF-8 输入流；大文件不得通过 JS `content` 传递。

```javascript
function process(record) {
  var path = '/out/' + record.id + '.json';
  var result = storage.update('target-ftp', {
    action: 'write',
    target: { path: path },
    content: JSON.stringify(record.payload)
  }, {
    overwrite: 'overwrite',
    dryRun: false
  });

  record.fileOperation = result;
  return record;
}
```

### 4.2 FTP 到 FTP 单文件复制

```javascript
function process(record) {
  if (!record.sourcePath || !record.targetPath) return record;

  var result = storage.update('target-ftp', {
    action: 'copy',
    source: {
      connection: 'source-ftp',
      path: record.sourcePath
    },
    target: { path: record.targetPath }
  }, {
    overwrite: 'skip',
    verify: 'size',
    retryTimes: 2,
    timeoutMs: 10 * 60 * 1000
  });

  record.fileOperation = {
    status: result.status,
    sourcePath: result.sourcePath,
    targetPath: result.targetPath,
    bytes: result.bytes,
    attempts: result.attempts
  };
  return record;
}
```

### 4.3 查询、存在性和删除

```javascript
function process(record) {
  var exists = storage.exists('source-ftp', record.sourcePath);
  if (exists) {
    var metadata = storage.find('source-ftp', { path: record.sourcePath });
    record.fileSize = metadata ? metadata.size : null;
  }
  if (record.deleteSource === true && exists) {
    storage.delete('source-ftp', { path: record.sourcePath });
  }
  return record;
}
```

### 4.4 参数和返回值

| 方法 | 参数 | 当前行为 |
| --- | --- | --- |
| `update` | 目标连接名、data、options | `action=write/copy`；同步返回普通 Map |
| `find` | 连接名、`{path}`、options | 返回 metadata Map；不存在返回 `null` |
| `exists` | 连接名、path | 返回 boolean |
| `delete` | 连接名、`{path}`、options | 返回 boolean |

`update` 支持的主要选项：

- `overwrite`: `skip`、`overwrite`、`fail`，默认 `skip`。
- `verify`: `none`、`size`、`checksum`，默认 `size`；checksum 还受目标 storage capability 限制。
- `retryTimes`、`timeoutMs`：copy 的有限重试和超时参数。
- `dryRun`：write/copy 只检查并返回 dry-run 结果，不执行目标写入。

返回结果字段包括 `status`、`sourcePath`、`targetPath`、`bytes`、`checksum`、`attempts` 和 `durationMs`。文件内容和 Java stream 不返回 JS。

## 5. 连接和协议范围

连接解析流程：

1. 以连接名称精确查询 `Connections`。
2. 使用连接 document 的 `config` 构造 `FileEndpoint`。
3. 通过 `ConnectionUtil.getDatabaseType`、`PdkUtil.createNode` 和 PDK `INIT` 创建连接级节点。
4. 获取 `ConnectorFunctions.getFileStorageFunction()`。
5. 当前只接受 protocol 为 `ftp`；其他协议返回 `FILE_UNSUPPORTED_OPERATION`。

凭据不进入 JS，也不写入日志。引擎不直接调用 `FileStorageFactory`，由 PDK `FileStorageFunction` 负责跨 classloader 提供 storage。

| 协议 | JS storage 状态 | 说明 |
| --- | --- | --- |
| FTP | 本期支持 | 需要 FTP connector 暴露 FileStorageFunction |
| SFTP | 本期不开放 | 已完成 host key、channel、stream 和错误边界修复，但未在 engine 放开 |
| FTPS | 不支持 | `ftpSsl` 不能替代 FTPSClient/TLS 和证书校验 |
| SMB/S3/NFS/OSS | 不支持 | 需先完成各自 adapter、能力声明和集成测试 |
| 数据库 | 不通过 storage | 继续使用 ScriptExecutorsManager/ScriptExecutor |

## 6. 文件资源治理

本次改造同时修复了代码审查确认的资源边界：

- FileConnector 停止时使用独立清理路径，merge/release 异常不会跳过 storage destroy 和 executor shutdown。
- FileSchema 的中断路径恢复 interrupt 并关闭 executor。
- CSV、JSON、Excel discovery 使用 finally 关闭 storage，覆盖正常、空文件和异常路径。
- FTP 初始化部分失败会 disconnect；destroy 可重复调用。
- FTP raw input/output stream 的 close 受管，并完成 pending command；FTP 有状态操作按 session 串行化。
- SFTP 初始化失败、raw stream close、channel 生命周期和错误分类有边界处理；未实现 move 不声明原子移动能力。
- 文件 session 采用引用计数、idle eviction 和 draining invalidate；坏 session 不会被下一次操作继续复用。
- copy 使用本地临时文件和目标临时路径，finally 清理本地/远端临时文件；最终路径执行 stat。

这些逻辑均位于公共文件服务或 Connector 层，不在 JS 节点维护事件 ledger、业务幂等或连接状态。

## 7. 验收重点

1. DAG 不包含 FTP 节点时，enhanced JS 可按连接名称调用 FTP。
2. 用户可在 `process(record)` 中按事件条件直接写入 FTP。
3. 用户可在 `process(record)` 中把 source FTP 的文件复制到另一个 target FTP。
4. 同一节点内重复使用连接名不会按事件重复创建 PDK/FTP session。
5. 远端连接错误会使坏 session 失效，重试不会无界建立连接。
6. FTP 初始化失败、任务关闭、FTP stream close 和临时文件清理路径无资源泄露。
7. `ScriptExecutorsManager.getScriptExecutor('target-ftp').update(...)` 不作为文件 API；Mongo aggregate 语义保持不变。
8. standard JS 没有 `storage`，SFTP/FTPS/其他未开放协议不会被误宣称为可用。

## 8. 后续扩展边界

后续若需要目录 list、批量过滤、checksum、跨重启幂等或 beforeTask，应分别新增明确的公共 API 和生命周期设计，并由文件服务/任务运行时实现。不能通过在 JS 节点中增加 Mongo ledger 或按事件自动校验来替代这些能力；本期功能的职责仅是提供用户显式调用文件操作的运行时支持。
