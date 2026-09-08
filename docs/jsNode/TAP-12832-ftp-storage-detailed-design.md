# TAP-12832：JS 节点操作 FTP 文件的详细设计

## 1. 文档信息

| 项目 | 内容 |
| --- | --- |
| Jira | [TAP-12832](https://tapdata.atlassian.net/browse/TAP-12832) |
| 类型 | 详细设计 / 可实施方案 |
| 范围 | JS 节点、文件连接、FTP 文件操作、文件存储公共能力 |
| 关联概要设计 | /Users/gavinxiao/kit/tapdata/tapdata/docs/jsNode/TAP-12832-ftp-storage-overview-design.md |

## 2. 目标、边界与核心结论

### 2.1 目标

支持用户在连接管理中新增 FTP 连接，并在 enhanced JS 节点中通过连接名称完成文件查询、存在性检查和受控复制。连接不要求出现在当前任务 DAG 中。大文件必须在 Java/connector 层流式传输，不得经过 JavaScript 堆；操作还需要具备路径安全、权限校验、重试、临时文件、校验、幂等、审计和可观测性。

CSV、JSON、XML、Excel、文件流等文件 source/target 也应逐步复用同一套配置映射、session 生命周期、路径策略和文件操作基础能力。

### 2.2 非目标

- 不把数据库命令能力扩展成任意文件命令能力。
- 不把 FTP 客户端、账号密码、Java stream 或 PDK 节点暴露给 JS。
- 首期只落地 FTP；其他协议复用公共接口和能力矩阵。
- 首期不宣称支持 FTPS。当前 ftpSsl 字段不能证明实现使用了 TLS。

### 2.3 对当前问题的判断

相关代码：

- /Users/gavinxiao/kit/tapdata/tapdata/iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/script/ScriptExecutorsManager.java
- /Users/gavinxiao/kit/tapdata/tapdata-connectors/connectors/mongodb-connector/src/main/java/io/tapdata/mongodb/MongodbConnector.java
- /Users/gavinxiao/kit/tapdata/tapdata-common-lib/plugin-kit/tapdata-pdk-api/src/main/java/io/tapdata/pdk/apis/functions/connector/source/ExecuteCommandFunction.java

现状结论：

1. ScriptExecutorsManager.getScriptExecutor(connectionName) 按连接名称查询，不要求连接出现在当前 DAG。
2. 连接配置、PDK hash 和 PDK 初始化满足条件时，没有 DAG FTP 节点也可以创建连接级 PDK 节点。
3. ScriptExecutor 只封装 execute、executeQuery、count、aggregate、call 等数据库命令。
4. Mongo aggregate 能工作，是因为 MongoDB Connector 注册了 ExecuteCommandFunction，并在该函数中路由到 Mongo 聚合。
5. 文件 Connector 当前没有通过 ExecuteCommandFunction 提供 FTP 文件操作。

因此：

~~~text
ScriptExecutorsManager.getScriptExecutor("target-ftp")
可以在没有 DAG FTP 节点时找到并初始化连接；
但当前不能像 Mongo aggregate 一样执行 FTP 文件操作。
~~~

本方案保留 ScriptExecutor 的数据库语义，新增独立的 StorageExecutor 和 JS storage 门面，不把文件操作伪装成数据库 command。

## 3. 现状代码与问题

### 3.1 JS 节点

主要代码：

- /Users/gavinxiao/kit/tapdata/tapdata/iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/hazelcast/processor/HazelcastJavaScriptProcessorNode.java
- /Users/gavinxiao/kit/tapdata/tapdata/iengine/iengine-common/src/main/java/com/tapdata/processor/ScriptUtil.java
- /Users/gavinxiao/kit/tapdata/tapdata/iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/hazelcast/HazelcastBaseNode.java

当前 enhanced JS 向脚本绑定 ScriptExecutorsManager、source、target、env，并按输入记录调用 process。没有任务级 beforeTask；如果把 FTP 复制写在 process 中会重复执行，没有输入记录时也无法完成一次性文件操作。

### 3.2 文件 Connector 与 FTP

主要代码：

- /Users/gavinxiao/kit/tapdata/tapdata-connectors/connectors-common/file-connector-core/src/main/java/io/tapdata/common/FileConnector.java
- /Users/gavinxiao/kit/tapdata/tapdata-connectors/connectors-common/file-connector-core/src/main/java/io/tapdata/common/FileStorageFactory.java
- /Users/gavinxiao/kit/tapdata/tapdata-connectors/file-storages/ftp-file/src/main/java/io/tapdata/storage/ftp/FtpFileStorage.java

已有 FileStorageFactory、FileConnector、TapFileStorage、DefaultFileStorageSessionManager、DefaultFileOperationService 等基础，但存在以下缺口：

- engine 没有稳定的 PDK 文件能力入口，直接使用 FileStorageFactory 会越过 PDK/classloader 边界。
- TapFileStorage 源码与部分实现的 capabilities() API 需要对齐。
- 文件 operation API 在当前工作树中需要确认并归入真实共享源码模块，不能只依赖 target/classes。
- FtpFileStorage.readFile(String) 使用 retrieveFileStream() 后没有统一保证 completePendingCommand()，可能污染可复用连接。
- FTPClient 有状态，工作目录、控制连接和数据连接不应由多个操作无边界并发共享。
- ftpSsl 字段与实际 FTPS 实现不一致。
- 需要在低层复制服务之上增加任务级幂等、字节预算、发布后二次确认和坏 session 失效。

## 4. 总体架构

~~~text
JS beforeTask(context)
        |
        v
StorageFacade：JS 安全 JSON API
        |
        v
StorageExecutorsManager：按名称解析、权限、缓存、生命周期
        |
        v
PdkFileStorageSessionManager：通过 PDK FileStorageFunction 获取 TapFileStorage
        |
        v
DefaultFileOperationService：路径、列表、流式复制、临时文件、校验、重试
        |
        v
TapFileStorage：协议无关共享接口
        |
        v
FTP / SFTP / SMB / S3 等 Connector PDK
~~~

数据库链路仍然是：

~~~text
ScriptExecutorsManager -> ScriptExecutor -> ExecuteCommandFunction -> Mongo aggregate
~~~

设计原则：

1. 连接按名称解析，但按租户、任务和权限隔离；不以 DAG 是否包含连接为条件。
2. engine 不直接 new FtpFileStorage，不解析 FTP 密码，不持有 connector 具体实现。
3. Connector 提供协议能力；公共服务负责路径、遍历、复制、校验、重试和幂等。
4. JS 只传递普通 JSON；大文件永远不经过 GraalJS。
5. 写入默认先写临时文件，校验后发布；协议不支持原子移动时必须明确失败或标记非原子降级。
6. 所有批量操作有文件数、字节数、超时和并发上限。

## 5. PDK 文件能力设计

### 5.1 新增 FileStorageFunction

建议新增文件：

/Users/gavinxiao/kit/tapdata/tapdata-common-lib/plugin-kit/tapdata-pdk-api/src/main/java/io/tapdata/pdk/apis/functions/connector/common/FileStorageFunction.java

接口：

~~~java
public interface FileStorageFunction extends TapConnectorFunction {
    TapFileStorage getStorage(TapConnectorContext connectorContext) throws Throwable;
}
~~~

TapFileStorage、FileStorageFunction 和公共 DTO 必须位于 PDK 与 engine 都能访问的父级公共 classloader。JS 不得取得返回的 Java 对象；StorageExecutor 负责其生命周期。

### 5.2 扩展 ConnectorFunctions

文件：

/Users/gavinxiao/kit/tapdata/tapdata-common-lib/plugin-kit/tapdata-pdk-api/src/main/java/io/tapdata/pdk/apis/functions/ConnectorFunctions.java

新增字段、注册方法和 getter：

~~~java
private FileStorageFunction fileStorageFunction;

public ConnectorFunctions supportFileStorageFunction(FileStorageFunction function) {
    this.fileStorageFunction = function;
    return this;
}

public FileStorageFunction getFileStorageFunction() {
    return fileStorageFunction;
}
~~~

ConnectorFunctions.getCapabilities() 会反射 TapFunction 字段，新增能力名必须固化为稳定值，例如 file_storage_function，并加入测试。

### 5.3 文件 Connector 统一注册

在 FileConnector 中增加：

~~~java
protected void registerFileStorageFunction(ConnectorFunctions functions) {
    functions.supportFileStorageFunction(context -> {
        if (storage == null) {
            throw new TapPdkException("File storage has not been initialized");
        }
        return storage;
    });
}
~~~

CSV、JSON、XML、Excel、File Stream 以及其他继承 FileConnector 的 Connector 都必须在 registerCapabilities() 中调用该方法。

### 5.4 API 源码基线

实现前必须完成：

1. 统一 TapFileStorage.capabilities() 的源码和实现。
2. 明确输入路径是连接 root 下的相对路径。
3. 统一 readFile、回调式读取、保存、移动、删除、建目录和能力查询的异常约定。
4. 优先使用回调式读取；若返回 stream，必须在 close 时完成协议 pending command。
5. 将 FileEndpoint、FileCopyRequest、FileOperationErrorCode、FileOperationStatus、FileStorageCapability 等 API 放入真实共享源码模块。

这是构建前置条件，避免源码构建与 target 构建看到不同 API。

## 6. 连接级 StorageExecutor

### 6.1 抽取公共 PDK 生命周期

从 ScriptExecutorsManager 中抽取以下组件：

~~~text
ConnectionResolver
  查询连接、校验租户/权限、返回 connectionId/config/pdkHash

PdkConnectionHandle
  PdkUtil.createNode、connectorInit、函数获取、stop/release/close

PdkConnectionExecutorFactory
  统一创建、超时、异常和资源清理
~~~

ScriptExecutorsManager 和 StorageExecutorsManager 共同复用，避免复制 PDK 初始化和关闭逻辑。

### 6.2 StorageExecutorsManager

建议新增：

io.tapdata.flow.engine.V2.script.storage.StorageExecutorsManager

职责：

- 按连接名称解析文件连接。
- 校验当前租户、任务和用户权限。
- 校验数据库类型是文件协议。
- 初始化对应 PDK 节点并获取 FileStorageFunction。
- 缓存 StorageExecutor，避免每次调用都登录 FTP。
- 连接认证失败、远程 I/O 失败或超时时使 session 失效。
- 节点关闭时关闭所有 StorageExecutor 和 PDK 节点。

缓存键建议：

~~~text
tenantId + connectionId + connectionConfigVersion + pdkHash
~~~

密码、私钥和完整连接参数不能进入日志或缓存键。连接配置变更必须使旧缓存失效。

### 6.3 连接解析和配置边界

storage.update('target-ftp', ...) 中的字符串是连接名称，不是 DAG target。

解析流程：

1. 当前租户内精确匹配名称。
2. 不存在返回 CONNECTION_NOT_FOUND 或 FILE_CONFIG_INVALID。
3. 多条脏数据匹配时拒绝执行，不随机选择。
4. 校验任务/用户的连接使用权限。
5. 校验文件类型和 FileStorageFunction。
6. 仅用连接配置初始化 PDK，不依赖当前 DAG node config。

配置边界：

| 配置 | 来源 | JS 是否可覆盖 |
| --- | --- | --- |
| protocol、host、port | 连接配置 | 否 |
| username、password、private key | 连接配置 | 否 |
| ssl、passive、timeout、encoding | 标准化连接配置 | 否 |
| rootPath | 连接配置 | 否 |
| sourcePath、targetPath | 当前操作请求 | 是，必须过路径策略 |
| pattern、recursive、preservePath | 当前操作请求 | 是，受系统上限 |
| CSV/JSON/XML 解析参数 | source/target node config | 不由本 API 处理 |

### 6.4 连接缓存、初始化失败与事件级性能

当前 ScriptExecutorsManager 的缓存行为必须作为本需求的性能和可靠性基线：

- getScriptExecutor(connectionName) 通过 CacheMap 按连接名称缓存 ScriptExecutor。
- 当前缓存最大 10 个连接，空闲过期值为 600；过期或容量淘汰时会调用 ScriptExecutor.close()。
- HazelcastJavaScriptProcessorNode 在 shared init 阶段创建一个 ScriptExecutorsManager，正常事件不会为每条记录创建新的 manager。
- 同一个 manager 中，同名连接首次访问会触发 Connections 查询、PdkUtil.createNode、connectorInit 和 PDK 能力获取；后续缓存命中不会重复创建 PDK。
- 缓存是 JVM/processor 实例本地缓存，不是跨 Hazelcast worker 的全局缓存。多个 worker、任务重启或任务重平衡会分别创建自己的 PDK 节点。

需要明确区分以下两种情况：

| 情况 | 当前行为 | 性能风险 |
| --- | --- | --- |
| PDK 初始化成功 | executor 放入缓存，后续按连接名命中 | 不会每条事件重复创建 PDK，但如果把文件操作放在 process 中，文件 I/O 仍会每条事件执行 |
| PDK 初始化失败 | supplier 异常不会形成可复用的成功缓存项 | 后续每条事件可能再次查询连接、创建 PDK、连接 FTP 和登录，造成异常风暴 |
| FTP PDK 初始化成功但调用 aggregate | executor 命中缓存，但文件 Connector 没有 ExecuteCommandFunction | 不会重复建连，但每条事件可能重复抛出不支持 execute command |
| 缓存过期或淘汰 | executor.close() 后下次访问重新创建 | 可能产生重新登录和连接抖动 |
| 多 worker 或任务重启 | 每个本地 manager 独立缓存 | 连接数按 worker/任务实例放大 |

因此，文件能力不能以每条事件的 process 调用为生命周期边界。StorageExecutorsManager 必须增加以下机制：

1. 按 connectionId、连接配置版本和 pdkHash 缓存 StorageExecutor，不能只按可变的连接名称缓存。
2. 对同一连接采用单飞初始化：同一时刻只允许一个线程执行 PDK 创建和 FTP 登录，其他线程等待同一个初始化结果。
3. 初始化失败设置短期负缓存和退避状态，避免每条事件重复建连。建议至少区分配置/认证错误、远程网络错误和 PDK 加载错误。
4. 配置错误、认证失败等永久错误直接阻断本次任务；网络超时等临时错误采用有限次数和指数退避。
5. 负缓存必须设置过期时间，不能永久阻止管理员修复连接后重试。
6. beforeTask 失败后设置任务级失败门禁，后续 process 不再调用 storage，也不再重复初始化连接。
7. 批量文件操作期间复用已经取得的 session，不为每个文件重新创建 PDK 节点。
8. 记录 cache hit、cache miss、初始化耗时、初始化失败、负缓存命中、淘汰、重建和当前 active session 数量。

建议状态机：

~~~text
不存在
  -> INITIALIZING（单飞）
  -> READY
  -> FAILED_BACKOFF（负缓存和退避）

READY
  -> 远程连接失效 -> INVALIDATED -> INITIALIZING
  -> 空闲/容量淘汰 -> CLOSED

FAILED_BACKOFF
  -> 退避时间未到 -> 直接返回原始分类错误
  -> 退避时间到 -> INITIALIZING
~~~

注意：ScriptExecutorsManager 当前只适合继续承载数据库 executor 兼容逻辑；即使它已有 CacheMap，也不能直接把该缓存语义复制到 FTP 文件操作。文件 executor 还需要配置版本、负缓存、单飞初始化、任务级失败门禁和 session 失效。

## 7. 文件操作公共服务

### 7.1 模块边界

现有公共类主要位于：

/Users/gavinxiao/kit/tapdata/tapdata-connectors/connectors-common/file-connector-core/src/main/java/io/tapdata/file/operation

推荐划分：

~~~text
shared-file-api
  FileEndpoint、FileCopyRequest、错误码、状态、能力、服务接口

file-connector-core
  FileStorageFactory、协议适配和 Connector 内部实现

engine file-operation
  PdkFileStorageSessionManager、文件编排、StorageFacade
~~~

如果 DefaultFileOperationService 继续位于 file-connector-core，则必须提供可注入 FileStorageSessionManager 的构造入口。engine 不能直接使用 FileStorageFactory 创建 FTP 类。

### 7.2 PdkFileStorageSessionManager

~~~text
retain(endpoint)
  -> StorageExecutorsManager.get(connection)
  -> FileStorageFunction.getStorage(context)
  -> 引用计数加一
  -> 返回 FileStorageSession

release(session)
  -> 引用计数减一
  -> 空闲超时或容量超限时回收

invalidate(session)
  -> 从缓存删除
  -> 关闭/销毁关联 PDK 节点
  -> 下次 retain 重新连接
~~~

必须支持最大 session 数、空闲回收、重复 close 保护、配置版本失效、连接失败 invalidate 和初始化异常清理。现有使用 thread id 参与 key 的逻辑不能作为唯一复用边界；key 应优先使用 connectionId、root、配置版本和非敏感指纹。

### 7.3 路径与 endpoint

FileEndpoint 建议增加 connectionId、connectionName、protocol、rootPath。params 只放非敏感标准化参数；如果不修改公开 endpoint，则新增内部 ResolvedFileEndpoint。

公共 FilePathPolicy 负责：

- 接受并归一化 JS 输入的前导 slash。
- 拒绝绝对路径、协议 URL、控制字符、空路径和 ..
- 在连接 root 内解析相对路径。
- 对日志中的路径做长度和字符限制。

### 7.4 DefaultFileOperationService 增强

必须补齐：

1. 临时路径使用 operationId、安全化文件名和随机后缀。
2. 写临时文件后执行 size/checksum 校验，move 后执行最终 stat。
3. finally 清理临时文件；清理失败单独告警，不覆盖主错误。
4. copyBatch 限制文件数、总字节数和执行中累计字节数。
5. 返回部分成功明细，不因第一项失败丢失已完成结果。
6. 重试前 invalidate 可疑 session。
7. 默认串行；并发时每个任务使用独立 session。
8. 幂等判断位于 facade/ledger，低层服务执行单次已解析复制。

## 8. JS API 与生命周期

### 8.1 API

增强 JS 暴露：

~~~javascript
storage.find(connectionName, query, options)
storage.exists(connectionName, path)
storage.update(targetConnectionName, data, options)
storage.delete(connectionName, data, options)
~~~

首期 update 以 copy action 为主，不暴露 getConnection、openStream、getRawClient。

推荐脚本：

~~~javascript
function beforeTask(context) {
  return storage.update('target-ftp', {
    action: 'copy',
    source: {
      connection: 'source-ftp',
      path: '/inbound',
      pattern: '*.csv',
      recursive: true
    },
    target: {
      path: '/outbound',
      preservePath: true
    }
  }, {
    overwrite: 'skip',
    verify: 'size',
    idempotencyKey: 'ftp-' + context.businessDate,
    maxFiles: 1000,
    maxBytes: 10 * 1024 * 1024 * 1024,
    timeoutMs: 30 * 60 * 1000,
    retryTimes: 2
  });
}

function process(record) {
  return record;
}
~~~

update 选项：

| 字段 | 说明 |
| --- | --- |
| overwrite | skip、overwrite、fail，默认 skip |
| verify | none、size、checksum，默认 size |
| preservePath | 是否保留 source root 下相对目录 |
| recursive | 是否递归目录 |
| pattern/include/exclude | glob 过滤 |
| maxFiles/maxBytes | 数量和字节上限，不能超过系统硬上限 |
| timeoutMs/retryTimes | 超时和可重试次数 |
| idempotencyKey | 跨重试、跨实例幂等键 |
| dryRun | 只列出计划，不写入 |
| failFast | 首个失败是否停止批次 |

大文件内容不进入 JS，返回值只包含状态、文件摘要、字节数、耗时和错误。

### 8.2 beforeTask

~~~text
doInit
  -> 创建 StorageExecutorsManager / StorageFacade
  -> 构建 enhanced JS engine
  -> 绑定 storage、env、source、target
  -> 调用可选 beforeTask(context)
  -> SUCCESS：允许 process
  -> FAILED：节点失败，不向下游发送记录
~~~

在 HazelcastJavaScriptProcessorNode.doInit() 调用 super.doInit() 后，通过 Invocable.invokeFunction("beforeTask", context) 执行。没有 beforeTask 时状态为 SKIPPED，保持旧行为。

context 至少包含 taskId、nodeId、taskType、businessDate、trialRun、operationId、env。不得包含密码、私钥、完整连接配置或 PDK 节点。

tryProcess() 增加 BeforeTaskState 门禁：SUCCESS 才调用 process，FAILED 直接失败，RUNNING 不接收记录。测试和预览任务强制 dryRun 或禁写。

doClose() 先停止新记录，再等待文件操作，关闭 facade session、StorageExecutorsManager、PDK 节点，最后关闭旧 source/target executor 和脚本引擎。

beforeTask 是连接初始化和文件操作的任务级边界：

- 正常任务在 beforeTask 中完成 FTP 连接解析、权限校验和必要的 session 初始化。
- beforeTask 失败后将节点状态设置为 FAILED，后续事件直接失败或被丢弃，不再重复尝试连接。
- process 只处理记录转换，不承担一次性 FTP 同步、建连和登录。
- 测试/预览任务也不能通过每条事件重新初始化远程连接；默认使用 dryRun、共享 session 和同样的负缓存策略。

## 9. 幂等、重试与发布

### 9.1 ledger

建议增加 Mongo 集合 file_operation_ledger：

~~~json
{
  "tenantId": "...",
  "taskId": "...",
  "nodeId": "...",
  "idempotencyKey": "...",
  "requestHash": "sha256(...)",
  "operationId": "...",
  "status": "RUNNING",
  "leaseOwner": "...",
  "leaseExpireAt": "...",
  "files": [],
  "summary": {},
  "error": {},
  "createdAt": "...",
  "updatedAt": "..."
}
~~~

建立唯一索引：

~~~text
(tenantId, taskId, nodeId, idempotencyKey)
~~~

requestHash 包含规范化 source、target、过滤条件、覆盖策略、校验策略和连接配置版本，不包含密码。

状态：

~~~text
不存在 -> RUNNING -> SUCCEEDED
                  -> FAILED
RUNNING 且 lease 过期 -> RUNNING（接管）
SUCCEEDED 且 hash 相同 -> 复用结果
已存在且 hash 不同 -> IDEMPOTENCY_CONFLICT
~~~

已成功时先 stat/校验目标；正在执行且 lease 未过期时不得并发写同一批目标；失败时下一次只处理未成功或校验不通过文件。

### 9.2 发布流程

~~~text
确保目标父目录
  -> 写入 .tapdata-tmp/<operationId>/<safe-name>.part
  -> size/checksum 校验
  -> move 到最终路径
  -> final stat
  -> 写入 ledger
~~~

默认 overwrite=skip；目标存在且校验通过返回 REUSED。overwrite=fail 返回 FILE_TARGET_CONFLICT。协议不支持原子 rename 时默认返回 FILE_UNSUPPORTED_OPERATION，不得伪装为原子发布。

## 10. FTP 与文件数据源整体优化

### 10.1 FTP

FtpFileStorage 初始化必须校验 host、port、认证、connect/login/passive/binary 返回值，并设置 connect、data、socket timeout；失败时主动 disconnect。读取优先采用回调式 API，确保 completePendingCommand；失败连接必须 invalidate。首期批量操作默认串行，避免共享有状态 FTPClient。

ftpSsl 首期明确拒绝，除非同版本使用 FTPSClient 完成 TLS、证书校验、显式/隐式模式和集成测试。

FTP 能力只声明真实支持的 READ、WRITE、LIST、MAKE_DIRECTORY、DELETE 和经过验证的 ATOMIC_RENAME；默认不声明 CHECKSUM、APPEND。

### 10.2 统一配置

统一 FileConnectionConfig：

~~~text
protocol、host、port、username、password/credentialRef、rootPath、
ssl、passive、connectTimeoutMs、dataTimeoutMs、encoding
~~~

DefaultFileServiceConfigMapper 统一处理 host/server、user/username、timeout、encoding 等别名，source、target、JS storage 共用。

### 10.3 统一文件基础能力

- 所有 file connector 共享 session 生命周期、空闲回收、配置变更失效和异常 reconnect。
- 所有列表操作支持最大数量、最大字节数、稳定排序和分页/截断语义。
- 所有协议通过 TapFileStorage.capabilities() 声明目录创建、删除、原子移动、checksum 和并发能力。
- SFTP、SMB、S3 等协议不在 JS facade 中写特判，差异由 adapter 和能力矩阵处理。

## 11. 安全、审计与可观测性

storage 首次取得连接时校验租户、用户/服务账号、任务引用权限、读写删除权限和跨连接复制权限。连接名称不是权限凭证。

JS 不得读取连接配置；日志、异常、ledger 和指标标签不得包含 password、private key、token 或完整连接 URL。连接缓存只保存运行时对象和不可逆配置版本标识。

storage.delete 默认只允许单文件或受限列表；递归删除需要显式 recursive=true 且受系统开关控制；预览和测试禁止真实删除；删除必须写审计日志。

建议指标：

- storage_operation_total{action,protocol,status}
- storage_operation_failed{errorCode,protocol}
- storage_operation_bytes_total{protocol}
- storage_operation_duration_ms{action,protocol}
- storage_session_active{protocol}
- storage_session_eviction_total{reason}
- storage_idempotency_reuse_total
- storage_idempotency_conflict_total
- storage_temp_cleanup_failed_total

日志至少包含 taskId、nodeId、operationId、连接 ID、相对路径、action、status、errorCode、耗时、文件数和字节数；禁止包含密码、私钥、token 和完整连接 URL。

## 12. 代码改造清单

| 模块 | 主要改造 |
| --- | --- |
| tapdata-pdk-api | FileStorageFunction；ConnectorFunctions 注册、getter 和 capability |
| tapdata-api/shared-file-api | TapFileStorage API 对齐；FileEndpoint、FileCopyRequest、错误码、状态和能力源码归位 |
| file-connector-core | FileConnector 统一注册；配置 mapper；可注入 session manager |
| CSV/JSON/XML/Excel/FileStream Connector | 注册 FileStorageFunction |
| ftp-file | stream pending command、初始化、timeout、session 并发、FTPS 边界 |
| engine V2 | ConnectionResolver、PdkConnectionHandle、StorageExecutorsManager |
| engine file operation | PdkFileStorageSessionManager、StorageFacade、DefaultFileOperationService 增强；单飞初始化、负缓存、退避和 session 失效 |
| HazelcastJavaScriptProcessorNode | storage binding、beforeTask、BeforeTaskState、关闭流程 |
| engine metadata | file_operation_ledger、lease、幂等恢复 |

## 13. 测试设计

### 13.1 单元测试

- ConnectorFunctions 可注册、获取 FileStorageFunction，capability 名称稳定。
- 没有 DAG 节点时按连接名初始化文件 PDK。
- 同一个连接的并发首次访问只创建一个 PDK executor，其余调用复用或等待。
- PDK/FTP 初始化失败后，退避窗口内不会按每条事件重复建连。
- 修复连接配置后，负缓存到期能够重新初始化成功。
- 非文件连接、不存在连接、无权限、缺少能力均返回稳定错误。
- PDK 初始化失败时资源完整释放。
- storage DTO 映射、路径安全、overwrite、verify、preservePath、dryRun 和 limits 正确。
- copy、copyBatch、list、exists、stat、validate、临时清理、checksum、session invalidate 正确。
- 批量部分成功结果完整。

### 13.2 FTP 集成测试

使用本地 FTP server 或 Testcontainers 覆盖单/多文件、递归、Unicode 文件名、目标目录创建、skip/overwrite/fail、size 校验、控制连接断开重试、临时文件清理、session 复用和配置变更失效。FTPS 未实现时必须明确失败，不得建立明文替代连接。

### 13.3 JS 节点测试

- enhanced JS 有 storage，standard JS 没有。
- 无输入记录时 beforeTask 仍执行一次。
- beforeTask 在一个任务实例中只执行一次。
- beforeTask 失败时不调用 process、不向下游输出。
- beforeTask 失败后后续事件不会再次触发 FTP PDK 初始化。
- process 中重复调用连接名解析时命中已有 StorageExecutor，不重新建连。
- 多 worker 场景下每个 worker 的连接数受 session 上限约束。
- 预览/测试强制 dryRun。
- 多 engine 实例通过 ledger 去重。
- 任务重试只处理未完成文件。

### 13.4 端到端验收

~~~text
source FTP 文件 -> JS beforeTask storage.update -> target FTP 文件
               -> JS process 输出 -> 下游数据库
~~~

必须验证：DAG 没有 FTP 节点也能使用连接列表中的 FTP；复制成功后才向下游输出；认证失败、源文件不存在、校验失败或目标冲突时下游不提交；重试不重新覆盖已校验成功文件；日志不泄露密码和完整连接串。

## 14. 发布、回滚与验收标准

### 14.1 Feature flag

建议使用：

- js.node.storage.enabled
- js.node.beforeTask.enabled
- js.node.storage.write.enabled
- js.node.storage.ftp.enabled

旧 JS、Mongo aggregate 和既有 file source/target 默认不变；standard JS 不自动获得 storage；预览和测试强制 dryRun 或禁写。

### 14.2 发布顺序

1. 统一 TapFileStorage 和文件 operation API 源码。
2. 完成 FileStorageFunction、ConnectorFunctions 和全部文件 Connector 注册。
3. 完成 PDK-backed session manager 和公共文件服务。
4. 完成 StorageFacade、beforeTask、状态门禁和 ledger。
5. 完成 FTP 集成、异常恢复、指标和审计。
6. 使用 feature flag 灰度开启 FTP storage 写能力。

### 14.3 回滚

关闭 storage 和 beforeTask 开关后，新 JS 文件操作停止；旧数据库 JS 和既有文件 source/target 不受影响。ledger 保留用于诊断，不删除连接配置和历史记录。

### 14.4 验收标准

1. 无 DAG FTP 节点时，授权 JS 可以按连接名使用 FTP。
2. ScriptExecutorsManager.getScriptExecutor('target-ftp') 的数据库语义不变，FTP 文件操作通过 storage。
3. 大文件内容不进入 JS。
4. beforeTask 每个任务只执行一次，失败不向下游发送记录。
5. 复制支持过滤、递归、建目录、覆盖、校验、重试和幂等。
6. 重试复用成功文件，不重复覆盖。
7. 非法路径、越权连接、敏感信息泄露和无能力协议被拒绝。
8. FTP 异常会使坏 session 失效并重建。
9. 连接初始化失败不会在每条事件中无限重复建连，退避和负缓存生效。
10. 同一连接并发首次访问不会创建多个 PDK executor。
11. Mongo aggregate、既有文件 source/target 回归测试通过。
12. FTPS 未完成时不会被错误宣称为已支持。

## 15. 最终结论

ScriptExecutorsManager.getScriptExecutor('target-ftp') 在没有 DAG FTP 节点时可以按连接名初始化连接级 PDK 节点，但当前只能执行 ExecuteCommandFunction 支持的数据库命令，不能直接像 Mongo aggregate 一样完成 FTP 文件操作。

可落地实现：

~~~text
保留 ScriptExecutor 数据库语义
        +
新增 PDK FileStorageFunction
        +
新增 StorageExecutorsManager / PdkFileStorageSessionManager
        +
复用并增强 DefaultFileOperationService
        +
在 enhanced JS 中增加 storage facade
        +
通过 beforeTask 执行一次性文件操作
        +
通过 ledger、临时发布、校验和 session 管理保证可靠性
~~~

该方案满足 TAP-12832 的 FTP 连接级操作需求，并为 SFTP、SMB、S3 等后续文件数据源提供统一扩展点。
