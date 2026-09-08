# TAP-12832：JS 节点操作 FTP 文件的详细设计

## 1. 文档信息

| 项目 | 内容 |
| --- | --- |
| Jira | [TAP-12832](https://tapdata.atlassian.net/browse/TAP-12832) |
| 类型 | 详细设计 / 可实施方案 |
| 范围 | JS 节点、文件连接、FTP 文件操作、文件存储公共能力 |
| 关联概要设计 | /Users/gavinxiao/kit/tapdata/tapdata/docs/jsNode/TAP-12832-ftp-storage-overview-design.md |

> 实现状态（2026-09-08）：共享文件 API、FileStorageFunction、文件 Connector 能力注册、FTP/SFTP 生命周期修复、引擎侧 session/operation service、PDK storage executor 缓存和 enhanced JS `storage` 注入已提交到对应仓库。本文件中的“当前实现”以 `StorageFacade`、`StorageExecutorsManager`、`PdkStorageExecutor` 和 `DefaultFileOperationService` 的源码为准；未实现的批量筛选、持久化幂等 ledger、`beforeTask`、feature flag 和指标审计不作为本期验收条件。

## 2. 目标、边界与核心结论

### 2.1 目标

支持用户在连接管理中新增 FTP 连接，并在 enhanced JS 节点中通过连接名称完成文件查询、存在性检查、写入、复制和删除。连接不要求出现在当前任务 DAG 中。用户可以根据每条 Tapdata 事件的业务逻辑，在 process(record) 中直接调用 storage；也可以根据事件内容把一个 FTP 的文件复制到另一个 FTP。大文件必须在 Java/connector 层流式传输，不得经过 JavaScript 堆。

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

当前 enhanced JS 向脚本绑定 ScriptExecutorsManager、source、target、env，并按输入记录调用 process。TAP-12832 的核心场景就是允许用户在 process(record) 中按照事件业务逻辑调用文件操作；因此不能把 storage.update/delete 限制为任务级 beforeTask，也不能默认把文件操作改造成一次性任务初始化逻辑。

### 3.2 文件 Connector 与 FTP

主要代码：

- /Users/gavinxiao/kit/tapdata/tapdata-connectors/connectors-common/file-connector-core/src/main/java/io/tapdata/common/FileConnector.java
- /Users/gavinxiao/kit/tapdata/tapdata-connectors/connectors-common/file-connector-core/src/main/java/io/tapdata/common/file/FileStorageFactory.java
- /Users/gavinxiao/kit/tapdata/tapdata-connectors/file-storages/ftp-file/src/main/java/io/tapdata/storage/ftp/FtpFileStorage.java

已有 FileStorageFactory、FileConnector、TapFileStorage、DefaultFileStorageSessionManager、DefaultFileOperationService 等基础，但存在以下缺口：

- engine 没有稳定的 PDK 文件能力入口，直接使用 FileStorageFactory 会越过 PDK/classloader 边界。
- TapFileStorage 源码与部分实现的 capabilities() API 需要对齐。
- 文件 operation API 在当前工作树中需要确认并归入真实共享源码模块，不能只依赖 target/classes。
- FtpFileStorage.readFile(String) 使用 retrieveFileStream() 后没有统一保证 completePendingCommand()，可能污染可复用连接。
- FTPClient 有状态，工作目录、控制连接和数据连接不应由多个操作无边界并发共享。
- ftpSsl 字段与实际 FTPS 实现不一致。
- 需要把连接复用、流式复制、临时文件、协议能力和坏 session 失效收敛到文件公共层；不在 JS 节点中增加任务级 ledger、事件门禁或业务级状态维护。

### 3.3 基于当前源码的文件数据源 Connector 审查与明确修复点

本节只记录已经从当前工作树源码确认的事实。文件路径和行号用于实现阶段定位；如果后续代码移动，必须同步更新对应测试和设计评审记录。没有在源码中确认的行为不作为本需求的实现前提。

#### 3.3.1 连接建立路径与 FileConnector 生命周期

当前连接建立链路是同步、真实建连的，不是只创建配置对象：

~~~text
FileConnector.initConnection()
  -> FileConnector.buildStorage()
  -> FileStorageFactory.build(protocol, connectionParams)
  -> TapFileStorageBuilder.build(...)
  -> fileStorage.init(params)
~~~

已确认的源码位置：

| 源码位置 | 当前行为 | 对本需求的影响 | 明确修复 |
| --- | --- | --- | --- |
| file-connector-core/src/main/java/io/tapdata/common/FileConnector.java:44-67 | initConnection 读取连接配置、构建 storage；buildStorage 直接进入 FileStorageFactory | 如果 JS 每次事件直接绕过连接级 manager 调用 buildStorage，就会每次真实初始化 connector/FTP 连接 | FileStorageFunction 只暴露连接级 storage handle；由 StorageExecutorsManager 按连接名称缓存并复用，不能在 process 的每次调用中直接 new storage |
| connectors-common/file-connector-core/src/main/java/io/tapdata/common/file/FileStorageFactory.java:20-32 | 根据 protocol 查找实现类，再由 TapFileStorageBuilder 创建实例 | FileStorageFactory 是 connector 内部工厂，不应由 engine 直接调用，否则会跨越 PDK/classloader 边界 | engine 只通过 PDK FileStorageFunction 取得能力；FileStorageFactory 只保留在 file connector/PDK 内部 |
| tapdata-common-lib/plugin-kit/tapdata-api/src/main/java/io/tapdata/file/TapFileStorageBuilder.java:26-56 | 反射实例化 TapFileStorage 并调用 init(params) | 证明“获取 storage”会触发实际连接初始化；不能把它当成无连接的轻量对象 | 增加单飞初始化、连接级缓存、坏 session 失效和关闭流程；在测试中验证同一连接并发首次访问只建一次 |
| connectors-common/file-connector-core/src/main/java/io/tapdata/common/FileConnector.java:90-101 | onStop 先 mergeCacheFiles、releaseResource，最后才 storage.destroy，没有 try/finally | merge 或 writer 释放异常时，storage.destroy 可能不执行，FTP/SFTP 等连接可能泄露 | 用 try/finally 保证 writer、storage、executorService 分别释放；释放异常只记录并继续清理其他资源 |
| connectors-common/file-connector-core/src/main/java/io/tapdata/common/FileConnector.java:297-315 | initMergeCacheFilesThread 创建单线程池；onStop 没有 shutdown；worker 中合并异常直接抛 RuntimeException | connector 停止后线程池可能继续存活；合并异常会结束 worker，但 executor 本身没有明确回收 | 保存 Future；onStop 调用 shutdownNow 并等待；worker 响应 interrupt；合并异常记录后按 connector 生命周期退出或进入可观测失败状态 |

#### 3.3.2 文件数据源 schema/discovery 的已确认资源问题

CSV、JSON、Excel 的 discoverSchema 在异常或提前 return 路径没有保证销毁 storage；XML 的实现已经使用 try/finally，可作为修复参照。这个问题与 JS storage 共享同一批 file storage 实现，必须在本需求的文件公共层优化中一并修复。

| 源码位置 | 已确认问题 | 明确修复与验证 |
| --- | --- | --- |
| connectors/csv-connector/src/main/java/io/tapdata/connector/csv/CsvConnector.java:80-108 | discoverSchema 初始化连接后，空文件/无首行等提前 return，异常也可能绕过末尾 storage.destroy() | 改为 try/finally；成功、空文件、解析异常三条路径都断言 storage.destroy 只执行一次 |
| connectors/json-connector/src/main/java/io/tapdata/connector/json/JsonConnector.java:119-134 | 与 CSV 相同，destroy 只位于正常路径末尾 | 使用与 XML 相同的 finally 结构，并增加异常路径资源测试 |
| connectors/xml-connector/src/main/java/io/tapdata/connector/xml/XmlConnector.java:85-104 | 已使用 try/finally 销毁 storage | 作为统一模板；同时补充 destroy 异常不遮蔽原始 schema 异常的测试 |
| connectors/excel-connector/src/main/java/io/tapdata/connector/excel/ExcelConnector.java:206-228 | 初始化后异常或提前 return 可能绕过 storage.destroy() | 改为 try/finally，并覆盖 workbook、input stream、storage 的关闭顺序和异常路径 |
| connectors-common/file-connector-core/src/main/java/io/tapdata/common/FileSchema.java:22-53 | 创建固定线程池；InterruptedException 路径可能在 shutdown 前抛出 | finally 中 shutdownNow 并恢复 interrupt；增加中断场景测试 |
| connectors-common/file-connector-core/src/main/java/io/tapdata/common/AbstractFileRecordWriter.java:33-49,141-144 | writer 创建 localStorage；资源由 releaseResource 释放 | 与 FileConnector.onStop 的 finally 改造一起验证 localStorage、writer、临时文件都被释放；不能只验证 FTP 主连接 |

#### 3.3.3 FTP 实现中的连接、流和有状态会话问题

以下问题直接来自 FtpFileStorage 和 FtpConfig，不是对 FTP 库行为的推测。

| 源码位置 | 已确认问题 | 设计中的修复 |
| --- | --- | --- |
| file-storages/ftp-file/src/main/java/io/tapdata/storage/ftp/FtpFileStorage.java:29-49 | init 创建 FTPClient、connect、login、设置编码/被动模式/二进制模式；只有收到非正响应时显式 disconnect。connect、login 或后续设置抛异常时，没有统一的 finally 清理部分初始化 client | init 使用局部 client 或 try/catch 清理 partial client；connect、login、setControlEncoding、enterLocalPassiveMode、setFileType 的返回值和异常统一映射；失败一定 disconnect |
| ftp-file/src/main/java/io/tapdata/storage/ftp/FtpFileStorage.java:53-61 | destroy 只处理已连接 client；对部分初始化、重复 destroy、logout 异常的处理不够明确 | 实现幂等 close；无论 connected、登录失败或数据命令失败，都能安全 disconnect；close 失败产生指标但不阻断其他资源释放 |
| ftp-file/src/main/java/io/tapdata/storage/ftp/FtpFileStorage.java:111-117 | raw readFile 返回 retrieveFileStream()；该方法本身没有在流关闭时完成 completePendingCommand() | raw stream 必须包装为受管流，在 close 中完成 pending command 且 close 幂等；若 close/pending command 失败，使当前 session 失效。优先使用回调式 readFile，回调方法现有 try-with-resources 并在 finally 调用 completePendingCommand |
| file-stream-connector/src/main/java/io/tapdata/connector/json/FileStreamConnector.java:42-55,90-106 | readOneFile 使用 raw storage.readFile(path) 把输入流放入 file_data；写出时才由 try-with-resources 关闭 | 明确 stream 所有权：FileStreamConnector 改为 callback/受管 stream；即使保留 raw API，也必须依赖 FtpFileStorage 的 close wrapper，测试“读取后关闭”确实触发 pending command |
| ftp-file/src/main/java/io/tapdata/storage/ftp/FtpFileStorage.java:119-122,129-132,175-211 | isFileExist、delete、递归列表等方法未全部 synchronized；FTPClient 的 current working directory 是共享可变状态 | 首期同一 storage session 的操作串行化，或把每个操作需要的目录切换和恢复封装在公共 adapter；不允许多个 JS 事件无边界并发操作同一个有状态 FTPClient |
| ftp-file/src/main/java/io/tapdata/storage/ftp/FtpFileStorage.java:135-147,175-215 | isDirectoryExist/changeWorkingDirectory 会改变 FTPClient 当前目录，后续相对路径操作依赖隐含状态 | 统一使用 root 下规范化相对路径；在 adapter 内部串行执行 cwd/list/store；补充并发、失败后 cwd 恢复和跨操作路径隔离测试 |
| ftp-file/src/main/java/io/tapdata/storage/ftp/FtpFileStorage.java:149-172 | openFileOutputStream 的 wrapper 只显式覆盖 write(int) 和 close；close 后 completePendingCommand，但没有幂等关闭、bulk write/flush 和失败后的 session 失效策略 | 实现完整 OutputStream 委托、flush、bulk write、幂等 close；pending command 或远端写失败时 invalidate session，并保证临时文件清理 |
| ftp-file/src/main/java/io/tapdata/storage/ftp/FtpFileStorage.java:175-211 | getFilesInDirectory 递归参数 batchSize 没有统一的正数校验；递归过程中依赖当前目录和实时 FTPClient 状态 | 公共文件服务校验 limits；adapter 对 batchSize 做正数/default 处理；断连时返回稳定的 FILE_REMOTE_IO/FILE_CONNECT_FAILED |
| ftp-file/src/main/java/io/tapdata/storage/ftp/FtpConfig.java:9-23 与 FtpFileStorage.java:237-243 | FtpConfig 没有 encoding 默认值，而 encodeISO 直接使用 ftpConfig.getEncoding() | 配置归一化时默认 UTF-8，校验 host、port、encoding、timeout；不让 null/非法 charset 以未映射 RuntimeException 泄露 |
| ftp-file/src/main/java/io/tapdata/storage/ftp/FtpFileStorage.java:29-39 | ftpSsl 为 true 时仍然创建 FTPClient，只改变 login 的调用参数，没有创建 FTPSClient/TLS 会话 | 首期未实现 FTPS 时返回明确“不支持 FTPS”错误；完成 FTPSClient、证书校验和集成测试前，不得把 ftpSsl 宣称为安全连接 |
| ftp-file/src/main/java/io/tapdata/storage/ftp/FtpFileStorage.java:227-243 | 当前 FTP 声明 ATOMIC_RENAME、MAKE_DIRECTORY，并存在 capabilities 实现 | 能力矩阵只声明已实现并测试通过的能力；补充 READ、WRITE、LIST、DELETE、MAKE_DIRECTORY、ATOMIC_RENAME 的协议测试，不默认声明 CHECKSUM/APPEND |

#### 3.3.4 文件 session manager 与公共文件操作服务

| 源码位置 | 已确认问题 | 明确修复 |
| --- | --- | --- |
| connectors-common/file-connector-core/src/main/java/io/tapdata/common/file/DefaultFileStorageSessionManager.java:27-50 | retain 超过 maxSessions 直接报 FILE_SESSION_LIMIT；没有空闲回收 | 增加 lastAccess、idle eviction 和可观测淘汰；maxSessions 仍保留为硬上限，避免无限建连 |
| DefaultFileStorageSessionManager.java:79-87 | session key 需要避免线程绑定和明文配置泄露 | 当前实现按 protocol、rootPath 和 endpoint params 的 SHA-256 fingerprint 建 key；不使用线程 ID，key 和日志不输出原始参数。连接配置变更由新的连接 executor 生命周期承接，当前 manager 没有动态配置版本监听 |
| DefaultFileStorageSessionManager.java:52-70 | release 只减少引用；invalidate 直接 remove/destroy，即使仍有引用 | invalidate 改为 mark-draining；新请求不再取得旧 session，待引用归零后销毁；必要时由坏 session 触发新 session 建立 |
| DefaultFileStorageSessionManager.java:72-77 | destroy 吞掉所有异常 | 保留清理不中断语义，但增加 debug/error 指标和连接标识；测试 destroy 异常不会阻塞其他 session 关闭 |
| common/file/DefaultFileOperationService.java:31-53 | copy 重试 copyOnce，但重试前没有 sessions.invalidate；坏连接可能被重复复用 | 仅对可恢复远端错误执行 invalidate 后重试；参数、权限、路径和能力错误不重试 |
| DefaultFileOperationService.java:86-127 | copy 通过临时路径写入、校验并 move；finally 删除临时文件，但清理失败无单独诊断；move 后返回的元数据来自临时文件 | 增加 temp cleanup failed 指标；move 后对最终路径做 final stat；临时路径使用 operationId，避免线程/纳秒碰撞和跨调用残留 |
| DefaultFileOperationService.java:140-160 | copyBatch 固定 MAX_BATCH_FILES=100，按顺序处理，首个异常即停止 | 将批量上限和失败策略显式化；返回已完成/失败项，不能把“部分成功”伪装成整体成功；JS facade 只透传结果，不维护事件 ledger |
| DefaultFileOperationService.java:60-84,129-160 | list/stat/exists/copy 等操作都通过 session manager retain/release，但远端异常没有统一 invalidate 入口 | 公共 operation 层统一错误分类：连接断开/远端 I/O 使 session 失效；业务错误保留原 session |

#### 3.3.5 文件能力 API 与 Connector 注册的源码基线及落地结果

开发前确认过以下源码不一致，并已在 T1/T2 中收口：

- TapFileStorage.java:13-129 的接口源码没有 capabilities()。
- FtpFileStorage.java:227-230 已有 @Override capabilities()。
- DefaultFileOperationService.java:86 已调用 target.getStorage().capabilities()。
- file operation API 的 io.tapdata.file.operation 源码目录在当前工作树中未找到，但 file-connector-core 已导入这些类型。

已落地结果：

1. `TapFileStorage.capabilities()` 和 `io.tapdata.file.operation` DTO/错误码/服务接口已归入 `tapdata-common-lib/plugin-kit/tapdata-api`。
2. `FileStorageFunction` 已加入 PDK API，`ConnectorFunctions` 已提供注册和 getter。
3. `FileConnector.registerFileStorageFunction()` 已由 CSV、JSON、XML、Excel、File Stream Connector 调用。
4. 协议能力仍以各实现真实返回值为准；engine 的 JS storage 当前只允许 FTP，其他协议由 `PdkStorageExecutor` 返回 `FILE_UNSUPPORTED_OPERATION`。

五类文件 Connector 的统一注册已完成：

| Connector | 当前注册位置 | 已确认现状 | 修复 |
| --- | --- | --- | --- |
| CSV | csv-connector/src/main/java/io/tapdata/connector/csv/CsvConnector.java:62-77 | `registerCapabilities()` 调用统一注册入口 | 已完成；FileStorageFunction 返回 connector 已初始化的 storage |
| JSON | json-connector/src/main/java/io/tapdata/connector/json/JsonConnector.java:102-116 | `registerCapabilities()` 调用统一注册入口 | 已完成 |
| XML | xml-connector/src/main/java/io/tapdata/connector/xml/XmlConnector.java:68-82 | `registerCapabilities()` 调用统一注册入口 | 已完成 |
| Excel | excel-connector/src/main/java/io/tapdata/connector/excel/ExcelConnector.java:177-203 | `registerCapabilities()` 调用统一注册入口 | 已完成 |
| File Stream | file-stream-connector/src/main/java/io/tapdata/connector/json/FileStreamConnector.java:58-73 | `registerCapabilities()` 调用统一注册入口 | 已完成；raw stream 仍按对应 connector 的实现负责关闭 |

#### 3.3.6 SFTP 等其他文件实现的同步优化边界

本需求以 FTP 为首个落地协议，但整体文件公共层不能把 FTP 问题复制到其他实现。当前源码已确认的 SFTP 问题如下：

| 源码位置 | 已确认现状 | 设计要求 |
| --- | --- | --- |
| file-storages/sftp-file/src/main/java/io/tapdata/storage/sftp/SftpFileStorage.java:15-40 | StrictHostKeyChecking 被硬编码为 no；session timeout 硬编码 10000；初始化失败调用 destroy | 增加 host key 校验配置，默认安全模式；timeout 纳入统一 FileConnectionConfig；保留初始化失败清理并增加测试 |
| SftpFileStorage.java:53-98,192-198 | channel.cd 改变共享 current directory；raw readFile 返回 channel.get 流 | 与 FTP 相同，按 storage session 串行化有状态操作；raw stream 使用受管 close 或强制 callback API |
| SftpFileStorage.java:101-126 | move 直接 UnsupportedOperationException；delete 把所有 SftpException 都当成 false | 能力矩阵不得声明 MOVE/ATOMIC_RENAME；区分 not found、权限错误和连接错误，不能吞掉远端异常 |

以上是公共文件层的代码修复，不是 JS 节点业务校验；JS 只负责调用 facade 和处理用户脚本选择的结果/异常。当前 SFTP 已完成边界修复，但没有在 `PdkStorageExecutor` 中开放给 JS；本期仍只开放 FTP。

## 4. 总体架构

~~~text
JS process(record)
        |
        v
StorageFacade：JS 文件操作 API
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
3. Connector 提供协议能力；公共服务负责路径、遍历、复制、校验、连接级重试和 session 管理。
4. JS 只传递普通 JSON；大文件永远不经过 GraalJS。
5. 写入默认先写临时文件，校验后发布；协议不支持原子移动时由文件服务按能力返回结果。
6. 文件服务不记录每条 Tapdata 事件的持久化操作状态，业务是否对某个事件执行文件操作由用户脚本决定。

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

实际缓存分两层：

~~~text
StorageExecutorsManager:
  trimmed connectionName -> CompletableFuture<StorageExecutor>
  作用域：一个 JS processor node 实例

DefaultFileStorageSessionManager:
  protocol + rootPath + SHA-256(endpoint.params)
  作用域：一个 StorageExecutor
~~~

`StorageExecutorsManager` 当前通过连接名称查询 `Connections`，并用单飞 future 防止并发首次访问重复创建 PDK；创建失败有 1 秒连接级退避。当前实现没有动态配置版本监听，连接配置变更应通过任务/节点重启或显式 `invalidate(connectionName, cause)` 使旧 executor 失效。密码、私钥和完整连接参数不进入日志；session key 只保存不可逆 fingerprint。

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

### 6.4 连接缓存与事件级性能

当前 ScriptExecutorsManager 的缓存行为说明了文件执行器应该如何设计：

- getScriptExecutor(connectionName) 通过 CacheMap 按连接名称缓存 ScriptExecutor。
- 当前缓存最大 10 个连接，空闲过期值为 600；过期或容量淘汰时会调用 ScriptExecutor.close()。
- HazelcastJavaScriptProcessorNode 在 shared init 阶段创建一个 ScriptExecutorsManager，正常事件不会为每条记录创建新的 manager。
- 同一个 manager 中，同名连接首次访问会触发 Connections 查询、PdkUtil.createNode、connectorInit 和 PDK 能力获取；后续缓存命中不会重复创建 PDK。
- 缓存是 JVM/processor 实例本地缓存，不是跨 Hazelcast worker 的全局缓存。多个 worker、任务重启或任务重平衡会分别创建自己的 PDK 节点。

TAP-12832 允许 process(record) 按事件调用文件操作，因此要区分连接创建成本和文件操作成本：

| 调用情况 | 预期行为 |
| --- | --- |
| 首次使用某个连接 | 创建连接级 StorageExecutor/PDK 节点并建立 FTP session |
| 后续事件使用同一连接 | 命中缓存，复用 PDK 节点和 session；本次文件读写仍然按事件执行 |
| 用户根据事件选择不同连接 | 按实际使用的连接分别缓存，不能假设只有一个固定 target |
| PDK 初始化失败 | 不应重复创建无界资源；由连接管理器返回初始化错误并做连接级退避 |
| 远程 session 失效 | 只失效当前连接 session，下次操作重新建立 |
| 多 worker 或任务重启 | 每个本地执行实例有自己的连接缓存，连接数按实例数量放大 |

StorageExecutorsManager 只需要提供连接级资源管理，不承担业务级事件验证或任务级状态维护：

1. 按连接名称缓存 StorageExecutor；一个 manager 实例内同名连接只创建一个 executor。
2. 同一连接并发首次访问使用单飞初始化，避免同时创建多个 PDK/FTP session。
3. 连接初始化失败采用有限的连接级退避，避免异常时每条事件重复登录；退避不改变用户脚本的事件语义。
4. 远程 I/O 失败后让 session 失效并在下一次操作重建。
5. 一次 copy 调用内复用 source/target session，但不记录每条事件的持久化状态。
6. manager 关闭时关闭 executor、session、底层 storage 和 PDK associate id；命中/创建指标暂未接入本期实现。

这里的缓存只解决连接和 PDK 创建成本，不阻止用户根据事件执行文件操作。文件操作本身的次数和业务条件由 JS 脚本决定。

## 7. 文件操作公共服务

### 7.1 模块边界

现有公共类主要位于：

/Users/gavinxiao/kit/tapdata/tapdata-connectors/connectors-common/file-connector-core/src/main/java/io/tapdata/common/file

共享的文件 operation API 位于 tapdata-api 的 io/tapdata/file/operation 包；当前工作树需要确认这些 API 的源文件是否完整。

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

当前实现支持最大 session 数、空闲回收、重复 close 保护、连接失败 invalidate 和初始化异常清理；session key 使用 protocol、rootPath 和非敏感 fingerprint，不绑定线程。配置动态变更监听尚未实现，应通过上层 executor 失效/重建处理。

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
8. 低层服务执行单次已解析复制，是否根据业务结果继续处理由 JS 脚本决定。

## 8. JS API 与生命周期

### 8.1 API

增强 JS 暴露：

~~~javascript
storage.find(connectionName, query, options)
storage.exists(connectionName, path)
storage.update(targetConnectionName, data, options)
storage.delete(connectionName, data, options)
~~~

首期 `update` 支持按事件进行 `write` 和 `copy`；`find` 当前返回单个文件/目录 metadata，`exists` 和 `delete` 是独立方法，不提供 `list` facade。接口不暴露 getConnection、openStream、getRawClient。storage 调用可以出现在 process(record) 中，操作结果同步返回；连接和 session 由 Java 文件服务复用。

推荐脚本：

~~~javascript
function process(record) {
  if (record.needTransfer) {
    storage.update('target-ftp', {
      action: 'copy',
      source: {
        connection: 'source-ftp',
        path: record.sourcePath
      },
      target: {
        path: record.targetPath
      }
    }, {
      overwrite: 'skip',
      verify: 'size',
      retryTimes: 2
    });
  }
  return record;
}
~~~

update 选项：

| 字段 | 说明 |
| --- | --- |
| overwrite | skip、overwrite、fail，默认 skip |
| verify | none、size、checksum，默认 size |
| preservePath、recursive、pattern/include/exclude、maxFiles/maxBytes | 当前 facade 尚未实现；不要传入后假设会生效 |
| timeoutMs/retryTimes | 超时和可重试次数；当前 copy 支持，write 使用底层单次写入 |
| operationId | 当前 facade 尚未对外提供；不持久化为每事件 ledger |
| dryRun | 只列出计划，不写入 |
| failFast | 当前 facade 尚未实现批量操作 |

大文件内容不进入 JS，返回值只包含状态、文件摘要、字节数、耗时和错误。

### 8.2 事件级调用与可选 beforeTask

~~~text
process(record)
  -> 根据事件字段决定是否调用 storage
  -> StorageFacade 解析请求
  -> StorageExecutor 复用连接级 PDK/session
  -> FileOperationService 执行 FTP 文件操作
  -> 返回结果或抛出本次操作异常
~~~

storage 的绑定由 enhanced JS 的 buildEngine() 完成。process(record) 每次执行时可以根据事件内容调用 storage.update、storage.find、storage.exists 或 storage.delete。用户是否调用、调用哪一个连接、操作哪一个文件，属于脚本的业务逻辑，不由 JS 节点额外维护任务状态。

beforeTask 可以作为后续可选扩展，用于用户主动编写一次性任务初始化操作，但不是 TAP-12832 的必要生命周期，也不能替代 process 中的事件级文件操作。

doClose() 负责关闭 StorageExecutorsManager、StorageExecutor 和关联 PDK/session。关闭动作不依赖某个事件是否调用过 storage。

### 8.3 JS 脚本编写示例与协议范围

以下代码是当前实现可直接使用的脚本契约。storage 由 enhanced JS 运行时注入，用户只传连接名称和普通 JSON/字符串参数，不获取 ScriptExecutor、FTPClient、PDK 节点或 Java stream。

#### 8.3.1 按事件直接写入 FTP

适合事件内容本身就是需要落盘的小型文本/JSON。content 由 Java 文件服务转换为输入流；大文件不应通过 JS content 传递。

~~~javascript
function process(record) {
  var path = '/out/' + record.id + '.json';
  var result = storage.update('target-ftp', {
    action: 'write',
    target: {
      path: path
    },
    content: JSON.stringify(record.payload),
    contentType: 'application/json'
  }, {
    overwrite: 'overwrite',
    verify: 'size',
    retryTimes: 2
  });

  record.fileOperation = {
    status: result.status,
    path: path,
    bytes: result.bytes
  };
  return record;
}
~~~

#### 8.3.2 按事件把一个 FTP 文件复制到另一个 FTP

源文件和目标文件可以属于两个不同连接；目标连接作为 update 的第一个参数，源连接在 data.source.connection 中声明。

~~~javascript
function process(record) {
  if (!record.sourcePath || !record.targetPath) {
    return record;
  }

  var result = storage.update('target-ftp', {
    action: 'copy',
    source: {
      connection: 'source-ftp',
      path: record.sourcePath
    },
    target: {
      path: record.targetPath
    }
  }, {
    overwrite: 'skip',
    verify: 'size',
    timeoutMs: 10 * 60 * 1000,
    retryTimes: 2
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
~~~

#### 8.3.3 查询、存在性检查和删除

这些调用同样按连接名称解析，不要求连接出现在 DAG 中：

~~~javascript
function process(record) {
  var exists = storage.exists('source-ftp', record.sourcePath);
  if (exists) {
    var metadata = storage.find('source-ftp', {
      path: record.sourcePath
    });
    record.fileSize = metadata ? metadata.size : null;
  }

  if (record.deleteSource === true && exists) {
    storage.delete('source-ftp', {
      path: record.sourcePath
    });
  }
  return record;
}
~~~

#### 8.3.4 与 ScriptExecutorsManager.getScriptExecutor 的关系

两者不是同一个脚本入口：

~~~text
数据库命令：
  ScriptExecutorsManager.getScriptExecutor('mongo-test').aggregate(...)
    -> ScriptExecutor
    -> ExecuteCommandFunction
    -> 数据库 Connector 命令实现

文件操作：
  storage.update('target-ftp', ...)
    -> StorageFacade
    -> StorageExecutorsManager
    -> PDK FileStorageFunction
    -> DefaultFileOperationService
    -> FTP/SFTP/其他文件 storage
~~~

当前 ScriptExecutor 只调用 ExecuteCommandFunction；源码中 FTP Connector 没有注册该数据库命令函数，因此不能通过下面的写法获得 FTP 文件能力：

~~~javascript
var ftp = ScriptExecutorsManager.getScriptExecutor('target-ftp');
ftp.update(...);
~~~

除非后续明确把文件能力另行挂载到 ScriptExecutor，否则不建议扩展通用 ScriptExecutor 的方法集合。文件 API 使用独立 storage 门面，可以避免把数据库 command、文件流式复制和协议能力混成一个接口。

#### 8.3.5 首期与后续协议支持

当前需求的首期用户可用范围是 FTP：

| 连接类型 | 首期是否可通过 storage 使用 | 前置条件 |
| --- | --- | --- |
| FTP | 是 | Connector 注册 FileStorageFunction；FTP 初始化、流关闭、session 复用和能力矩阵修复完成 |
| SFTP | 否，当前明确不开放 | SFTP 生命周期边界已修复，但 engine storage executor 当前只允许 FTP |
| SMB、S3、NFS、OSS 等文件连接 | 否，预留扩展 | 各自实现 FileStorageFunction、TapFileStorage 能力和统一 operation API |
| 数据库连接 | 否 | 继续使用 ScriptExecutorsManager.getScriptExecutor 和数据库命令 API |

因此，当前实现是“公共 storage 契约 + FTP 首期落地”：用户现在只能通过 storage 操作 FTP；SFTP/SMB/S3 等只有在 engine 明确放开协议、完成 FileStorageFunction 接入和集成测试后才能使用。协议差异应由 adapter 和能力矩阵处理，不在 JS 中写协议特判。

## 9. 文件操作执行语义

### 9.1 文件级重试

重试属于文件公共服务和协议 session 层，只处理连接断开、临时网络错误、远程读写失败等可恢复错误。连接不存在、权限错误、路径错误和参数错误直接返回给当前 JS 调用。

重试不创建任务级 Mongo 记录，也不改变用户按事件调用 storage 的语义。每次 process 调用得到本次文件操作的成功结果或异常；是否跳过当前事件、记录业务失败或继续处理，由用户脚本决定。

### 9.2 临时文件与发布

这里的“发布”只表示把一个已经写完并校验通过的临时文件移动为最终文件，不是任务发布，也不是软件发布：

~~~text
确保目标父目录
  -> 写入 .tapdata-tmp/<operationId>/<safe-name>.part
  -> size/checksum 校验
  -> move 到最终路径
  -> final stat
~~~

overwrite=skip、overwrite=overwrite、overwrite=fail 是当前一次 storage.update 调用的操作选项。目标是否存在、临时文件是否清理、移动是否支持，由文件操作服务处理；不新增跨事件的持久化 ledger。

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

JS 不得读取连接配置；日志、异常和指标标签不得包含 password、private key、token 或完整连接 URL。连接缓存只保存运行时对象和不可逆配置版本标识。

storage.delete 默认只允许单文件或受限列表；递归删除需要显式 recursive=true 且受系统开关控制；预览和测试禁止真实删除；删除必须写审计日志。

建议指标：

- storage_operation_total{action,protocol,status}
- storage_operation_failed{errorCode,protocol}
- storage_operation_bytes_total{protocol}
- storage_operation_duration_ms{action,protocol}
- storage_session_active{protocol}
- storage_session_eviction_total{reason}
- storage_temp_cleanup_failed_total

日志至少包含 taskId、nodeId、operationId、连接 ID、相对路径、action、status、errorCode、耗时、文件数和字节数；禁止包含密码、私钥、token 和完整连接 URL。

## 12. 代码改造清单

| 模块 | 主要改造 |
| --- | --- |
| tapdata-pdk-api | FileStorageFunction；ConnectorFunctions 注册、getter 和 capability |
| tapdata-api/shared-file-api | TapFileStorage API 对齐；FileEndpoint、FileCopyRequest、错误码、状态和能力源码归位 |
| file-connector-core | FileConnector 统一注册；配置 mapper；可注入 session manager；onStop finally 清理；merge executor shutdown；FileSchema 中断清理 |
| CSV/JSON/XML/Excel/FileStream Connector | 注册 FileStorageFunction；CSV/JSON/Excel discoverSchema 使用 finally；FileStream raw stream 所有权和 close 语义修复 |
| ftp-file | stream pending command、初始化失败 disconnect、timeout/encoding 校验、session 并发、OutputStream 幂等 close、FTPS 边界 |
| sftp-file 及其他 file storage | 能力矩阵对齐；SFTP host key 校验、超时配置、错误分类和受管 stream；没有实现的 move/atomic rename 不得声明 |
| DefaultFileStorageSessionManager | 非敏感配置 key、跨 worker 复用、idle eviction、mark-draining invalidate、清理失败可观测 |
| DefaultFileOperationService | 远端失败 invalidate 后重试、final stat、临时文件清理指标、批量部分成功语义 |
| engine V2 | ConnectionResolver、PdkConnectionHandle、StorageExecutorsManager |
| engine file operation | PdkFileStorageSessionManager、StorageFacade、DefaultFileOperationService 增强；单飞初始化、连接级退避和 session 失效 |
| HazelcastJavaScriptProcessorNode | storage binding、事件级 process 调用、关闭流程 |

## 13. 测试设计

### 13.1 单元测试

- ConnectorFunctions 可注册、获取 FileStorageFunction，capability 名称稳定。
- 没有 DAG 节点时按连接名初始化文件 PDK。
- 同一个连接的并发首次访问只创建一个 PDK executor，其余调用复用或等待。
- PDK/FTP 初始化失败后，连接级退避避免无界重复建连。
- 修复连接配置后，连接级退避结束能够重新初始化成功。
- 非文件连接、不存在连接、无权限、缺少能力均返回稳定错误。
- PDK 初始化失败时资源完整释放。
- FileConnector.onStop 在 merge/release 抛异常时仍销毁 storage，并关闭 merge executor。
- CSV、JSON、Excel discoverSchema 在成功、提前 return 和异常路径都销毁 storage；XML 回归不受影响。
- FTP connect/login/配置初始化失败时释放 partial client；destroy 可重复调用。
- FTP raw input/output stream close 只完成一次 pending command；bulk write、flush、close 异常会使 session 失效。
- session key 不含线程 ID、密码和私钥；同一连接跨 worker 可复用，空闲 session 可回收。
- copy 发生可恢复远端异常时先 invalidate 再重试；最终路径执行 final stat；临时清理失败可观测。
- storage DTO 映射、路径安全、overwrite、verify、preservePath、dryRun 和 limits 正确。
- copy、copyBatch、list、exists、stat、validate、临时清理、checksum、session invalidate 正确。
- 批量部分成功结果完整。

### 13.2 FTP 集成测试

使用本地 FTP server 或 Testcontainers 覆盖单/多文件、递归、Unicode 文件名、目标目录创建、skip/overwrite/fail、size 校验、控制连接断开重试、临时文件清理、session 复用和配置变更失效。FTPS 未实现时必须明确失败，不得建立明文替代连接。

### 13.3 JS 节点测试

- enhanced JS 有 storage，standard JS 没有。
- process 可以根据事件字段决定是否调用 storage。
- 同一事件可把 source FTP 文件复制到 target FTP。
- 不同事件可使用不同连接名和不同文件路径。
- process 中重复调用连接名解析时命中已有 StorageExecutor，不重新建连。
- FTP 文件操作结果可以返回给 JS，异常可以被 JS 捕获或让当前事件失败。
- 多 worker 场景下每个 worker 的连接数受 session 生命周期控制。
- 连接级初始化失败不会在异常时无界重复登录。
- 不产生按 Tapdata 事件写入的 file_operation_ledger。

### 13.4 端到端验收

~~~text
Tapdata 事件 -> JS process(record)
            -> 按业务条件调用 storage.update
            -> source FTP 文件 -> target FTP 文件
            -> 返回当前事件处理结果
~~~

必须验证：DAG 没有 FTP 节点也能使用连接列表中的 FTP；脚本能够按事件条件执行直接写入或跨 FTP 复制；同一连接的事件级调用复用 PDK/session；连接初始化异常不会无限重复建连；日志不泄露密码和完整连接串。

## 14. 发布、回滚与验收标准

### 14.1 Feature flag

建议使用：

- js.node.storage.enabled
- js.node.storage.write.enabled
- js.node.storage.ftp.enabled

旧 JS、Mongo aggregate 和既有 file source/target 默认不变；standard JS 不自动获得 storage。是否在测试/预览任务中允许真实写操作，沿用现有任务运行模式，不在本需求中新增 beforeTask 门禁。

### 14.2 发布顺序

1. 统一 TapFileStorage 和文件 operation API 源码。
2. 完成 FileStorageFunction、ConnectorFunctions 和全部文件 Connector 注册。
3. 完成 PDK-backed session manager 和公共文件服务。
4. 完成 StorageFacade 和事件级 process 调用。
5. 完成 FTP 集成、连接恢复、指标和审计。
6. 使用 feature flag 灰度开启 FTP storage 写能力。

### 14.3 回滚

关闭 storage 开关后，新 JS 文件操作停止；旧数据库 JS 和既有文件 source/target 不受影响。

### 14.4 验收标准

1. 无 DAG FTP 节点时，授权 JS 可以按连接名使用 FTP。
2. ScriptExecutorsManager.getScriptExecutor('target-ftp') 的数据库语义不变，FTP 文件操作通过 storage。
3. process(record) 可以根据事件逻辑直接写入 FTP，也可以在两个 FTP 连接之间复制文件。
4. 大文件内容不进入 JS。
5. 同一连接的事件级调用复用 StorageExecutor、PDK 和 FTP session。
6. 复制支持单文件、覆盖策略、size 校验、临时文件清理和连接级重试；过滤、递归批量复制和 checksum 不属于当前 facade 已交付能力。
7. 不产生按 Tapdata 事件写入的 file_operation_ledger。
8. 连接初始化失败不会无界重复建连；修复配置后可以重新初始化。
9. FTP 异常会使坏 session 失效并重建。
10. Mongo aggregate、既有文件 source/target 回归测试通过。
11. FTPS、SFTP 和其他未开放协议不会被错误宣称为已支持。

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
在 enhanced JS 的 process(record) 中增加 storage facade
        +
通过文件公共层的流式传输、临时文件、校验和 session 管理支持事件级操作
~~~

该方案满足 TAP-12832 的事件级 FTP 文件操作需求，并为 SFTP、SMB、S3 等后续文件数据源提供统一扩展点。任务级 ledger、beforeTask 和跨重启幂等不属于本需求首期范围。
