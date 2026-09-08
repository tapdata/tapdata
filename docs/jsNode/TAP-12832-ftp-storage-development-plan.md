# TAP-12832 FTP 文件操作支持研发实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 执行本计划。每个任务完成后必须独立测试并通过评审。

**目标：** 在 enhanced JS 节点中支持用户按连接名称、按 Tapdata 事件调用文件操作；首期落地 FTP，支持直接写入、查询、存在性检查、删除，以及 FTP 到 FTP 的流式复制。

**架构：** 保留现有 ScriptExecutorsManager/ScriptExecutor 的数据库命令语义；新增 FileStorageFunction、StorageExecutorsManager、PDK-backed file session manager 和 StorageFacade。JS 只调用 storage API，Java/Connector 层负责连接复用、流式传输、临时文件、协议能力和资源释放。

**技术栈：** Java、Tapdata PDK、TapFileStorage、Apache Commons Net FTPClient、现有 enhanced JS/GraalJS 运行时、现有 Connector 测试框架、FTP 集成测试容器或本地 FTP server。

**设计依据：** [TAP-12832 详细设计](./TAP-12832-ftp-storage-detailed-design.md)

## 全局约束

- 首期用户可用协议为 FTP；SFTP、SMB、S3、NFS、OSS 只完成公共能力预留或明确不支持，不得提前宣称支持。
- 文件操作通过 storage facade，不把 FTP 操作伪装成 ScriptExecutor 的数据库 command。
- storage 必须可以在 process(record) 中调用；不能只提供 beforeTask。
- 不在 JS 节点维护按 Tapdata 事件写入的 file_operation_ledger、跨事件幂等状态或任务级业务状态。
- 大文件内容不经过 JavaScript 堆；跨 FTP 复制必须在 Java/Connector 层流式完成。
- 同一文件 storage session 的连接资源必须可复用、可关闭、可失效；不得每条事件重新创建 PDK/FTP 连接。
- 所有初始化失败、异常退出、重试和任务关闭路径都必须释放连接、stream、executor 和临时文件。
- ftpSsl 在 FTPSClient、证书校验和集成测试完成前不得被标记为已支持。
- JS 只传连接名称和普通 JSON/字符串参数，不暴露 FTPClient、PDK 节点、连接凭据或 Java stream。

---

## 1. 研发范围和交付边界

### 1.1 首期交付

| 能力 | 首期要求 |
| --- | --- |
| 连接解析 | 按连接名称解析，不要求连接出现在任务 DAG |
| JS 调用位置 | enhanced JS 的 process(record) |
| 目标文件写入 | 支持小型文本/JSON content 写入 FTP；大文件不通过 JS content |
| 文件复制 | 支持 source FTP 到 target FTP 的单文件和受限批量流式复制 |
| 查询 | exists、find/stat、list 的公共 API；首期按已实现能力暴露 |
| 删除 | 显式 delete；不默认删除源文件 |
| 覆盖策略 | skip、overwrite、fail |
| 校验 | none、size；checksum 只有协议实现并测试通过后才能开启 |
| 连接性能 | PDK、storage session、FTP 控制连接按连接配置版本复用 |
| 资源治理 | raw stream close、completePendingCommand、partial init cleanup、session invalidate、executor shutdown |
| 失败处理 | 文件级有限重试；坏 session 先失效再重试 |
| 观测 | 连接创建/复用/失效、操作耗时、字节数、临时文件清理失败指标 |

### 1.2 明确不属于首期

- 任务级 beforeTask 作为 FTP 操作的唯一入口。
- 每事件持久化幂等 ledger。
- 跨任务重启的业务幂等。
- 任意 Java stream 或 FTPClient 暴露给 JS。
- FTPS。
- 将所有 file connector 一次性改造成全部可用协议。
- 通过 ScriptExecutorsManager.getScriptExecutor('target-ftp').update() 直接执行 FTP 文件操作。

---

## 2. 模块和源码边界

| 模块 | 当前源码位置 | 本计划负责内容 |
| --- | --- | --- |
| PDK API | /Users/gavinxiao/kit/tapdata/tapdata-common-lib/plugin-kit/tapdata-pdk-api | FileStorageFunction、ConnectorFunctions 注册和 getter |
| 文件共享 API | /Users/gavinxiao/kit/tapdata/tapdata-common-lib/plugin-kit/tapdata-api | TapFileStorage.capabilities()、FileEndpoint、FileCopyRequest、错误码和状态源码归位 |
| 文件 Connector 公共层 | /Users/gavinxiao/kit/tapdata/tapdata-connectors/connectors-common/file-connector-core | FileConnector 生命周期、session manager、公共文件 operation、配置映射 |
| FTP Connector | /Users/gavinxiao/kit/tapdata/tapdata-connectors/file-storages/ftp-file | FTP 初始化、stream、并发、编码、能力和关闭 |
| 文件 source/target Connector | /Users/gavinxiao/kit/tapdata/tapdata-connectors/connectors/csv-connector、json-connector、xml-connector、excel-connector、file-stream-connector | 能力注册、schema/discovery 资源关闭、FileStream stream 所有权 |
| JS 引擎 | /Users/gavinxiao/kit/tapdata/tapdata/iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2 | StorageExecutorsManager、PdkFileStorageSessionManager、StorageFacade、JS 生命周期 |
| JS 运行时辅助 | /Users/gavinxiao/kit/tapdata/tapdata/iengine/iengine-common/src/main/java/com/tapdata/processor | 脚本包装和现有 process 兼容性核对 |
| 设计和使用说明 | /Users/gavinxiao/kit/tapdata/tapdata/docs/jsNode | 详细设计、研发计划、JS 示例、验收记录 |

---

## 3. 任务总表

估算为初始人日，实际工期取决于各模块分支和集成环境。任务依赖用任务编号表示；同一阶段中无依赖的任务可以并行。

| 编号 | 任务 | 负责角色 | 依赖 | 估算 | 主要交付物 | 完成门槛 |
| --- | --- | --- | --- | ---: | --- | --- |
| T0 | 源码基线与共享 API 收口 | 架构/文件平台 | 无 | 2 | API/实现清单、编译基线、能力矩阵 | 明确 operation API 源码归属和所有实现编译入口 |
| T1 | TapFileStorage 与文件 operation API 对齐 | 文件平台 | T0 | 3 | shared file API、capability enum、DTO/错误码 | 所有引用方编译通过，未实现能力不会被声明 |
| T2 | FileStorageFunction 和 Connector 能力注册 | PDK/Connector | T1 | 3 | PDK 函数、统一注册入口、五类 Connector 注册 | FTP 可通过 PDK 获取文件能力，不依赖 DAG 节点 |
| T3 | 文件 Connector 生命周期与 discovery 资源修复 | Connector | T1 | 4 | FileConnector、CSV/JSON/Excel/FileStream 资源修复 | 正常、异常、提前 return 路径资源均关闭 |
| T4 | FTP storage 连接和 I/O 修复 | FTP Connector | T1 | 5 | FTP adapter、受管 stream、并发/配置/关闭修复 | FTP 集成测试覆盖连接失败、stream、复制和断连 |
| T5 | 公共 session manager 与文件 operation 修复 | 文件平台 | T1、T4 | 5 | session reuse/eviction/invalidate、copy/list/stat/delete | 重试不会复用坏连接；临时文件和 final stat 正确 |
| T6 | SFTP/其他协议边界收口 | 文件平台/Connector | T1 | 2 | 能力矩阵和不支持行为 | 未完成协议不被 storage facade 暴露为可用 |
| T7 | 引擎 PDK-backed StorageExecutorsManager | 引擎 | T2、T5 | 6 | 连接解析、单飞初始化、缓存、关闭、失效 | 同连接并发只建一次 PDK/session，异常可恢复 |
| T8 | JS StorageFacade 和 process(record) 注入 | 引擎/JS | T7 | 4 | storage.find/exists/update/delete、生命周期绑定 | 按事件可直接写 FTP、跨 FTP 复制并返回结果 |
| T9 | JS 文档、编辑器提示和兼容处理 | JS/产品 | T8 | 2 | 使用示例、能力提示、旧脚本兼容 | 用户能区分 storage 与 ScriptExecutor |
| T10 | 单元、FTP 集成和资源回归测试 | QA/各模块 | T3、T4、T5、T8 | 6 | 自动化测试集、故障注入 | 关键资源泄露和事件级语义全部有测试 |
| T11 | 端到端、并发和性能验证 | QA/引擎/文件平台 | T8、T10 | 4 | E2E 报告、性能基线 | 事件吞吐和连接数量达到预设基线 |
| T12 | 灰度发布、监控、回滚和验收 | 发布/QA/研发 | T11 | 3 | feature flag、指标、回滚手册、验收单 | FTP storage 可独立开关，旧脚本回归通过 |

**总工作量初始估算：49 人日。** T3、T4、T6 可在 T1 后并行；T7 是关键路径，T8、T10、T11 依次依赖。

---

## 4. 详细任务计划

### T0：源码基线与共享 API 收口

**目标：** 在写新代码前固定当前工作树中已确认的 API 不一致和资源问题，避免 engine、connector、target/classes 使用不同版本。

**涉及文件/目录：**

- /Users/gavinxiao/kit/tapdata/tapdata-common-lib/plugin-kit/tapdata-api/src/main/java/io/tapdata/file/TapFileStorage.java
- /Users/gavinxiao/kit/tapdata/tapdata-common-lib/plugin-kit/tapdata-api/src/main/java/io/tapdata/file/TapFileStorageBuilder.java
- /Users/gavinxiao/kit/tapdata/tapdata-connectors/connectors-common/file-connector-core/src/main/java/io/tapdata/common/file/FileStorageFactory.java
- /Users/gavinxiao/kit/tapdata/tapdata-connectors/connectors-common/file-connector-core/src/main/java/io/tapdata/common/file/DefaultFileStorageSessionManager.java
- /Users/gavinxiao/kit/tapdata/tapdata-connectors/connectors-common/file-connector-core/src/main/java/io/tapdata/common/file/DefaultFileOperationService.java

**实施步骤：**

- [x] 建立 TapFileStorage 方法清单，确认当前接口没有 capabilities()，而 FTP 实现和 DefaultFileOperationService 已经使用该方法。
- [x] 建立 io.tapdata.file.operation 类型清单，确认当前 connector core 有导入但当前工作树没有对应的真实 API 源码目录。
- [x] 固化 FileStorageFactory -> TapFileStorageBuilder -> storage.init(params) 的真实建连路径。
- [x] 固化所有协议实现的 capabilities、move、read stream、destroy 方法现状。
- [x] 输出一份编译依赖图，标出 PDK、engine、connector 共享的 API jar。
- [x] 在 T1 开始前确定 operation API 唯一源码归属，不允许继续依赖 target/classes 或残留 sources jar。

**验收：**

- 能从源码列出所有 FileStorageCapability 实现。
- 能在干净构建中复现或排除 capabilities() 和 operation package 的编译问题。
- T1 的接口改造不再依赖未提交的二进制产物。

---

### T1：TapFileStorage 与文件 operation API 对齐

**目标：** 建立 engine、PDK、connector 都能编译和运行的共享文件 API。

**涉及文件：**

- 修改：/Users/gavinxiao/kit/tapdata/tapdata-common-lib/plugin-kit/tapdata-api/src/main/java/io/tapdata/file/TapFileStorage.java
- 新增或归位：/Users/gavinxiao/kit/tapdata/tapdata-common-lib/plugin-kit/tapdata-api/src/main/java/io/tapdata/file/operation/FileEndpoint.java
- 新增或归位：/Users/gavinxiao/kit/tapdata/tapdata-common-lib/plugin-kit/tapdata-api/src/main/java/io/tapdata/file/operation/FileCopyRequest.java
- 新增或归位：/Users/gavinxiao/kit/tapdata/tapdata-common-lib/plugin-kit/tapdata-api/src/main/java/io/tapdata/file/operation/FileOperationResult.java
- 新增或归位：/Users/gavinxiao/kit/tapdata/tapdata-common-lib/plugin-kit/tapdata-api/src/main/java/io/tapdata/file/operation/FileOperationErrorCode.java
- 新增或归位：/Users/gavinxiao/kit/tapdata/tapdata-common-lib/plugin-kit/tapdata-api/src/main/java/io/tapdata/file/operation/FileOperationStatus.java
- 新增或归位：/Users/gavinxiao/kit/tapdata/tapdata-common-lib/plugin-kit/tapdata-api/src/main/java/io/tapdata/file/operation/FileStorageCapability.java

**接口要求：**

~~~java
public interface TapFileStorage {
    void init(Map<String, Object> params) throws Exception;
    void destroy() throws Exception;
    TapFile getFile(String path) throws Exception;
    void readFile(String path, Consumer<InputStream> consumer) throws Exception;
    InputStream readFile(String path) throws Exception;
    boolean isFileExist(String path) throws Exception;
    boolean move(String sourcePath, String destPath) throws Exception;
    boolean delete(String path) throws Exception;
    TapFile saveFile(String path, InputStream is, boolean canReplace) throws Exception;
    OutputStream openFileOutputStream(String path, boolean append) throws Exception;
    void getFilesInDirectory(String directoryPath, Collection<String> includeRegs,
                             Collection<String> excludeRegs, boolean recursive,
                             int batchSize, Consumer<List<TapFile>> consumer) throws Exception;
    boolean isDirectoryExist(String path) throws Exception;
    EnumSet<FileStorageCapability> capabilities();
    String getConnectInfo();
}
~~~

**实施步骤：**

- [x] 把 operation DTO、错误码、状态和 capability enum 放入唯一 shared API 源码目录。
- [x] 在 TapFileStorage 增加 capabilities()；所有实现补齐实现，未实现协议返回空能力或明确 capability。
- [x] 明确路径是 rootPath 下的相对路径，统一路径规范化和越界错误。
- [x] 明确 raw InputStream 的所有权：调用方必须 close；FTP/SFTP 实现必须在 close 时完成协议 pending command。
- [x] 更新 FileStorageFactory、DefaultFileStorageSessionManager、DefaultFileOperationService 的 import。
- [x] 为接口新增行为写单元测试，覆盖 null file、not found、unsupported capability 和 close contract。

**验收：**

- shared API、file-connector-core、FTP module 可从源码干净编译。
- DefaultFileOperationService 不再调用不存在的接口。
- 所有 storage 实现的能力声明与实际方法一致。

---

### T2：FileStorageFunction 和 Connector 能力注册

**目标：** 通过 PDK 能力暴露文件 storage，engine 不直接调用 FileStorageFactory。

**涉及文件：**

- 新增：/Users/gavinxiao/kit/tapdata/tapdata-common-lib/plugin-kit/tapdata-pdk-api/src/main/java/io/tapdata/pdk/apis/functions/connector/common/FileStorageFunction.java
- 修改：/Users/gavinxiao/kit/tapdata/tapdata-common-lib/plugin-kit/tapdata-pdk-api/src/main/java/io/tapdata/pdk/apis/functions/ConnectorFunctions.java
- 修改：/Users/gavinxiao/kit/tapdata/tapdata-connectors/connectors-common/file-connector-core/src/main/java/io/tapdata/common/FileConnector.java
- 修改：CSV、JSON、XML、Excel、FileStream 的 registerCapabilities()

**接口要求：**

~~~java
public interface FileStorageFunction extends TapConnectorFunction {
    TapFileStorage getStorage(TapConnectorContext connectorContext) throws Throwable;
}

protected void registerFileStorageFunction(ConnectorFunctions functions) {
    functions.supportFileStorageFunction(context -> {
        if (storage == null) {
            throw new TapPdkException("File storage has not been initialized");
        }
        return storage;
    });
}
~~~

**实施步骤：**

- [x] 在 ConnectorFunctions 增加字段、supportFileStorageFunction() 和 getter。
- [x] 在 FileConnector 中建立统一注册方法，避免每个文件 Connector 重复创建 storage。
- [x] CSV、JSON、XML、Excel、FileStream 的 registerCapabilities() 调用统一入口。
- [x] 非文件 Connector 不注册 FileStorageFunction。
- [x] 增加 capability 名称的稳定反射测试。
- [x] 增加“没有 DAG 文件节点，但按连接配置创建 PDK 文件能力”的 connector-level 测试。

**验收：**

- FTP Connector 的 ConnectorFunctions 能取得 FileStorageFunction。
- 无 DAG FTP 节点时，engine 通过 PDK 能力获取 FTP storage。
- 通过 ScriptExecutorsManager.getScriptExecutor('target-ftp') 仍然只保留数据库命令语义。

---

### T3：文件 Connector 生命周期与 discovery 资源修复

**目标：** 修复当前 FileConnector、schema/discovery 和 writer 的已确认连接资源泄露。

**涉及文件：**

- 修改：/Users/gavinxiao/kit/tapdata/tapdata-connectors/connectors-common/file-connector-core/src/main/java/io/tapdata/common/FileConnector.java
- 修改：/Users/gavinxiao/kit/tapdata/tapdata-connectors/connectors-common/file-connector-core/src/main/java/io/tapdata/common/FileSchema.java
- 修改：/Users/gavinxiao/kit/tapdata/tapdata-connectors/connectors-common/file-connector-core/src/main/java/io/tapdata/common/AbstractFileRecordWriter.java
- 修改：CSV、JSON、Excel 的 discoverSchema()
- 参照：XML 的 discoverSchema() finally 结构
- 修改：/Users/gavinxiao/kit/tapdata/tapdata-connectors/connectors/file-stream-connector/src/main/java/io/tapdata/connector/json/FileStreamConnector.java

**实施步骤：**

- [x] 重写 FileConnector.onStop() 为独立 try/finally，保证 mergeCacheFiles、releaseResource、storage.destroy、executor shutdown 互不阻断。
- [x] 保存 merge worker Future，onStop 调用 shutdownNow 并等待；worker 响应 interrupt。
- [x] FileSchema 的 InterruptedException 路径恢复 interrupt 并在 finally shutdownNow。
- [x] CSV、JSON、Excel discoverSchema 使用 XML 同等的 try/finally 结构，覆盖提前 return 和 schema 异常。
- [ ] FileStreamConnector 明确 file_data stream 的所有权，优先改为 callback/受管 stream。（待补充专项改造）
- [x] 让 destroy 异常不覆盖 discoverSchema 或 writer 的主异常，同时输出资源清理指标。
- [x] 增加成功、提前 return、异常、中断四类测试。

**验收：**

- discovery 异常后 storage.destroy 必定执行。
- FileConnector 停止后没有存活的 merge executor。
- FileStream 的输入流关闭时不会留下 FTP pending command。
- 既有 CSV/JSON/XML/Excel/FileStream source/target 回归通过。

---

### T4：FTP storage 连接和 I/O 修复

**目标：** 修复 FTPClient 的部分初始化泄露、raw stream pending command、状态型并发和配置缺陷。

**涉及文件：**

- 修改：/Users/gavinxiao/kit/tapdata/tapdata-connectors/file-storages/ftp-file/src/main/java/io/tapdata/storage/ftp/FtpFileStorage.java
- 修改：/Users/gavinxiao/kit/tapdata/tapdata-connectors/file-storages/ftp-file/src/main/java/io/tapdata/storage/ftp/FtpConfig.java
- 新增测试：FTP storage unit/integration test

**实施步骤：**

- [x] init 使用 try/catch 清理 partial FTPClient；connect、login、setControlEncoding、passive mode、binary mode 失败都 disconnect。
- [x] destroy 改为幂等 close，处理登录失败、重复关闭和 logout 异常。
- [x] raw readFile(String) 返回受管 InputStream；close 只完成一次 completePendingCommand；close/pending 失败使 session invalidate。
- [x] openFileOutputStream() 增加 bulk write、flush、幂等 close 和错误后的 session invalidate。
- [x] 对 FTPClient 有状态操作建立 session 级串行边界，尤其是 changeWorkingDirectory、list、store、retrieve、rename。
- [x] 所有路径转换统一在 rootPath 下完成；避免操作依赖上一次调用遗留的 current working directory。
- [x] FtpConfig 设置默认 encoding、校验 host/port/timeout，并把秒/毫秒转换集中在配置层。
- [x] ftpSsl=true 时明确返回 FTPS 未支持错误；在 FTPSClient 和证书测试完成前不得建立普通 FTP 连接冒充 SSL。
- [x] 能力矩阵只声明已实现并测试通过的能力。

**验收：**

- connect/login/配置异常均释放 client。
- raw stream close 会完成 pending command，重复 close 不重复发送命令。
- 并发事件不会交叉污染 FTP current working directory。
- FTP 文件复制、Unicode 路径、断连重试、临时文件和能力矩阵集成测试通过。
- ftpSsl=true 不会静默降级为明文 FTP。

---

### T5：公共 session manager 与文件 operation 修复

**目标：** 将连接复用、坏 session 失效和文件复制资源控制收敛到公共文件层。

**涉及文件：**

- 修改：/Users/gavinxiao/kit/tapdata/tapdata-connectors/connectors-common/file-connector-core/src/main/java/io/tapdata/common/file/DefaultFileStorageSessionManager.java
- 修改：/Users/gavinxiao/kit/tapdata/tapdata-connectors/connectors-common/file-connector-core/src/main/java/io/tapdata/common/file/DefaultFileOperationService.java
- 修改：FileStorageSession、FileServiceConfigMapper 及对应测试

**实施步骤：**

- [x] session key 移除线程绑定，使用 protocol/rootPath 和非敏感配置 fingerprint。（connectionId/configVersion 动态监听暂未实现）
- [x] 移除 Thread.currentThread().getId()，确保同一 executor 内不同 worker 可复用 session。
- [x] 移除密码、私钥、token 等明文进入日志和可见 key。
- [x] 增加 lastAccess、idle eviction 和 maxSessions 硬上限。
- [x] invalidate 改为 mark-draining；旧 session 有引用时不立即销毁，引用归零后再销毁。
- [x] destroy 异常不阻断其他 session 关闭。
- [x] DefaultFileOperationService 的 retryable 远端错误在下一次 copyOnce 前先 invalidate source/target session。
- [x] 临时文件使用随机后缀，finally 清理本地和远端临时文件。
- [x] move 后对最终路径执行 final stat。
- [x] copyBatch 保留已完成和失败项；当前批量上限为 100，未接入 maxBytes/failFast 参数。
- [x] list/stat/exists/delete/copy 统一错误分类，连接错误才 invalidate，参数/权限/路径错误不重试。

**验收：**

- 同一连接不同 worker 使用同一可复用 session。
- session 超限时有界失败，不会无限建连。
- 连接断开后重试使用新 session。
- 临时文件清理失败可观测；最终结果路径和元数据正确。
- 不产生 file_operation_ledger。

---

### T6：SFTP 和其他协议边界收口

**目标：** 避免公共 API 把尚未修复的协议误暴露为可用，同时为后续扩展保留能力矩阵。

**涉及文件：**

- 修改或补充：/Users/gavinxiao/kit/tapdata/tapdata-connectors/file-storages/sftp-file/src/main/java/io/tapdata/storage/sftp/SftpFileStorage.java
- 检查：SMB、S3FS、NFS、OSS 等 TapFileStorage 实现
- 修改：各实现的 capability 注册和测试

**实施步骤：**

- [x] SFTP 的 StrictHostKeyChecking 从硬编码 no 改为统一配置，默认安全模式。
- [x] SFTP channel timeout 纳入 SFTP 配置。
- [x] SFTP raw stream 使用受管 close 或强制 callback API。
- [x] SFTP 的 move UnsupportedOperationException 不声明 MOVE/ATOMIC_RENAME。
- [x] SFTP delete/isFileExist 区分 not found、权限错误和连接错误，不把所有 SftpException 返回 false。
- [x] 为其他协议保留能力边界；没有 engine 放行和集成测试的协议保持不可用。
- [ ] 首期 feature flag 只允许 FTP。（本期未新增 feature flag，engine 通过 protocol guard 限制为 FTP）

**验收：**

- SFTP/其他协议不会因为共享 API 增加而被错误地开放到 JS。
- capability 查询结果与实际协议行为一致。
- 现有文件 source/target 的协议回归不受影响。

---

### T7：引擎 PDK-backed StorageExecutorsManager

**目标：** 在没有 DAG 文件连接节点时，按连接名称创建并复用 PDK-backed 文件 storage。

**涉及文件：**

- 新增：/Users/gavinxiao/kit/tapdata/tapdata/iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/script/storage/StorageExecutorsManager.java
- 新增：/Users/gavinxiao/kit/tapdata/tapdata/iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/script/storage/PdkFileStorageSessionManager.java
- 新增或复用：PdkConnectionHandle、StorageExecutor、FileConnectionResolver
- 参照：/Users/gavinxiao/kit/tapdata/tapdata/iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/script/ScriptExecutorsManager.java
- 参照：/Users/gavinxiao/kit/tapdata/tapdata/iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/hazelcast/processor/HazelcastJavaScriptProcessorNode.java

**核心接口：**

~~~java
public interface StorageExecutorsManager extends AutoCloseable {
    StorageExecutor getStorageExecutor(String connectionName) throws Throwable;
    void invalidate(String connectionName, Throwable cause);
    void close();
}

public interface StorageExecutor extends AutoCloseable {
    FileOperationResult update(Map<String, Object> data,
                               Map<String, Object> options) throws Throwable;
    FileMetadata find(Map<String, Object> query) throws Throwable;
    boolean exists(String path) throws Throwable;
    FileOperationResult delete(Map<String, Object> data,
                               Map<String, Object> options) throws Throwable;
    void close();
}
~~~

**实施步骤：**

- [x] 按连接名称查询 connection document；当前权限校验沿用连接查询边界，未新增独立授权模型。
- [x] 按 protocol 和 connector specification 创建 PDK node，不直接调用 FileStorageFactory。
- [x] 以 trimmed connectionName 作为 StorageExecutor 缓存键，session 层以 endpoint fingerprint 复用。
- [x] 对同一连接使用单飞初始化；并发首次访问只允许一个 PDK 创建，其余等待结果。
- [x] 创建失败进入 1 秒有限退避；退避结束后允许重新创建。
- [x] FileStorageFunction 不存在、协议不支持或能力不足时返回稳定错误码。
- [x] 远端断连、FTP session 损坏时使对应 executor/session 失效。
- [x] doClose 时关闭所有 StorageExecutor、session 和 PDK associate id。
- [x] 不把密码、私钥、完整连接 URL 写入日志。

**验收：**

- DAG 没有 FTP 节点时，getStorageExecutor('target-ftp') 可以按连接名取得文件能力。
- 同一连接的并发事件不会每条事件触发一次 PDK 创建。
- 节点关闭后所有关联 PDK/session 都释放。
- 连接配置更新后旧缓存失效，新配置可以重新初始化。

---

### T8：JS StorageFacade 和 process(record) 注入

**目标：** 向 enhanced JS 提供稳定的事件级 storage API，不暴露 Java 资源对象。

**涉及文件：**

- 修改：/Users/gavinxiao/kit/tapdata/tapdata/iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/hazelcast/processor/HazelcastJavaScriptProcessorNode.java
- 修改或新增：/Users/gavinxiao/kit/tapdata/tapdata/iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/script/storage/StorageFacade.java
- 参照：/Users/gavinxiao/kit/tapdata/tapdata/iengine/iengine-common/src/main/java/com/tapdata/processor/ScriptUtil.java

**JS 契约：**

~~~javascript
storage.update(connectionName, data, options)
storage.find(connectionName, query, options)
storage.exists(connectionName, path)
storage.delete(connectionName, data, options)
~~~

**实施步骤：**

- [x] enhanced JS buildEngine 时注入 storage；standard JS 保持没有 storage。
- [x] process(record) 每次调用时都可以根据事件字段决定是否操作文件。
- [x] update 支持 write 和 copy；copy 的 source.connection 可以与目标连接不同。
- [x] exists/find/delete 通过 StorageExecutor 执行，结果转换为普通 JSON。
- [x] 大文件复制只返回状态、文件摘要、字节数、耗时和错误，不把 stream 返回 JS。
- [x] storage 异常按现有 JS 节点异常传播规则处理；用户脚本可捕获，也可让当前事件失败。
- [x] 不在 JS 层增加事件 ledger、跨事件重复调用判断或任务级门禁。
- [x] doClose 关闭 StorageExecutorsManager，不依赖某个事件是否调用过 storage。

**验收：**

- 以下脚本契约能执行目标 FTP 写入：
  
~~~javascript
function process(record) {
  return storage.update('target-ftp', {
    action: 'write',
    target: { path: '/out/' + record.id + '.json' },
    content: JSON.stringify(record.payload),
    contentType: 'application/json'
  }, {
    overwrite: 'overwrite',
    verify: 'size'
  });
}
~~~

- 以下脚本契约能完成 source FTP 到 target FTP 的事件级复制：
  
~~~javascript
function process(record) {
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
  return record;
}
~~~

- 现有 Mongo aggregate 脚本仍使用 ScriptExecutorsManager，不受 storage 注入影响。

---

### T9：JS 文档、编辑器提示和兼容处理

**目标：** 让用户知道文件连接使用 storage，数据库连接使用 ScriptExecutor，并明确首期协议范围。

**涉及文件：**

- 修改：/Users/gavinxiao/kit/tapdata/tapdata/docs/jsNode/TAP-12832-ftp-storage-detailed-design.md
- 修改：/Users/gavinxiao/kit/tapdata/tapdata/docs/jsNode/TAP-12832-ftp-storage-overview-design.md
- 检查：JS 节点编辑器的 enhanced/standard 模式提示和现有脚本文档
- 检查：/Users/gavinxiao/kit/tapdata/tapdata/manager/tm/src/main/resources/init/idaas 中的 JS API 帮助项

**实施步骤：**

- [x] 增加 storage.update、find、exists、delete 的参数和返回值说明。
- [x] 增加按事件直接写入、FTP 到 FTP 复制、条件删除示例。
- [x] 明确 getScriptExecutor('target-ftp') 不是 FTP 文件 API。
- [x] 明确大文件不通过 JS content 传输。
- [x] 明确首期只支持 FTP，SFTP/其他协议未注册能力时返回不支持。
- [x] 旧脚本不定义 storage 时保持原行为。

**验收：**

- 文档示例与 T8 的实际接口签名一致。
- 不出现“所有文件连接都已经可用”的误导性描述。
- 旧 Mongo aggregate 示例回归通过。

---

### T10：单元、FTP 集成和资源回归测试

**目标：** 用自动化测试证明资源、能力、事件级语义和错误恢复正确。

**测试分组：**

| 测试组 | 重点场景 | 最低覆盖 |
| --- | --- | --- |
| shared API | capabilities、路径、错误码、DTO | 每个公开方法至少有成功和错误路径 |
| FileConnector | onStop、executor、writer、storage | merge 异常、release 异常、storage destroy 异常 |
| discovery | CSV/JSON/Excel/XML | 成功、提前 return、解析异常、空文件 |
| FTP init | connect/login/config | connect 异常、login 异常、encoding 异常、重复 destroy |
| FTP stream | retrieve/store stream | close、重复 close、bulk write、pending command 失败 |
| FTP state | cwd/list/store/retrieve | 多线程调用、失败后路径恢复、Unicode 路径 |
| session | reuse/invalidate/eviction | 跨 worker 复用、坏连接失效、引用计数、空闲淘汰 |
| file operation | copy/list/stat/exists/delete | overwrite、verify、临时清理、final stat、部分成功 |
| JS facade | process(record) | 条件调用、不同连接、异常传播、无 ledger |
| PDK lifecycle | create/stop/release | 单飞初始化、退避、配置变更和 close |

**实施步骤：**

- [ ] 先为每个已确认 bug 编写失败测试。
- [ ] 修复后分别运行对应 module test。
- [ ] 使用本地 FTP server 或 Testcontainers 覆盖真实 FTP 命令序列。
- [ ] 注入 connect/login/retrieve/store/completePendingCommand/rename 异常。
- [ ] 统计测试前后 FTP session、线程和临时文件数量。
- [ ] 将资源泄露测试加入 CI，不能只依赖人工检查日志。

**验收：**

- 资源泄露相关测试在重复运行和异常注入下稳定通过。
- FTP 复制失败后不会留下可复用坏连接。
- 测试验证每条事件可以按脚本条件调用 storage，未写入事件 ledger。

---

### T11：端到端、并发和性能验证

**目标：** 验证功能链路和性能目标，重点确认不会一条事件创建一次 PDK/FTP 连接。

**测试场景：**

| 场景 | 测试方法 | 验收指标 |
| --- | --- | --- |
| 无 DAG FTP 节点 | 任务只放 JS 节点，连接 FTP 只存在连接管理 | 文件操作成功 |
| 事件级直接写入 | 多条 record 按不同路径写 FTP | 每条事件按脚本逻辑执行 |
| FTP 到 FTP | source-ftp 读取，target-ftp 写入 | 内容、size、路径和状态正确 |
| 连接复用 | 同一连接连续处理大量事件 | PDK/session 创建次数按连接数和配置版本增长，不按事件数增长 |
| 多 worker | 并行事件访问同一 FTP | 无 cwd 串扰；连接数受上限控制 |
| 断连恢复 | 测试中断 FTP 控制/数据连接 | session 失效后有限重试成功或返回明确错误 |
| 关闭 | 停止任务、异常停止、取消任务 | PDK、session、executor、stream 均释放 |
| 大文件 | 传输大于 JS 堆适配值的文件 | JS 堆无文件全量副本，Java 层流式传输 |

**性能基线：**

- 记录 process 调用次数、storage operation 次数、PDK 创建次数、FTP session 创建次数、复用命中率、平均/TP95/TP99 耗时和失败率。
- 记录 maxSessions、idle eviction、连接退避和资源关闭指标。
- 用固定文件大小和固定并发度比较开启 storage 前后的 JS 节点吞吐。
- 连接创建次数必须显著低于事件数量；如果仍接近事件数量，阻止发布并回到 T7/T8 排查缓存边界。

---

### T12：灰度发布、监控、回滚和验收

**目标：** 在不影响旧 JS、Mongo aggregate 和既有文件 source/target 的前提下发布 FTP storage。

**Feature flag：**

- js.node.storage.enabled
- js.node.storage.write.enabled
- js.node.storage.ftp.enabled

**实施步骤：**

- [ ] 默认关闭 FTP storage 写能力，只在测试租户/灰度环境开启。
- [ ] 发布 storage operation、session、PDK 创建和资源清理指标。
- [ ] 配置失败率、重试率、session limit、temp cleanup failed 告警阈值。
- [ ] 准备关闭 feature flag 的回滚步骤，不删除用户连接配置。
- [ ] 执行旧 JS、Mongo aggregate、CSV/JSON/XML/Excel/FileStream 回归。
- [ ] 执行无 DAG FTP 节点、事件级写入、跨 FTP 复制和异常恢复验收。
- [ ] 形成发布验收单，记录版本、feature flag、测试文件、连接类型和结果。

**回滚条件：**

- PDK 创建次数接近事件数。
- FTP session 或线程持续增长。
- raw stream pending command 导致后续命令失败。
- 文件操作错误导致旧 JS/Mongo aggregate 回归。
- 任务关闭后仍有 PDK、FTP session、executor 或临时文件。
- FTP storage 错误率超过灰度阈值。

**回滚动作：**

- 关闭 js.node.storage.ftp.enabled 和 js.node.storage.write.enabled。
- 保留 storage operation 诊断日志，停止新的文件写入。
- 旧 ScriptExecutor、旧 JS 和既有 source/target 继续运行。
- 修复后重新运行 T10/T11，不能通过直接放宽重试或跳过资源检查上线。

---

## 5. 依赖关系和里程碑

~~~text
T0 源码/API 基线
  |
  v
T1 shared file API
  |
  +--> T2 PDK FileStorageFunction --> T7 引擎 StorageExecutorsManager --> T8 JS StorageFacade
  |
  +--> T3 FileConnector/discovery 资源修复
  |
  +--> T4 FTP I/O 修复 --> T5 session/file operation --> T7
  |
  +--> T6 其他协议边界
                                      |
                                      v
                              T9 JS 文档兼容
                                      |
                              T10 自动化测试
                                      |
                              T11 E2E/性能
                                      |
                              T12 灰度发布
~~~

| 里程碑 | 完成条件 | 预计阶段 |
| --- | --- | --- |
| M1 API 基线完成 | T0、T1 完成，shared API 干净编译 | 第 1 周 |
| M2 Connector 能力完成 | T2、T3、T4 完成，FTP 资源和 capability 测试通过 | 第 2 周 |
| M3 文件公共服务完成 | T5、T6 完成，session 和复制服务可独立测试 | 第 3 周 |
| M4 JS 功能完成 | T7、T8、T9 完成，process(record) 可直接调用 FTP | 第 4 周 |
| M5 发布候选完成 | T10、T11 完成，性能和关闭资源检查通过 | 第 5 周 |
| M6 灰度发布 | T12 完成，回滚和监控可用 | 第 5 周末 |

---

## 6. 关键技术验收清单

### 功能

- [ ] DAG 不包含 FTP 节点时，JS 可以按连接名称使用授权 FTP。
- [ ] process(record) 可以按事件条件直接写 FTP。
- [ ] process(record) 可以按事件条件把 source FTP 文件复制到 target FTP。
- [ ] 支持 skip/overwrite/fail、size 校验、受限重试和临时文件发布。
- [ ] 文件操作结果和异常可以返回/传播到 JS。

### 性能与连接

- [ ] PDK 创建不按事件发生，而是按连接配置版本复用。
- [ ] FTP session 跨 worker 复用或按明确并发边界创建。
- [ ] 坏连接重试前会 invalidate，不会重复复用。
- [ ] maxSessions、idle eviction 和退避机制有指标。

### 资源安全

- [ ] FTP 初始化中途失败会 disconnect。
- [ ] raw stream close 会完成 pending command。
- [ ] output stream close 幂等，bulk write/flush 正确。
- [ ] FileConnector、discovery、writer、FileSchema executor 都会关闭。
- [ ] 任务停止/取消后 PDK、session、executor、stream、临时文件无残留。

### 范围和兼容

- [ ] ScriptExecutorsManager 的 Mongo aggregate 语义不变。
- [ ] standard JS 不自动获得 storage。
- [ ] 不产生 file_operation_ledger。
- [ ] 首期只对 FTP 开放 storage；SFTP/其他协议未完成时返回不支持。
- [ ] ftpSsl 未实现 FTPS 前不会静默降级为明文 FTP。

## 7. 研发过程中的提交和评审策略

每个任务至少拆分为以下提交顺序：

1. 失败测试或基线测试。
2. 最小实现。
3. 资源、异常和边界测试。
4. 文档/指标/配置更新。
5. 模块级测试和代码评审。

评审重点：

- T1/T2：API 和 classloader 边界是否稳定。
- T3/T4：所有异常路径是否释放连接和 stream。
- T5/T7：session 是否跨事件复用，是否存在并发销毁。
- T8：JS 是否仍保持事件级语义，是否错误引入任务级 ledger。
- T10/T11：是否真正验证 PDK 创建次数和 FTP 资源数量，而不是只验证文件最终存在。

本计划完成后，最终交付物包括：代码变更、自动化测试、feature flag、监控指标、回滚手册、发布验收单，以及与本计划一致的 TAP-12832 详细设计文档。
