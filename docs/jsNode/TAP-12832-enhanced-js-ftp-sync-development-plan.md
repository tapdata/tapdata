# TAP-12832 增强 JS 节点文件同步开发计划

| 属性 | 内容 |
| --- | --- |
| 需求 | [TAP-12832：增强 JS 节点支持 FTP 文件同步后继续下游数据库同步](https://tapdata.atlassian.net/browse/TAP-12832) |
| 依据 | [概要设计](/Users/gavinxiao/kit/tapdata/tapdata/docs/jsNode/TAP-12832-enhanced-js-ftp-sync-design.md)、[详细设计](/Users/gavinxiao/kit/tapdata/tapdata/docs/jsNode/TAP-12832-enhanced-js-ftp-sync-detailed-design.md) |
| 目标 | 将详细设计拆分为可并行开发、可独立验收、可逐步合并的开发任务 |
| 计划原则 | 先稳定公共 API 和文件复用链路，再接入引擎和前端；旧 JS 任务全程保持兼容 |

## 1. 总体交付范围

本次交付包括：

1. 在 JS 节点持久化通用 `scriptParams`，支持任意 key、类型、加密标记和增删改。
2. 在 GraalJS 中注入 `jsNodeConfig`，提供 `get`、`has`、`getOrDefault`。
3. 注入受控 `ftp` facade，提供同步 `copy`、`copyByConfig`、`copyBatch`。
4. 在 `tapdata-api` 定义稳定的文件操作 API，在 `file-connector-core` 实现共享服务。
5. 复用 FTP、SFTP、SMB、S3FS、NFS、OSS 的 `TapFileStorage`，不在引擎实现第二套 FTP。
6. 文件成功后才让当前业务事件进入原有下游；不同业务事件各自等待，不建立全局 barrier。
7. 提供 dry-run 试运行、错误码、幂等复用、临时文件发布、路径限制、日志脱敏和资源回收。

## 2. 依赖关系和推荐节奏

```text
P0 设计冻结和代码脚手架
 ├─ P1 公共文件操作 API
 │   └─ P2 TapFileStorage/FTP 适配器修复
 │       └─ P3 file-connector-core 共享服务
 ├─ P4 JS 节点参数模型和 TM 校验
 ├─ P5 引擎 jsNodeConfig + ftp facade
 │   └─ P6 试运行 dry-run
 └─ P7 前端参数配置和模板
         └─ P8 集成、性能、安全和回归验收
```

P1、P4 可以并行；P2 依赖 P1 中的 capability 和错误模型；P3 依赖 P1、P2；P5 依赖 P1、P3、P4；P6 和 P7 可在 P5 接口冻结后并行；P8 必须等待所有功能包合并。

建议每个阶段独立提交并通过对应验收后再进入下一阶段，避免前端先产生没有运行时实现的任务配置。

## 3. 开发计划总表

| ID | 阶段 | 开发内容 | 主要代码位置 | 前置 | 实现目标 | 阶段验收标准 |
| --- | --- | --- | --- | --- | --- | --- |
| P0-01 | 设计冻结 | 确认协议、字段、错误码、幂等和并发边界 | 本文档、详细设计 | 无 | 形成唯一可执行契约 | 研发、测试、产品确认：运行时对象叫 `jsNodeConfig`，持久化字段叫 `scriptParams`；一期不开放删除源文件 |
| P0-02 | 脚手架 | 建立包、接口、错误码、feature switch | `tapdata-api`、`file-connector-core`、`iengine-app` | P0-01 | 编译通过但不改变旧流程 | 全仓相关模块编译通过；旧 JS 节点无新参数时行为不变 |
| P1-01 | 公共 API | 定义 endpoint、copy request、result、batch result、capability | `tapdata-api/io/tapdata/file/operation` | P0-02 | 固定引擎与 connector 之间的数据契约 | API 可被 mock；不依赖 FTPClient、Spring 或 connector 实现 |
| P1-02 | 公共 API | 定义统一文件错误码和异常结构 | `tapdata-api` error package | P0-01 | 让引擎保留可识别的文件错误 | 每个错误包含 code、阶段、脱敏路径；不包含密码和完整连接串 |
| P1-03 | 公共 API | 定义 `FileOperationServiceProvider`/handle | `tapdata-api`、SPI 资源 | P1-01 | 支持 connector classloader 装载共享实现 | provider 缺失能返回 `FILE_SERVICE_UNAVAILABLE`，不产生 ClassCastException |
| P2-01 | 存储适配器 | 扩展 `TapFileStorage.capabilities()` | `TapFileStorage.java` | P1-01 | 显式声明 rename、mkdir、checksum 等能力 | 未声明能力的协议不会被 service 静默降级 |
| P2-02 | FTP 适配器 | 检查 `storeFile`、`completePendingCommand` 返回值 | `FtpFileStorage.java` | P1-02 | 远端写入失败必然被发现 | 模拟返回 false 时不发布正式文件并返回 `FILE_WRITE_FAILED` |
| P2-03 | FTP 适配器 | 实现 rename/move、目录创建、路径和编码测试 | `FtpFileStorage.java`、FTP tests | P2-01 | 支持临时文件发布和中文路径 | 同 endpoint rename 成功；父目录缺失可按约定创建；中文名可读写 |
| P2-04 | 其他适配器 | 为 SFTP/SMB/S3FS/NFS/OSS 声明已有能力 | 对应 `file-storages/*` | P2-01 | service 能判断协议边界 | 不支持 rename 的协议返回明确 unsupported；不执行半成品发布 |
| P3-01 | 共享服务 | 实现 `FileServiceConfigMapper` | `file-connector-core` | P1-01、P2-01 | 任意参数前缀映射为 `FileEndpoint` 和 storage params | `mgm.in.*`、`mgm.out.*` 可映射；未知协议/缺必填字段在调用前失败 |
| P3-02 | 共享服务 | 实现 `FileStorageSessionManager` | `file-connector-core` | P1-03、P2 | 统一 session 建立、复用、失效和引用计数 | 同配置可复用；不同租户/凭据隔离；一方释放不影响另一方 |
| P3-03 | 共享服务 | 实现路径策略和临时路径工厂 | `FilePathPolicy`、`TempPathFactory` | P1-01 | 防止路径越界并保证临时文件归属 | `..`、绝对路径、协议前缀被拒绝；临时文件只能由本次调用清理 |
| P3-04 | 共享服务 | 实现流式传输、超时和重试 | `FileTransferExecutor`、`RetryExecutor` | P3-02、P3-03 | 不把文件正文放进 JS 或内存大对象 | 内存随 buffer 而非文件大小增长；网络临时错误按次数和总时长重试 |
| P3-05 | 共享服务 | 实现 size/checksum 校验和 `REUSED` | `FileVerifier`、`TargetProbe` | P3-04 | 支持重放和数据库失败后的文件复用 | 目标一致返回 `REUSED`；不一致返回 `FILE_TARGET_CONFLICT` |
| P3-06 | 共享服务 | 实现 `copy`/`copyBatch` | `DefaultFileOperationService` | P3-01~P3-05 | 提供同步单文件和批量服务 | 单文件成功返回 `COPIED`；批次任一失败不返回 success；已发布文件不误删 |
| P3-07 | 数据源复用 | 将 `FileConnector`/`FileTest` 接到共享校验和装配链 | `FileConnector.java`、`FileTest.java` | P3-01~P3-06 | 数据源和 JS 经过同一服务和 adapter | 源 connector 读写回归通过；没有新增第二套 FTP builder |
| P4-01 | 节点模型 | 增加 `JsNodeConfigParam`、`scriptParams` | `ScriptProcessNode.java`、tm-commons DTO | P0-01 | 旧 DAG 可反序列化，新参数可持久化 | 缺字段按空列表；复制、导入导出、配置 diff 保留参数 |
| P4-02 | 节点校验 | 实现 key/type/value/encryption 校验 | TM validator/service | P4-01 | 在保存阶段发现配置错误 | 重复 key、非法 key、未知 type、超限值返回字段级错误 |
| P4-03 | 密文权限 | 接入现有密文/secret 解密和脱敏 | TM/engine secret service | P4-02 | 明文只在授权运行内存中出现 | UI、任务导出、diff、日志和错误均不暴露明文 |
| P5-01 | JS 配置访问器 | 实现 `JsNodeConfigAccessor` | `iengine-app` | P4-01、P4-03 | 脚本按 key 读取通用参数 | `get`、`has`、`getOrDefault` 结果正确；缺失 key code 明确 |
| P5-02 | 文件 facade | 实现 `FileScriptExecutor` | `iengine-app` | P3-06、P5-01 | 只暴露 Map/List/基础值的受控 Java 方法 | JS 无法取得 FTPClient、TapFileStorage 或 InputStream |
| P5-03 | 引擎注入 | 改造 engine 构建、事件调用和关闭 | `HazelcastJavaScriptProcessorNode.java` | P5-01、P5-02 | 注入 `jsNodeConfig`/`ftp`，成功后继续事件 | `copy` 成功后返回事件；失败时当前事件不输出且保留原错误码 |
| P5-04 | 引擎并发 | 文件能力节点关闭记录级并发 | `supportConcurrentProcess()` | P5-03 | 避免 FTP 当前目录和数据连接跨线程串扰 | 文件能力开启时返回 false；无文件能力旧节点仍按原并发配置运行 |
| P5-05 | 引擎资源 | 实现 session、facade、engine 的关闭顺序 | `doClose()` | P5-03、P3-02 | 任务停止不遗留 socket、线程和 session | 正常关闭、异常关闭、任务取消均能释放资源且不影响其他引用 |
| P5-06 | 安全边界 | 验证 `ScriptUtil` HostAccess 不扩大 | `ScriptUtil.java` | P5-02 | 只通过注入对象开放能力 | `Java.type` 无法访问 FTPClient、File、Runtime、ProcessBuilder |
| P6-01 | 试运行 | 传递 `scriptParams` 到 test-run 任务 | `js-processor/index.tsx`、`JSProcessNodeTestRunService.java` | P4-01、P5-03 | 试运行与正式运行使用相同参数模型 | 试运行能读取参数；旧 test-run 请求兼容 |
| P6-02 | 试运行 dry-run | 实现只校验不写入的 facade/service | `DryRunFileOperationService` | P3-06、P6-01 | 防止调试误写生产 FTP | `copy/copyBatch` 只返回 `DRY_RUN`；目标无新增/覆盖文件 |
| P6-03 | 试运行超时 | 统一 JS 总超时和文件操作超时 | engine/test-run | P6-02 | 超时后线程和远端连接可停止 | 超时返回明确 code；没有后台无主传输 |
| P7-01 | 前端参数编辑器 | 增加任意参数增删改、类型、加密和描述 | `JavaScript.js`、`JsNodeConfigEditor` | P4-01 | 用户无需创建 FTP 连接或记忆 ID | UI 可增删参数；重复 key 即时提示；密文掩码显示 |
| P7-02 | 前端脚本模板 | 增加 `jsNodeConfig.get`、`ftp.copyByConfig` 模板 | `js-processor/index.tsx` | P5-02 | 降低脚本编写错误 | 模板生成的 JS 可通过语法和试运行校验 |
| P7-03 | 前端试运行展示 | 展示状态、字节数、耗时和脱敏错误 | `js-processor/index.tsx` | P6-01 | 用户能区分 `COPIED`、`REUSED`、`DRY_RUN` | 展示不包含密码、token、secret 或完整连接串 |
| P8-01 | 单元测试 | API、mapper、path、verify、retry、accessor | 各模块 test | 对应实现包 | 覆盖正常、非法和边界输入 | 单元测试稳定通过，关键分支覆盖：缺 key、越界、冲突、重试、dry-run |
| P8-02 | 协议集成测试 | FTP 实际服务及已有协议回归 | storage/service integration tests | P2、P3 | 验证共享 service 真正复用 adapter | FTP 复制、rename、中文路径、断连恢复成功；其他协议边界明确 |
| P8-03 | 引擎集成测试 | JS 脚本到事件下游的完整链路 | iengine tests | P5、P6 | 验证文件结果决定事件是否输出 | success 输出 1 条；失败不输出；A/B 事件互不回滚 |
| P8-04 | 安全和兼容测试 | secret、导出、skip error、旧任务 | TM/engine/web tests | P4~P7 | 确认不泄密、不改变旧任务 | 旧任务回归通过；文件错误默认不可跳过；日志脱敏 |
| P8-05 | 性能和稳定性测试 | 大文件、批量、连接回收、取消 | performance/stability tests | P3、P5 | 评估资源上限和取消行为 | 内存、session、耗时均受限；取消后无遗留线程/socket |
| P8-06 | 发布门禁 | 文档、开关、迁移和回滚检查 | release checklist | P8-01~P8-05 | 可灰度上线并快速关闭功能 | feature switch 可关闭；关闭后旧 JS 仍可运行；无未迁移字段异常 |

## 4. 分阶段详细实施目标和验收标准

### P0：设计冻结和工程脚手架

**实现目标**

- 冻结持久化字段 `scriptParams`、运行时对象名 `jsNodeConfig`、文件对象名 `ftp`。
- 冻结一期方法：`copy`、`copyByConfig`、`copyBatch`、`exists`；默认不开放 `delete`。
- 冻结文件状态：`COPIED`、`REUSED`、`DRY_RUN`。
- 冻结并发策略：文件能力开启时 JS 节点关闭记录级并发；共享服务不跨线程复用非线程安全 storage。
- 建立 feature switch，使功能可以在不影响旧任务的情况下关闭。

**验收标准**

1. 详细设计中的对象名、字段名、错误码和方法名不存在相互矛盾。
2. 旧任务 JSON 不含 `scriptParams` 时可以正常反序列化。
3. 关闭 feature switch 后不创建文件 service、session 或远端连接。
4. 代码评审确认引擎不直接依赖 Apache Commons Net 或 `FtpFileStorage` 实现类。

### P1：公共文件操作 API

**实现目标**

- 在 `tapdata-api` 增加稳定 DTO 和 service interface。
- 请求包含 source、target、路径、overwrite、verify、checksum、retry、timeout、dryRun。
- 结果只包含状态、路径摘要、字节数、校验摘要、attempt 和耗时。
- 定义统一异常，使 `HazelcastJavaScriptProcessorNode` 能保留错误 code。

**验收标准**

1. API 模块可独立编译，不依赖 connector 实现、Spring bean 或引擎类。
2. DTO 可在 GraalJS facade 与 connector service 之间进行 Map 转换。
3. 序列化、反序列化和空字段行为有单元测试。
4. 异常 message 中禁止输出 password、secret、完整 endpoint params。

### P2：TapFileStorage 和 FTP 适配器

**实现目标**

- 让 storage 明确声明是否支持 rename、mkdir、checksum 和 append。
- 修复 `FtpFileStorage.saveFile` 未检查 `storeFile` 返回值的问题。
- 修复 `openFileOutputStream` 未检查 `completePendingCommand` 的问题。
- 实现同一 FTP endpoint 内的可靠 rename，补充目录创建和中文编码测试。
- 对 SFTP、SMB 等协议声明能力差异，不能在 service 中猜测。

**验收标准**

1. FTP `storeFile=false`、`completePendingCommand=false` 均能触发失败。
2. FTP 临时文件可以 rename 到目标路径；rename 失败不会返回成功。
3. 源文件不存在、连接失败、权限失败分别映射为不同错误类别。
4. 中文文件名、子目录、零字节文件和大文件流式写入测试通过。
5. 不支持原子发布的 adapter 在 capability 中明确标记并被 service 拒绝。

### P3：共享文件服务和数据源复用

**实现目标**

- 从 `FileConnector` 当前的 `FileProtocolEnum -> TapFileStorageBuilder` 链路抽取公共装配能力。
- 实现任意参数前缀到 `FileEndpoint` 的受控映射。
- 实现 session 指纹、引用计数、每线程隔离、失效重建和空闲回收。
- 实现路径规范化、临时文件、流式 copy、校验、重试、目标探测和 `REUSED`。
- 让 `FileConnector.connectionTest` 和 JS facade 使用同一 validator/mapper。

**验收标准**

1. 数据源 connector 和 JS facade 的复制操作最终都调用 `TapFileStorage`，没有引擎专属 FTP 实现。
2. 相同 endpoint 指纹可复用 session；不同租户、凭据、协议或 rootPath 不会复用。
3. `../../`、绝对路径、协议 URL 和控制字符均被拒绝。
4. 传输失败只清理本次临时文件，不删除其他任务的正式文件。
5. 目标已存在且校验一致返回 `REUSED`；内容不一致返回 `FILE_TARGET_CONFLICT`。
6. `copyBatch` 任一失败不会返回整体 success，且返回失败索引/错误 code。
7. 源 connector 原有读、目录扫描、重连和 connection test 回归通过。

### P4：节点参数模型和密文处理

**实现目标**

- 在 `ScriptProcessNode` 增加 `@EqField protected List<JsNodeConfigParam> scriptParams`。
- 参数支持 `STRING`、`NUMBER`、`BOOLEAN`、`JSON`。
- 保存时校验 key 正则、唯一性、总量、单值大小和 JSON 格式。
- 对敏感值复用现有密文/secret 机制，运行时才解密。
- 节点复制、导入导出和配置 diff 保留参数结构但不泄露值。

**验收标准**

1. 新建、编辑、复制、导入和导出节点后参数不丢失。
2. 重复 key、非法 key、未知类型、超限值在保存阶段被拒绝。
3. 没有 secret 权限的用户不能启动依赖该参数的任务。
4. 任务 JSON、审计、diff、错误和日志中不出现明文密码或密文。
5. 旧任务没有该字段时得到空列表，原脚本行为保持不变。

### P5：引擎访问器、facade 和事件语义

**实现目标**

- 在 engine 初始化阶段把 `scriptParams` 转为 `JsNodeConfigAccessor`。
- 在 `buildEngine()` 中注入 `jsNodeConfig`；文件能力开启时注入 `ftp`。
- `FileScriptExecutor` 只接受 Map/List/基础值，内部调用公共 service。
- 修改 `wrapScriptProcessException`，保留文件错误 code 和 dynamic parameters。
- 文件调用成功后 JS 返回记录才入队；异常不输出当前事件。
- 文件能力节点关闭记录级并发，关闭时正确释放 facade、service handle、session 和 engine。

**验收标准**

1. `jsNodeConfig.get("key")` 能读取 string/number/boolean/json；缺失 key 返回 `JS_NODE_CONFIG_KEY_NOT_FOUND`。
2. `ftp.copyByConfig` 能通过 `sourcePrefix`/`targetPrefix` 读取并映射参数，脚本无需读取密码。
3. 成功返回 `COPIED` 或 `REUSED` 时，原业务记录继续到下游。
4. 文件失败、校验失败、超时和参数错误时，当前事件不进入下游。
5. 脚本无法通过 `Java.type` 获取 `FTPClient`、`File`、`Runtime`、`ProcessBuilder`。
6. 普通旧 JS 节点仍保持原有 source/target executor、并发和返回值行为。
7. 节点关闭或任务取消后无后台文件线程、未关闭流和泄漏 session。

### P6：试运行和 dry-run

**实现目标**

- test-run 请求携带 `scriptParams`，不单独维护另一套配置。
- 试运行通过 dry-run service 校验参数、路径、协议和读取权限。
- `copy`/`copyBatch` 在试运行时返回 `DRY_RUN`，禁止目标写入、覆盖和 rename。
- 文件超时受 JS 测试总超时约束，取消时关闭连接。

**验收标准**

1. 在 UI 试运行中调用 `jsNodeConfig.get` 能取得当前表单参数。
2. dry-run 能发现缺 key、协议未知、权限不足和路径越界。
3. dry-run 前后目标 FTP 文件列表、大小和修改时间不变。
4. 返回日志只包含状态、路径摘要、字节数、耗时和错误 code，不包含敏感值。
5. 试运行超时后没有继续传输或后台 socket。

### P7：前端配置和脚本体验

**实现目标**

- 在 JS 节点基本设置中增加通用参数编辑器，而不是 FTP 固定字段。
- 支持任意 key 的增删改、类型选择、加密标记和描述。
- 自动生成 `jsNodeConfig.get`、`has`、`getOrDefault` 和 `ftp.copyByConfig` 模板。
- 试运行结果展示 `COPIED`、`REUSED`、`DRY_RUN` 和脱敏错误。

**验收标准**

1. 用户可以不创建连接资源、不记连接 ID，直接在 JS 节点配置参数并运行。
2. 参数增删改后保存 DAG，刷新页面仍保持一致。
3. key 重复、key 非法和类型错误在前端即时提示，后端仍再次校验。
4. 加密值始终掩码显示；复制节点、导出任务和试运行不会显示明文。
5. 模板生成脚本可被后端编译并完成 dry-run。

### P8：测试、发布和回滚

**实现目标**

- 覆盖单元、协议集成、引擎集成、前端、性能、安全和兼容测试。
- 验证每条业务数据独立等待文件，数据库失败重放返回 `REUSED`。
- 提供 feature switch、灰度策略和回滚路径。

**验收标准**

1. 单文件、批量文件、重试、目标冲突、中文路径、零字节和大文件用例通过。
2. A 文件成功、B 文件失败时 A 不回滚，B 不输出。
3. FTP 成功后数据库失败再重放，文件不重复上传，数据库按既有幂等策略处理。
4. FTP、SFTP、SMB、S3FS 等协议均通过共享 service；不支持的能力有明确失败。
5. 多分区、任务取消、节点停止、连接重建和 session 回收无资源泄漏。
6. 旧 JS 任务、旧 test-run 请求、旧 DAG 导入全部回归通过。
7. 关闭 feature switch 后文件能力不可用但旧 JS 正常运行，可恢复到发布前版本。

## 5. 用例到开发任务的映射

| 用例 | 关键开发任务 | 必须通过的验收 |
| --- | --- | --- |
| 单条记录复制一个 FTP 文件 | P2-02、P2-03、P3-06、P5-03 | DT-03、DT-08 |
| 一条记录复制多个文件 | P3-06、P5-02 | DT-08，批次任一失败不输出 |
| 每条记录独立等待关联文件 | P5-03、P5-04、P8-03 | DT-09 |
| 数据库失败后的文件复用 | P3-05、P5-03、P8-03 | DT-04、DT-重放 |
| 任意参数 key 和加密密码 | P4-01~P4-03、P5-01、P7-01 | DT-02、DT-16 |
| 试运行不写生产 FTP | P6-01~P6-03、P7-03 | DT-10 |
| 路径越界和目标冲突 | P3-03、P3-05 | DT-05、DT-06 |
| 数据源 connector 与 JS 同时使用 | P3-02、P3-07、P8-02 | DT-12 |
| 禁止 JS 直接访问 FTP 客户端 | P5-02、P5-06 | DT-14 |

## 6. 交付物清单

| 交付物 | 内容 | 完成标志 |
| --- | --- | --- |
| 公共 API | operation DTO、service、provider、capability、错误码 | API 模块编译和单测通过 |
| 协议适配器 | FTP 修复及各协议 capability | adapter 集成测试通过 |
| 共享文件服务 | mapper、session、path、transfer、verify、retry、batch | service 集成测试通过 |
| 节点模型 | `scriptParams` DTO、校验、密文处理、导入导出 | TM 回归通过 |
| 引擎实现 | accessor、facade、注入、事件语义、关闭和并发 | iengine 集成测试通过 |
| 试运行 | dry-run service、请求传递、超时和脱敏 | 目标 FTP 无写入验证通过 |
| 前端 | 参数编辑器、模板、掩码、试运行展示 | UI 测试和人工验收通过 |
| 测试报告 | 单元、集成、性能、安全、兼容矩阵 | P8 全部门禁通过 |
| 发布方案 | feature switch、灰度、监控和回滚说明 | 可在预发关闭并恢复旧行为 |

## 7. 发布前最终检查表

- [ ] `scriptParams` 是唯一持久化字段，`jsNodeConfig` 是唯一脚本配置访问器名称。
- [ ] `config.get(key)` 没有出现在示例、模板或实现接口中。
- [ ] 引擎没有创建 `FTPClient`、Sftp channel 或直接反射 `FtpFileStorage`。
- [ ] 数据源 `FileConnector` 和 JS facade 使用同一共享 service、mapper、session 和 adapter。
- [ ] FTP `storeFile`、`completePendingCommand`、rename 和目录创建均有结果检查。
- [ ] 临时文件发布、目标复用、checksum/size 校验和冲突策略有自动化测试。
- [ ] 文件能力节点的并发、超时、取消、关闭和 session 引用计数有验证。
- [ ] 试运行永远不会写入目标 FTP。
- [ ] 密码、token、密文和 secretRef 不进入日志、错误、导出和试运行结果。
- [ ] 文件依赖错误默认不可跳过，旧 JS 任务仍保持原行为。
- [ ] feature switch 可以关闭新功能，关闭后旧任务可启动、运行和回滚。
