# 引擎集成测试（Engine IT）

引擎（iengine-app）集成测试：**不依赖 MongoDB，只依赖管理端 TM**。引擎以 DAAS 形态
（`isCloud=false`）启动，与 TM 的全部交互（下载 connector、获取任务配置、加载模型、上报心跳、更新任务
状态/指标等）由本目录的 `MockTM` 模拟；任务真实运行（真实 connector 读写真实数据库），
用于验证引擎调度、全量/增量流转、生命周期、断点续跑等行为。

**用例不依赖特定数据源，只依赖 connector**：用例对源/目标的全部写读操作（建表/插数/
更新/删除/查询/断言）统一经 `tapdata-it-common` 的 `ConnectorVerifier` 旁路验证器完成；
验证器从任务源/目标节点的 connector 实例反射获取（`TaskNodeVerifiers`：
`ConnectorNodeService` 全局注册表 → `dagId=taskId` + `associateId` 含节点 id →
`ConnectorNode.getConnector()` → `VerifierFactory` 按成员类型装配：
JdbcContext → JdbcVerifier，MongoClient → MongoVerifier）。源/目标替换为任意
connector 时，只需新增一个配置具体类实现 `EngineIT` 的扩展点，通用用例代码不变。
**通用用例集中声明在 `EngineIT`**：对任意 connector 组合都成立的用例（冒烟/生命周期/
全量读/增量/目标写/断点续跑）以 `@Test` 方法定义在基类，JUnit 5 会在每个具体组合子类中
执行继承的用例；组合/连接器特有用例在继承相应组合类的单独类中编写。

## 架构

```
┌────────────────────────────── 测试 JVM（failsafe） ──────────────────────────────┐
│                                                                                  │
│  EngineIT（抽象基类：JVM 级引擎单例 + 数据源无关的操作辅助 + 通用用例库）        │
│    ├── 通用用例（@Test，任意组合通用，由具体子类继承执行）：                     │
│    │     冒烟 2 + D1 生命周期 5 + D2 全量读 2 + D3 增量 4 + D4 目标写 1 + 断点 1 │
│    ├── 扩展点：sourceSpec()/targetSpec()/testTableFields()/prepareEnvironment()  │
│    │           directSourceVerifier()/directTargetVerifier()                     │
│    ├── EngineRuntime ── 启动 MockTM + 预置数据 + Spring 上下文（引擎本体）       │
│    │     ├── MockTM（JDK HttpServer）── login / singleton-lock / health /        │
│    │     │    Task CRUD / transformAllParam / syncProgress / pdk jar 下载等端点   │
│    │     └── 引擎启动后经 Spring Boot 启动流程向 MockTM 注册、轮询任务            │
│    ├── TaskFixture ── Connection 预置（连接规格由扩展点提供）                    │
│    ├── TaskDtoBuilder ── 任务 DTO 构造（migrate / fullSync）                     │
│    ├── TaskNodeVerifiers ── 反射任务节点 connector → 装配旁路验证器              │
│    └── TestTaskService ── 下发任务 / 等待状态 / 同步阶段 / 数据一致性断言        │
│                                                                                  │
│  MySqlMongoIT（具体类：MySQL 源 → MongoDB 目标的配置，继承执行全部通用用例）     │
│  组合特有用例：继承相应组合类的单独类（当前无）                                  │
└──────────────────────────────────────────────────────────────────────────────────┘
```

| 组件 | 职责 |
| --- | --- |
| [MockTM](java/io/tapdata/engine/it/mock/MockTM.java) | 模拟管理端 TM：`login`、`agent/singleton-lock`、`health`、`Task` 集合 REST 代理（find/findById/updateById/updateField）、`Task/transformAllParam/{taskId}`（任务配置下发）、`Task/syncProgress/{taskId}`（进度上报）、`pdk/jar/v2`（connector jar 下载）+ `pdk/checkMd5/v3`（jar md5 校验）。内存集合 + 简易查询匹配（支持顶层 `_id`+`$or`/`$and` 混合） |
| [EngineRuntime](java/io/tapdata/engine/it/EngineRuntime.java) | 公共运行时：启动 MockTM（端口 `engine.it.tm.port`，默认 18080）、预置 Agent/Connection 基础数据、启动 Spring 上下文（真实引擎）、提供下发任务/查询状态/请求停止等 API。`close()` 关闭引擎与 MockTM |
| [TaskFixture](java/io/tapdata/engine/it/TaskFixture.java) | Connection 等预置数据定义（连接规格由基类扩展点提供，连接 ID 固定为 24 位十六进制，引擎侧需转 ObjectId） |
| [TaskDtoBuilder](java/io/tapdata/engine/it/TaskDtoBuilder.java) | 任务 DTO 构造：`buildMigrateTask`（迁移：全量+增量）、`buildFullSyncTask`（全量同步）。节点 id、边、转换参数按引擎运行契约拼装 |
| [TaskNodeVerifiers](java/io/tapdata/engine/it/TaskNodeVerifiers.java) | 从运行中任务的节点反射获取 connector 并装配旁路验证器：反射 `ConnectorNodeService` 私有 `connectorNodeMap` → `dagId=taskId` 且 associateId 含节点 id → `ConnectorNode.getConnector()` → `VerifierFactory`。`awaitVerifier` 轮询等待连接器 init 完成 |
| [EngineIT](java/io/tapdata/engine/it/EngineIT.java) | 抽象基类 + **通用用例库**：JVM 级引擎单例（`@BeforeAll` 启动 + shutdown hook 统一关闭）、数据源无关的源表准备辅助（`prepareSourceTable` 直连优先 / `insertSourceRows` / `updateSourceRow` / `deleteSourceRow`）、任务日志断言；15 个通用 `@Test` 用例（冒烟/生命周期/全量读/增量/目标写/断点续跑）声明在本类，由具体组合子类继承执行。扩展点：`sourceSpec()` / `targetSpec()` / `testTableFields()` / `prepareEnvironment()` / `directSourceVerifier()` / `directTargetVerifier()`（基类无任何数据源硬编码） |
| [MySqlMongoIT](java/io/tapdata/engine/it/MySqlMongoIT.java) | MySQL(源) → MongoDB(目标) 组合的**纯配置具体类**：实现扩展点（连接规格 + 表字段规格 + 源库预创建 + 直连验证器），环境支持 `IT_MYSQL_*` / `IT_MONGO_*` 环境变量覆盖；通用用例经 JUnit 继承自动执行。扩展新组合（如 PG → Doris）仿照本类新增配置具体类即可；组合特有用例在继承本类的单独类中编写 |
| [TestTaskService](java/io/tapdata/engine/it/TestTaskService.java) | 用例常用编排：`startMigrateTask` / `startFullSyncTask` / `startTask`（taskDto 复用重启）/ `awaitStatus` / `awaitRunning` / `awaitSyncStage` / `stopTask` / `assertDataConsistent(taskId, ...)`（经任务源/目标节点验证器逐行逐列比对）/ `assertDataConsistentDirect(sourceVerifier, targetVerifier, ...)`（直连验证器版，任务完成后断言） |

### 引擎与 TM 交互的关键端点（MockTM 实现）

| 端点 | 用途 |
| --- | --- |
| `POST /api/login` | 引擎注册（返回 access_token），注册后进入 `backend_url` 心跳/任务轮询 |
| `GET /api/agent/singleton-lock/{instanceNo}` | 引擎单例锁（返回 200 + `singletonTag`） |
| `GET /api/health` | TM 健康检查 |
| `GET /api/Task/...` / `POST /api/Task/...` | Task 集合 CRUD 代理（引擎认领 `wait_run` 任务、上报状态/进度） |
| `GET /api/Task/transformAllParam/{taskId}` | 任务配置下发（表模型 + 转换参数），引擎 `engineTransformSchema` 消费 |
| `POST /api/Task/syncProgress/{taskId}` | 引擎上报各节点同步进度（`syncStage`：`INIT` → `CDC`/`FULL` → `DONE`） |
| `GET /api/pdk/jar/v2?pdkHash=&pdkBuildNumber=` | connector jar 下载（按任务配置的 pdkHash/fileName 定位 jar，返回二进制流） |
| `GET /api/pdk/checkMd5/v3?pdkHash=&fileName=` | jar md5 校验（返回与 `PdkSourceUtils.getFileMD5` 相同格式的 md5，避免引擎反复重下） |

## 环境依赖（当前组合：MySQL → MongoDB，由 `MySqlMongoIT` 定义）

| 依赖 | 位置 | 说明 |
| --- | --- | --- |
| MySQL | `127.0.0.1:13306`，root/root（`IT_MYSQL_*` 环境变量可覆盖） | 源库 `it_smoke_db` 由 `MySqlMongoIT.prepareEnvironment()` 预创建（连接器连接 URL 含库名，库必须存在）；测试表随机名 `it_tbl_*`，任务下发前经直连验证器创建 |
| MongoDB | `127.0.0.1:27017`（`IT_MONGO_*` 环境变量可覆盖） | 目标库 `it_smoke_db`（集合由引擎写入时隐式创建）；另作引擎任务状态存储库 `tapdata`（failsafe 注入的 `TAPDATA_MONGO_URI`，DAAS 形态下 PdkStateMap 落此库） |
| connector jar | `~/.m2/repository/io/tapdata` 下的 `{mysql|mongodb}-connector/1.0-SNAPSHOT/*.jar`（递归扫描） | 该目录作为 MockTM `pdk/jar/v2` 的下载源（模拟 TM 侧连接器包库）；引擎侧无本地预置，运行任务时按任务配置（DatabaseType 的 pdkHash/jarFile/jarRid）自动下载并加载 |
| **JDK 17** | 本机（**不能是 18+**） | 与引擎生产镜像 `eclipse-temurin:17-jdk` 一致；JDK 18+ 上 chronicle-core 2.21.91 初始化即抛 AssertionError（详见「关键实现要点」）。`EngineRuntime.checkEnv()` 会先拦住并提示原因 |

## 运行方式

```bash
# 在 iengine-app 模块目录内执行（聚合根跑 failsafe 会报 "No tests matching pattern"）
cd tapdata/iengine/iengine-app

# 集成测试默认不执行（与连接器模块一致），加 -DskipITs=false 执行全部 IT（含冒烟 + M1 + M2）
# 必须用 JDK 17（与引擎生产镜像一致），否则 chronicle-core 初始化失败→所有任务 runError
# 注：JAVA_HOME 要指向 JDK 17 的**真实路径**；本机 JDK 17 装在 /Users/lg/app/java 下、
# 未注册到系统，`/usr/libexec/java_home -v 17` 会回退返 JDK 21（由 checkEnv 拦住）
JAVA_HOME=/Users/lg/app/java/jdk-17.0.12.jdk/Contents/Home /Users/lg/app/maven/maven/bin/mvn -o test-compile \
  failsafe:integration-test failsafe:verify -DskipITs=false

# 指定用例（通用用例均在组合类中执行，-Dit.test 按组合类 + 方法名过滤）
JAVA_HOME=/Users/lg/app/java/jdk-17.0.12.jdk/Contents/Home /Users/lg/app/maven/maven/bin/mvn -o test-compile \
  failsafe:integration-test failsafe:verify -DskipITs=false \
  -Dit.test=MySqlMongoIT#should_start_task_from_wait_run+should_incremental_insert

# 指定单个方法
JAVA_HOME=/Users/lg/app/java/jdk-17.0.12.jdk/Contents/Home /Users/lg/app/maven/maven/bin/mvn -o test-compile \
  failsafe:integration-test failsafe:verify -DskipITs=false \
  -Dit.test=MySqlMongoIT#should_incremental_insert

# 自定义 MockTM 端口（默认 18080，冲突时换端口）
JAVA_HOME=/Users/lg/app/java/jdk-17.0.12.jdk/Contents/Home /Users/lg/app/maven/maven/bin/mvn -o test-compile \
  failsafe:integration-test failsafe:verify -DskipITs=false \
  -Dit.test=MySqlMongoIT -Dengine.it.tm.port=18081
```

> 在 IDEA 中直接跑 `MySqlMongoIT`：必须把运行配置的 **Enable assertions（`-ea`）去掉**，
> 并手工补上 pom 里 failsafe `environmentVariables` 的那几个环境变量（`app_type` /
> `isCloud` / `TAPDATA_MONGO_URI` / `process_id` / `mode` / `cloud_accessCode` /
> `TAPDATA_WORK_DIR` / `backend_url`，`backend_url` 指向 `http://127.0.0.1:18080/api/`）；
> 未满足时 `EngineRuntime.checkEnv()` 会快速失败并提示缺失项（不建议用 IDEA 跑回归，
> 以 maven failsafe 为基准）。

> 残留进程清理：引擎为 JVM 级单例 + shutdown hook 关闭，failsafe 结束后 fork JVM
> 可能不退出（SIGTERM 会被挂起的 hook 阻塞）。报 `MockTM 端口已被占用` /
> Hazelcast 5701 冲突时：`lsof -nP -iTCP:18080 -iTCP:5701` 定位后 `kill -9 {pid}` 重跑。

引擎运行环境（`app_type=DAAS`、`isCloud=false`、`TAPDATA_MONGO_URI`、`backend_url`、`TAPDATA_WORK_DIR` 等）
由 pom 中 failsafe 的 `environmentVariables` 注入（引擎通过 `System.getenv` 读取，测试
JVM 内不可改，故必须由 failsafe 注入）。DAAS 形态下集合读写仍全走 MockTM REST 代理，
而任务状态存储（`PdkStateMap`）按非 cloud 分支落 `TAPDATA_MONGO_URI` 指向的 MongoDB。

**运行产物**：

- 任务日志：`target/engine-it-work/logs/jobs/{taskId}.log`
- 引擎日志：`target/engine-it-work/logs/` 下各模块日志
- connector 下载源：`~/.m2/repository/io/tapdata`（MockTM 侧，模拟 TM 连接器包库）
- connector 下载落盘：`dist/{jarFile}__{jarRid}__.jar`（引擎侧，`PdkUtil` 固定目录，每次运行前清理）

## 用例清单

通用用例均声明在 `EngineIT`，由具体组合类（当前 `MySqlMongoIT`）继承执行，共 15 例。

### 冒烟（2 例）

| 用例 | 验证点 |
| --- | --- |
| should_engine_start_and_heartbeat | 引擎启动 + 调度器就绪 + singleton-lock + 心跳上报（不依赖真实数据源） |
| should_task_full_flow_smoke | submitTask 全量流转（任务认领 → 全量读取 → 写入目标端 → 状态上报） |

### 任务生命周期（5 例）

| 用例 | 验证点 |
| --- | --- |
| should_start_task_from_wait_run | 正常启动：wait_run → running → 全量 → CDC，端到端一致 |
| should_idempotent_start | 重复认领：不重建 DAG、不重读全量（"Starting batch read from 1 tables" 仅 1 次） |
| should_graceful_stop | 优雅停止：stopping → 停止流转 → stopped（MockTM `updateField` 就地更新认领停止信号） |
| should_natural_complete | 全量任务跑完自然 complete，完成后经直连验证器断言端到端一致 |
| should_error_then_retry | 启动阶段失败（transformAllParam 200+空 data）→ runError |

### 全量 / 增量 / 目标写入 / 断点（8 例）

| 用例 | 验证点 |
| --- | --- |
| should_normal_snapshot_serial | 多表全量串行、每表不重读 |
| should_snapshot_complete_event | 全量完成后进入 CDC（syncStage=CDC + "Batch read completed"） |
| should_incremental_insert | 增量 insert 流转到目标端 |
| should_incremental_update | 增量 update 流转到目标端 |
| should_incremental_delete | 增量 delete 流转到目标端 |
| should_offset_advance_after_write | 目标端写入后 syncProgress offset 推进 |
| should_data_consistent_end_to_end | 50 行全量 + 20 行增量端到端逐行逐列一致 |
| should_restart_replay_no_loss | 停止→重启后续跑从 offset 拾取新增量事件不丢不重（updateOrInsert 幂等） |

## 关键实现要点（踩坑记录）

- **MockTM.findById 返回深拷贝副本**：对副本 `put` 不生效，必须用 `updateField` 就地更新
  内部集合（否则引擎 `findStopTask` 永远看不到 stopping 状态，停止指令无法认领）。
- **失败注入不能用 HTTP 500**：`RestException` 会被 `TmUnavailableException.isInstance`
  识别为 TM 不可用，引擎只告警不置 runError。要模拟任务配置缺失需返回 **200 + 空 data**
  → `engineTransformSchema` 拿到 null → NPE → `TASK_FAILED_TO_LOAD_TABLE_STRUCTURE` → runError。
- **tapType 必须是 JSON 字符串**（`{"type":8}`=TapNumber / `{"type":10}`=TapString）：
  裸字符串抛 JsonParseException 字段被跳过，导致 nameFieldMap 为空、SQL 生成语法错误。
- **JVM 级引擎单例**：failsafe 同 JVM 顺序执行多个 IT 类，`@AfterAll` 反复启停引擎会因
  Hazelcast 5701 / MockTM 端口未释放而 BindException——引擎启动后注册 shutdown hook 统一关闭。
  所有 IT 类（含冒烟）统一继承 `EngineIT` 复用单例，不再各自 try-with-resources 启停。
- **Hazelcast 实例不随 Spring context 关闭**：`HazelcastTaskService` @PostConstruct 创建具名实例，
  但 @PreDestroy 只关缓存失效服务不关实例——`EngineRuntime.close()` 必须显式
  `Hazelcast.shutdownAll()`，否则同 JVM 内下一次引擎启动抛实例已存在 / 端口占用。
- **任务日志断言用唯一子串**：`countTaskLogOccurrences` 是 indexOf 子串匹配，
  "Starting batch read from table: xxx" 含 "Starting batch read" 子串，断言需精确到
  "Starting batch read from 1 tables" 这类唯一行。
- **connector 无本地预置，按任务配置从 MockTM 下载**：不设置 `pdk_external_jar_path`，引擎经
  `PdkUtil.downloadPdkFileIfNeed` 按 DatabaseType 的 pdkHash/jarFile/jarRid 从 `pdk/jar/v2`
  下载到 `{user.dir}/dist` 后 `refreshJars` 加载。`EngineRuntime` 启动前清理残留 jar
  保证每轮真实走下载链路；文件已存在时引擎直接复用不下载。
- **checkMd5/v3 必须返回真实 md5**：返回空串会被判不一致 → 删本地 jar 重下（循环 3 轮后
  本地无 jar，后续任务靠重新下载恢复，拖慢且不稳定）。MockTM 返回与
  `PdkSourceUtils.getFileMD5` 相同格式的 md5（`BigInteger(1, digest).toString(16)` 小写 hex）。
- **源表准备优先在下发前经直连验证器完成**：连接器 init 很快时，“下发后再经任务节点
  验证器建表插数”存在竞态（快照读先于建表，读到空表即完成）。用例一律先调
  `prepareSourceTable(table, rows)`（子类直连验证器，任务下发前）；无直连验证器的组合才用
  `prepareSourceTable(taskId, table, rows)`（任务节点验证器，下发后、快照读前）。
- **停机/完成后任务节点验证器失效**：任务停止或自然完成后引擎 `doClose` 会经
  `PDKIntegration.releaseAssociateId` 销毁 connector（连接池随之关闭）并把节点从
  `ConnectorNodeService` 移除——再按 taskId 反射找不到，已持有的验证器也随之失效。
  运行中任务的一致性断言需在停止/完成前完成（`assertDataConsistent`）；
  全量任务自然完成后的断言（D1.4/D2.1）改用子类动态提供的**直连旁路验证器**
  （`directSourceVerifier/directTargetVerifier`，不依赖引擎连接器生命周期）；
  断点续跑用例停机后的源端写入改在重启后新连接器就绪时进行（事件仍在停机 offset 之后）。
- **验证器反射必须跨 classloader**：引擎用外部 jar classloader 加载 connector，与测试
  classpath 的同名类不同源：`VerifierFactory` 用类名继承链匹配（非 isInstance），
  `MongoVerifier` 驱动类经运行时实例的 classloader 解析，否则报 "object is not an
  instance of declaring class"。
- **连接 id 必须为 24 位十六进制**：引擎侧 `generateQualifiedName` 用
  `ObjectId.toHexString()` 拼 qualifiedName，`TaskFixture` 预置时 `new ObjectId(id)` 校验。
- **connector jar 目录递归扫描 + `-connector` 命名约定**：maven 仓库布局为
  `{artifactId}/{version}/{jar}`（顶层无 jar）；pdkHash 前缀回退匹配不能只用前缀子串，
  否则 `mongodb-` 会先命中 `mongodb-storage-module` 等非连接器构件。
- **必须用 JDK 17 跑 IT**：chronicle-core 2.21.91（任务日志缓存 `TaskLogger`/`ObsLogger`
  依赖）在 `OS.<clinit>` 里用 `Jvm.getMethod(FileChannelImpl, "unmap0", long, long)` 取方法，
  JDK 18+ 上该方法签名已变，取不到即抛 **AssertionError（与 `-ea` 无关，无法绕过）**，
  表现为引擎启动正常但“所有任务 startTask 失败 → runError”。`EngineRuntime.checkEnv()`
  已加 JDK 版本卡点，避免排查方向被带偏（升级 chronicle 属于依赖变更，不在 IT 范围内）。
- **DAAS 形态下任务状态存储不能配 `type=httptm`**：`PdkStateMap.initConstructMap()` 仅在
  `AppType.currentType().isCloud()` 为真时走 `initHttpTMStateMap`；DAAS 分支进
  `initNodeStateMap`，开头就要 `documentIMapV2.isEmpty()` 探测 V1/V2，而
  `HttpTMIMap` 未实现 `PersistenceStorageStore.isEmpty()`（基类直接抛
  `UnsupportedOperationException`，`MongoDBIMap`/`RocksDBIMap` 都实现了），于是连接器节点
  init 必失败。因此 MockTM 预置的 ExternalStorage 按生产 DAAS 配 `type=mongodb`
  （`uri` 取 failsafe 注入的 `TAPDATA_MONGO_URI`）；若需回到 cloud/DRS 形态验证，
  需同时改 `app_type`/`isCloud` 并将存储改回 httptm。
- **IDEA 里跑 IT 必须去掉 `-ea`**：pom 已显式 `enableAssertions=false`，而 IDEA 的 JUnit
  运行配置模板默认加 `-ea`。mysql connector（debezium fork）
  `MySqlChangeEventSourceFactory.getStreamingChangeEventSource` 只调
  `ChangeEventQueue.disableBuffering()` 不调 `flushBuffer()`，而 `disableBuffering` 第 194 行是
  `assert bufferedEvent == null : "Buffer must be flushed"`——仅 `-ea` 下生效。一旦触发，
  `source_stream_read` 立即失败并进入 Auto Retry（周期 60 s，重试同样失败），CDC 事件永不
  进目标端，表现为 `should_incremental_update`（`target value not updated`，目标端仍是旧值）
  与 `should_offset_advance_after_write`（行数不一致 Expected 6 / Actual 3）在 120 s 轮询后超时。
  生产引擎 JVM 不带 `-ea`（不抛异常，但 `bufferedEvent` 会被静默丢弃，属连接器侧待修），
  故 IT 也必须关断言才有意义；`EngineRuntime.checkEnv()` 已加卡点，`-ea` 时直接抛
  `IllegalStateException` 并提示去掉 -ea，不再用 120 s 超时的迷惑失败误导排查。
- **下发任务只能用 `MockTM.upsert`，不能用 `put`**：`put` 的语义是“清空该集合后写入”，
  会把其它仍在运行的任务文档一并删掉。引擎 `TaskPingTimeMonitor`（5 s 一次）按
  `_id + status $nin [error, schedule_failed] + agentId` 更新 `pingTime`，文档消失后
  TM 返回 `count=0` → `modifiedCount=0`；`isCloud=true` 时只告警继续跑，
  **DAAS（`isCloud=false`）分支直接 `internalStop()` 自停那个健康任务**。停任务时
  `hazelcast-persistence` 的 `MongoClientHolder.close()` 会把进程级共享引用计数的
  MongoClient 归零并物理关闭，与下一个任务并发 `getMongoClient()` 竞态 →
  `IllegalStateException: state should be: server session pool is open`
  （栈经 `MongoDBIMap.isEmpty` ← `PdkStateMap.initNodeStateMap`），表现为随机用例
  `did not reach syncStage CDC within 180s`。这类串扰与顺序相关，用例本身逻辑无错。
