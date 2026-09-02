package io.tapdata.engine.it;

import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.engine.it.mock.MockTM;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.flow.engine.V2.schedule.TapdataTaskScheduler;
import io.tapdata.it.schema.TestFieldSpec;
import io.tapdata.it.verifier.ConnectorVerifier;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * 引擎集成测试基类 + 通用用例库：JVM 级引擎单例共享 + 数据源无关的任务数据操作。
 * <p>
 * <b>通用用例定义在本类</b>：引擎冒烟、任务全流转、生命周期（D1）、全量读（D2）、
 * 增量（D3）、目标写（D4）、断点续跑等对任意 connector 组合都成立的用例，以 {@code @Test}
 * 方法声明在本类。JUnit 5 会在每个具体子类中执行继承的测试方法，因此新增连接器组合只需
 * 继承本类并提供组合特有配置（扩展点），即可自动执行全部通用用例；
 * 组合/连接器特有的用例在继承相应组合类的单独类中编写。
 * <p>
 * 运行时通过 {@link #engine()} 静态单例共享（首个测试类触发启动，后续类复用）；
 * {@link #initEngine()} 自发现真实子类并实例化（避免基类硬编码子类）。
 * <p>
 * <b>数据源操作不依赖特定数据库</b>：用例对源/目标的全部写读操作（建表/插数/更新/删除/查询）
 * 统一经 tapdata-it-common 的 {@link ConnectorVerifier} 旁路验证器完成；验证器从任务
 * 源/目标节点的 connector 实例反射获取（{@link TaskNodeVerifiers}），按 connector 成员类型
 * 自动装配（JdbcContext → JdbcVerifier，MongoClient → MongoVerifier）。
 * 源/目标替换为任意 connector 时，只需在子类提供对应连接规格与表字段规格，基类与用例无需改动。
 * <p>
 * 扩展点（基类不硬编码任何数据源，由子类动态提供）：
 * <ul>
 *   <li>{@link #sourceSpec()} / {@link #targetSpec()}：源/目标连接规格（connector 类型、pdkHash、连接配置）</li>
 *   <li>{@link #testTableFields()}：测试表字段规格（方言类型直接用于旁路建表）</li>
 *   <li>{@link #directSourceVerifier()} / {@link #directTargetVerifier()}：直连数据源验证器（任务停止/完成后的断言）</li>
 *   <li>{@link #sourceNodeId()} / {@link #targetNodeId()}：DAG 节点 id（默认 TaskDtoBuilder 两节点 DAG）</li>
 *   <li>{@link #primaryKey()}：测试数据主键列名（默认 id）</li>
 * </ul>
 * <p>
 * 时序说明：源表准备（建表/种子数据）优先经子类直连验证器在任务下发**之前**完成，
 * 保证全量快照读必能读到完整数据（无竞态）；子类未提供直连验证器时回退到任务节点
 * 验证器（需在任务下发后、快照读前完成，见 {@link #prepareSourceTable(String, String, int)}）。
 */
public abstract class EngineIT {

	/** 源/目标连接 id（全部 IT 任务共用；连接规格内容由子类动态提供）。
	 *  必须为 24 位十六进制：引擎侧 generateQualifiedName 用 ObjectId.toHexString() 拼 qualifiedName，
	 *  TaskFixture 预置时 new ObjectId(id) 校验。 */
	protected static final String SOURCE_CONN_ID = "00000000000000000000000a";
	protected static final String TARGET_CONN_ID = "00000000000000000000000b";
	/** 等待任务节点连接器验证器可用的超时（覆盖调度认领 + jar 下载 + connector init） */
	private static final long VERIFIER_AWAIT_SECONDS = 180;

	/** JVM 级共享实例（真实子类实例，首个测试类触发创建） */
	static EngineIT engineIT;
	protected static TestTaskService taskService;
	private static EngineRuntime runtime;
	
	/** 共享引擎运行时（MockTM + Spring 上下文），首次调用触发启动 */
	protected static EngineRuntime engine() {
		if (runtime == null) {
			EngineRuntime.EngineRuntimeConfig config = new EngineRuntime.EngineRuntimeConfig();
			config.connectorJarDir = new java.io.File(resolveConnectorJarDir());
			try {
				runtime = EngineRuntime.start(config);
			} catch (Exception e) {
				throw new IllegalStateException("Start engine runtime failed", e);
			}
		}
		return runtime;
	}
	
	/**
	 * 全部测试前触发引擎启动。首次执行时经 {@link TestInfo} 拿到真实测试类并实例化，
	 * 并用子类提供的源/目标连接规格初始化 {@link #taskService}。
	 */
	@BeforeAll
	static void initEngine(TestInfo testInfo) {
		if (engineIT == null) {
			engine();
			Class<?> testClass = testInfo.getTestClass()
					.orElseThrow(() -> new IllegalStateException("Cannot resolve test class from TestInfo"));
			try {
				engineIT = (EngineIT) testClass.getDeclaredConstructor().newInstance();
			} catch (ReflectiveOperationException e) {
				throw new IllegalStateException("EngineIT 子类必须提供 public 无参构造器: " + testClass, e);
			}
			try {
				engineIT.prepareEnvironment();
			} catch (Exception e) {
				throw new IllegalStateException("prepareEnvironment failed", e);
			}
			taskService = new TestTaskService(engine(), engineIT.sourceSpec(), engineIT.targetSpec());
		}
	}
	
	/**
	 * 数据源环境前置钩子（首次启动时执行一次，默认空操作）。
	 * 用于子类预创建连接器 init 依赖的环境（如源库本身必须存在，
	 * 因连接 URL 含库名），仅限连接器组合子类实现。
	 */
	protected void prepareEnvironment() throws Exception {
	}

	// ===================== 源/目标扩展点（子类动态提供） =====================

	/** 源连接规格（connector 类型、pdkHash、连接配置）——子类动态提供 */
	protected abstract TaskFixture.ConnSpec sourceSpec();

	/** 目标连接规格（connector 类型、pdkHash、连接配置）——子类动态提供 */
	protected abstract TaskFixture.ConnSpec targetSpec();

	/** 测试表字段规格（方言类型直接拼入旁路建表）——子类动态提供 */
	protected abstract List<TestFieldSpec> testTableFields();

	/** 任务 DAG 源节点 id（默认 TaskDtoBuilder 两节点 DAG） */
	protected String sourceNodeId() {
		return TaskDtoBuilder.SOURCE_NODE_ID;
	}

	/** 任务 DAG 目标节点 id */
	protected String targetNodeId() {
		return TaskDtoBuilder.TARGET_NODE_ID;
	}

	/** 测试数据主键列名（行生成与一致性比对以此为基准） */
	protected String primaryKey() {
		return "id";
	}

	// ===================== 旁路验证器（从任务节点反射） =====================

	/** 任务源节点 connector 的旁路验证器（轮询至连接器 init 完成） */
	protected ConnectorVerifier sourceVerifier(String taskId) {
		return TaskNodeVerifiers.awaitVerifier(taskId, sourceNodeId(), toDataMap(sourceSpec().config), VERIFIER_AWAIT_SECONDS);
	}

	/** 任务目标节点 connector 的旁路验证器（轮询至连接器 init 完成） */
	protected ConnectorVerifier targetVerifier(String taskId) {
		return TaskNodeVerifiers.awaitVerifier(taskId, targetNodeId(), toDataMap(targetSpec().config), VERIFIER_AWAIT_SECONDS);
	}

	/**
	 * 直连源数据源的旁路验证器（不依赖任务节点 connector 生命周期）——子类动态提供。
	 * 用于任务停止/自然完成后的断言场景：此时引擎 doClose 已销毁任务节点连接器，
	 * 只有直连验证器仍可用。默认未实现（不需要完成/停止后断言的连接器组合无需提供）。
	 */
	protected ConnectorVerifier directSourceVerifier() throws Exception {
		throw new UnsupportedOperationException("子类未提供直连源验证器：覆写 directSourceVerifier()");
	}

	/** 直连目标数据源的旁路验证器（语义同 {@link #directSourceVerifier()}）——子类动态提供 */
	protected ConnectorVerifier directTargetVerifier() throws Exception {
		throw new UnsupportedOperationException("子类未提供直连目标验证器：覆写 directTargetVerifier()");
	}

	private static DataMap toDataMap(Map<String, Object> config) {
		return DataMap.create(config == null ? new LinkedHashMap<>() : new LinkedHashMap<>(config));
	}

	// ===================== 源端数据准备（数据源无关） =====================

	/** 生成随机表名（不触碰任何数据源） */
	protected static String randomTableName() {
		return "it_tbl_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
	}

	/**
	 * 建表并灌入 rows 行数据（直连优先，任务下发前后均可调用）。
	 * <p>
	 * 优先经子类直连验证器准备（任务下发前完成，全量快照读必能读到完整数据，无竞态）；
	 * 子类未提供直连验证器时回退到任务节点验证器（需在任务下发后、快照读前调用）。
	 */
	protected void prepareSourceTable(String table, int rows) throws Exception {
		ConnectorVerifier verifier = directSourceVerifierOrNull();
		if (verifier == null) {
			throw new IllegalStateException("无直连源验证器：无 taskId 的 prepareSourceTable 需要子类覆写 directSourceVerifier()");
		}
		verifier.createTable(table, testTableFields());
		if (rows > 0) {
			verifier.insert(table, generateRows(1, rows));
		}
	}

	/**
	 * 经任务源节点 connector 的验证器建表并灌入 rows 行数据（无直连验证器时的回退路径）。
	 * 必须在任务下发后调用：连接器在全量读之前完成注册，等待验证器就绪后
	 * 立即建表插数赶在快照读之前完成——但连接器 init 很快时存在竞态（快照读先于建表，
	 * 读到空表），能走直连验证器的子类优先用 {@link #prepareSourceTable(String, int)}。
	 */
	protected void prepareSourceTable(String taskId, String table, int rows) throws Exception {
		ConnectorVerifier verifier = sourceVerifier(taskId);
		verifier.createTable(table, testTableFields());
		if (rows > 0) {
			verifier.insert(table, generateRows(1, rows));
		}
	}

	/** 子类覆写了 directSourceVerifier 则返回直连验证器，否则返回 null（默认实现抛 Unsupported，仅回退路径可用） */
	private ConnectorVerifier directSourceVerifierOrNull() throws Exception {
		try {
			return directSourceVerifier();
		} catch (UnsupportedOperationException e) {
			return null;
		}
	}

	/** 从 startId 起插入 count 行（增量事件准备 / 停机期间写入） */
	protected void insertSourceRows(String taskId, String table, int startId, int count) throws Exception {
		sourceVerifier(taskId).insert(table, generateRows(startId, count));
	}

	/** 按主键更新单行首个非主键列（增量 update 事件准备） */
	protected void updateSourceRow(String taskId, String table, int id, String newValue) throws Exception {
		Map<String, Object> setValues = new LinkedHashMap<>();
		setValues.put(firstNonPrimaryKeyField(), newValue);
		sourceVerifier(taskId).update(table, setValues, primaryKey(), id);
	}

	/** 按主键删除单行（增量 delete 事件准备） */
	protected void deleteSourceRow(String taskId, String table, int id) throws Exception {
		sourceVerifier(taskId).delete(table, primaryKey(), id);
	}

	/** 从 startId 起生成 count 行测试数据：主键列取 int 序号，其余列取 "row-{序号}" 字符串 */
	protected List<Map<String, Object>> generateRows(int startId, int count) {
		List<Map<String, Object>> rows = new ArrayList<>(count);
		for (int i = 0; i < count; i++) {
			int id = startId + i;
			Map<String, Object> row = new LinkedHashMap<>();
			for (TestFieldSpec field : testTableFields()) {
				row.put(field.getName(), field.isPrimaryKey() ? id : "row-" + id);
			}
			rows.add(row);
		}
		return rows;
	}

	private String firstNonPrimaryKeyField() {
		for (TestFieldSpec field : testTableFields()) {
			if (!field.isPrimaryKey()) {
				return field.getName();
			}
		}
		throw new IllegalStateException("testTableFields 至少需要一个非主键列");
	}

	// ===================== 任务日志断言辅助 =====================

	/**
	 * 统计任务日志中某条目的出现次数。
	 * <p>
	 * 事实来源：任务日志文件 {@code {TAPDATA_WORK_DIR}/logs/jobs/{taskId}.log}——
	 * 引擎任务执行中经 log4j ThreadContext（taskId）路由，按行追加到该文件。
	 */
	protected long countTaskLogOccurrences(String taskId, String message) throws Exception {
		long deadline = System.currentTimeMillis() + 60_000;
		long count = 0;
		while (System.currentTimeMillis() < deadline) {
			count = readTaskLogs(taskId).stream().filter(msg -> msg.contains(message)).count();
			if (count > 0) {
				break;
			}
			Thread.sleep(1000);
		}
		return count;
	}

	/** 读取任务日志全部内容（日志文件按行返回；文件尚未生成时返回空） */
	protected List<String> readTaskLogs(String taskId) throws IOException {
		Path logFile = Paths.get(workDir(), "logs", "jobs", taskId + ".log");
		if (!Files.isRegularFile(logFile)) {
			return new ArrayList<>();
		}
		return Files.readAllLines(logFile, StandardCharsets.UTF_8);
	}

	/** 引擎工作目录（与 failsafe environmentVariables 注入的 TAPDATA_WORK_DIR 一致） */
	private static String workDir() {
		String workDir = System.getenv("TAPDATA_WORK_DIR");
		return workDir != null && !workDir.isEmpty()
				? workDir
				: System.getProperty("user.dir") + "/target/engine-it-work";
	}

	/** 轮询等待任务日志中出现包含指定子串的条目，返回该条日志全文（超时抛异常） */
	protected String awaitLogEntry(String taskId, String contains, long timeoutMillis) throws Exception {
		long deadline = System.currentTimeMillis() + timeoutMillis;
		while (System.currentTimeMillis() < deadline) {
			for (String msg : readTaskLogs(taskId)) {
				if (msg.contains(contains)) {
					return msg;
				}
			}
			Thread.sleep(1000);
		}
		throw new AssertionError("Timed out waiting log entry containing '" + contains + "' in task " + taskId);
	}

	// ===================== connector jar 目录定位 =====================

	/**
	 * 定位本地 maven 仓库中的 connector jar 目录（MockTM pdk/jar/v2 的下载源）：
	 * ~/.m2/repository/io/tapdata，包含 {mysql|mongodb}-connector/1.0-SNAPSHOT/...jar。
	 * <p>
	 * 注意：{@code user.home} 不能从环境变量 HOME 推导——failsafe 环境只有 5 个变量，
	 * 若 HOME 在其中（值为空），getProperty("user.home", ...) 对"存在但为空"的键返回空串，
	 * 导致 user.dir/../../.. 解析到工作区根目录。因此只认 {@code M2_HOME}，其余一律走
	 * System.getProperty("user.home")（JVM 启动时从 OS 用户目录初始化，不受环境变量影响）。
	 */
	private static String resolveConnectorJarDir() {
		String m2Home = System.getenv("M2_HOME");
		String m2Repo;
		if (m2Home != null && !m2Home.isEmpty()) {
			m2Repo = m2Home + "/repository";
		} else {
			m2Repo = System.getProperty("user.home") + "/.m2/repository";
		}
		Path jarDir = Paths.get(m2Repo, "io", "tapdata");
		if (containsJarRecursively(jarDir)) {
			return jarDir.toString();
		}
		// 兜底：user.dir 为 iengine-app 模块目录（failsafe workingDirectory）；
		// 注意 user.dir 不能从环境变量 HOME 推导（见方法注释）。
		Path fallback = Paths.get(System.getProperty("user.dir"), "..", "..", "..", "tapdata-it",
				"tapdata-connector-it", "target", "connectors");
		if (containsJarRecursively(fallback)) {
			return fallback.normalize().toString();
		}
		throw new IllegalStateException("No connector jar dir found: tried " + jarDir + " and " + fallback.normalize());
	}

	/** 目录（含子目录，如 maven 仓库 {artifactId}/{version}/）下是否存在 .jar 文件 */
	private static boolean containsJarRecursively(Path dir) {
		if (!Files.isDirectory(dir)) {
			return false;
		}
		try (Stream<Path> stream = Files.walk(dir)) {
			return stream.anyMatch(p -> Files.isRegularFile(p) && p.getFileName().toString().endsWith(".jar"));
		} catch (IOException e) {
			return false;
		}
	}

	// ===================== 通用集成用例（任意 connector 组合通用，由具体组合子类执行） =====================
	// 分组：引擎冒烟 / 任务全流转 / D1 生命周期 / D2 全量读 / D3 增量 / D4 目标写 / 断点续跑。
	// 用例内全部源/目标操作经旁路验证器（直连或任务节点反射），不触碰特定数据源。

	// ---------- 引擎冒烟 ----------

	/** 冒烟：引擎 + MockTM 最小闭环（调度器就绪 / singleton-lock / 心跳上报），不依赖真实数据源 */
	@Test
	@DisplayName("引擎启动：调度器就绪、单例锁占用、心跳上报 MockTM")
	void should_engine_start_and_heartbeat() throws Exception {
		EngineRuntime runtime = engine();
		MockTM mockTM = runtime.tm();

		// 1. 引擎启动完成：调度器 bean 就绪、实例号正确
		assertNotNull(runtime.getBean(TapdataTaskScheduler.class), "engine task scheduler should be ready");
		assertEquals("engine-it-agent", runtime.getInstanceNo());

		// 2. singleton-lock 已占用：Workers 集合中本实例记录被更新
		waitUntil("worker singleton-lock acquired", 30_000, () -> {
			for (Map<String, Object> worker : mockTM.find("Workers")) {
				if ("engine-it-agent".equals(String.valueOf(worker.get("process_id")))
						&& worker.get("singletonLock") != null) {
					return true;
				}
			}
			return false;
		});

		// 3. 心跳上报：Workers/health 出现引擎上报的心跳文档；Workers 记录存在
		waitUntil("worker heartbeat reported", 30_000, () -> !mockTM.find("Workers/health").isEmpty());
		List<Map<String, Object>> workers = mockTM.find("Workers");
		assertFalse(workers.isEmpty(), "Workers should be seeded");
		assertTrue(workers.stream().anyMatch(w -> "engine-it-agent".equals(String.valueOf(w.get("process_id")))),
				"seeded worker for engine-it-agent should exist");
	}

	// ---------- 任务全流转冒烟 ----------

	/** 任务全流转：下发 → 认领 → 模型加载 → jar 下载 → 建 DAG → 全量读写 → 旁路断言目标与源一致 */
	@Test
	@DisplayName("任务全流转：下发到全量写完，目标数据与源一致")
	void should_task_full_flow_smoke() throws Exception {
		// 1. 构造任务 + 预置 TM 侧数据（Connections/DatabaseTypes/transformAllParam 模型）
		String table = randomTableName();
		TaskDto taskDto = TaskDtoBuilder.buildMigrateTask(null, "it-flow-smoke",
				SOURCE_CONN_ID, TARGET_CONN_ID, List.of(table));
		TaskFixture.prepare(engine(), taskDto, sourceSpec(), targetSpec(), List.of(table));

		// 2. 源表经直连验证器在下发前准备（快照读必能读到完整数据），然后下发任务
		prepareSourceTable(table, 3);
		String taskId = engine().submitTask(taskDto);
		engine().awaitTaskStatus(taskId, 120, TaskDto.STATUS_RUNNING);

		// 3. 等待全量完成（表进入增量阶段 = 全量写完）
		engine().awaitSyncStage(taskId, "CDC", 180);

		// 4. 旁路断言：目标行数与内容（经目标节点 connector 验证器）
		assertTargetTable(taskId, table, 3);
	}

	// ---------- D1 任务生命周期 ----------

	/** D1.1：任务以 wait_run 下发 → 引擎认领并启动 → running；全量写完进 CDC（真实执行非仅状态置位） */
	@Test
	@DisplayName("D1.1 任务下发启动：wait_run 认领为 running，全量写完进入增量")
	void should_start_task_from_wait_run() throws Exception {
		String table = randomTableName();
		prepareSourceTable(table, 5);
		String taskId = taskService.startMigrateTask("d1-1-start", List.of(table));
		taskService.awaitRunning(taskId, 120);
		assertEquals(TaskDto.STATUS_RUNNING, taskService.getTaskStatus(taskId));

		taskService.awaitSyncStage(taskId, "CDC", 180);
		taskService.assertDataConsistent(taskId, table, "id");
	}

	/** D1.2：重复发启动信号 → 只刷状态，不重复建 DAG（日志只一次全量读） */
	@Test
	@DisplayName("D1.2 幂等启动：重复启动信号不重建 DAG、不重读全量")
	void should_idempotent_start() throws Exception {
		String table = randomTableName();
		prepareSourceTable(table, 3);
		TaskDto taskDto = TaskDtoBuilder.buildMigrateTask(null, "d1-2-idempotent", SOURCE_CONN_ID, TARGET_CONN_ID, List.of(table));
		String taskId = taskService.startTask(taskDto, List.of(table));
		taskService.awaitRunning(taskId, 120);
		taskService.awaitSyncStage(taskId, "CDC", 180);

		// 重复发启动信号（重新以 wait_run 写入 MockTM，引擎会再次认领）
		taskService.startTask(taskDto, List.of(table));
		// 给引擎一个调度周期处理重复信号，观察是否重复执行全量读
		Thread.sleep(20_000);

		assertEquals(TaskDto.STATUS_RUNNING, taskService.getTaskStatus(taskId), "duplicate start must not break running state");
		// 注意：不能匹配裸串 "Starting batch read"（"Starting batch read from table: xxx" 也含该子串），
		// 必须匹配唯一行 "Starting batch read from 1 tables" 才能证明全量读只发生一次
		long batchReadCount = countTaskLogOccurrences(taskId, "Starting batch read from 1 tables");
		assertEquals(1, batchReadCount, "duplicate start must not rebuild DAG / re-read snapshot");
		assertTrue(countTaskLogOccurrences(taskId, "Batch read completed") >= 1, "snapshot should have completed");
	}

	/** D1.3：运行中下发停止（MockTM Task 置 stopping）→ 引擎认领 → 任务落 stopped */
	@Test
	@DisplayName("D1.3 优雅停止：下发停止信号，任务落 stopped")
	void should_graceful_stop() throws Exception {
		String table = randomTableName();
		prepareSourceTable(table, 10);
		String taskId = taskService.startMigrateTask("d1-3-stop", List.of(table));
		taskService.awaitRunning(taskId, 120);
		taskService.awaitSyncStage(taskId, "CDC", 180);

		taskService.stopTask(taskId);
		taskService.awaitStatus(taskId, 120, "stopped");
	}

	/** D1.4：全量任务（无增量阶段）跑完 → 引擎回调 Task/complete → complete（完成后经直连验证器断言） */
	@Test
	@DisplayName("D1.4 自然完成：全量任务跑完落 complete，数据端到端一致")
	void should_natural_complete() throws Exception {
		String table = randomTableName();
		prepareSourceTable(table, 5);
		String taskId = taskService.startFullSyncTask("d1-4-complete", List.of(table));
		taskService.awaitStatus(taskId, 300, "complete");
		// 自然完成后引擎 doClose 已销毁任务节点连接器，改用子类动态提供的直连旁路验证器断言
		taskService.assertDataConsistentDirect(directSourceVerifier(), directTargetVerifier(), table, "id");
	}

	/** D1.5：启动阶段失败（模型加载返回空）→ 不可重试 → runError */
	@Test
	@DisplayName("D1.5 启动失败：模型加载返回空，任务落 runError")
	void should_error_then_retry() throws Exception {
		// 注入方式：transformAllParam 返回 200 + 空 data → engineTransformSchema 拿到 null → NPE
		// → TASK_FAILED_TO_LOAD_TABLE_STRUCTURE → startTask 同步失败 → runError（详见 MockTM.failTransformAllParam）。
		// 任务在模型加载阶段即失败（连接器未创建），无需准备源表数据（表名仅供任务配置）
		String table = randomTableName();
		String taskId = taskService.startMigrateTask("d1-5-error", List.of(table));
		engine().tm().failTransformAllParam(taskId);
		taskService.awaitStatus(taskId, 120, "runError");
	}

	// ---------- D2 全量读取路径 ----------

	/** D2.1：多表普通全量逐表串行——任务跑完 complete，每张表只读一次（已完成的表不重读） */
	@Test
	@DisplayName("D2.1 多表全量逐表串行：每表只读一次不重读")
	void should_normal_snapshot_serial() throws Exception {
		String t1 = randomTableName();
		String t2 = randomTableName();
		prepareSourceTable(t1, 5);
		prepareSourceTable(t2, 7);
		String taskId = taskService.startFullSyncTask("d2-1-serial", List.of(t1, t2));
		taskService.awaitStatus(taskId, 300, "complete");
		// 自然完成后引擎 doClose 已销毁任务节点连接器，改用子类动态提供的直连旁路验证器断言
		taskService.assertDataConsistentDirect(directSourceVerifier(), directTargetVerifier(), t1, "id");
		taskService.assertDataConsistentDirect(directSourceVerifier(), directTargetVerifier(), t2, "id");

		assertEquals(1, countTaskLogOccurrences(taskId, "Starting batch read from table: " + t1),
				"table " + t1 + " must be read exactly once");
		assertEquals(1, countTaskLogOccurrences(taskId, "Starting batch read from table: " + t2),
				"table " + t2 + " must be read exactly once");
	}

	/** D2.9：全部表读完后发出全量完成事件（Batch read completed），任务由快照切换到 CDC */
	@Test
	@DisplayName("D2.9 全量完成事件：全部表读完，任务切换到增量阶段")
	void should_snapshot_complete_event() throws Exception {
		String table = randomTableName();
		prepareSourceTable(table, 5);
		String taskId = taskService.startMigrateTask("d2-9-complete-event", List.of(table));
		taskService.awaitRunning(taskId, 120);
		taskService.awaitSyncStage(taskId, "CDC", 180);

		assertTrue(countTaskLogOccurrences(taskId, "Batch read completed") >= 1,
				"snapshot complete event must fire after all tables read");
		taskService.assertDataConsistent(taskId, table, "id");
	}

	// ---------- D3 增量读取与断点 ----------

	/** D3.1：CDC 阶段源端插入 → 增量 insert 事件流转到目标端 */
	@Test
	@DisplayName("D3.1 增量插入：源端新插行流转到目标端")
	void should_incremental_insert() throws Exception {
		String table = randomTableName();
		prepareSourceTable(table, 5);
		String taskId = taskService.startMigrateTask("d3-1-insert", List.of(table));
		taskService.awaitRunning(taskId, 120);
		taskService.awaitSyncStage(taskId, "CDC", 180);
		taskService.assertDataConsistent(taskId, table, "id");

		// CDC 阶段源端插入 3 行 → 目标端轮询追平（增量 insert 事件流转）
		insertSourceRows(taskId, table, 6, 3);
		taskService.assertDataConsistent(taskId, table, "id");
	}

	/** D3.2：CDC 阶段源端更新 → 增量 update 事件流转到目标端 */
	@Test
	@DisplayName("D3.2 增量更新：源端行更新同步到目标端")
	void should_incremental_update() throws Exception {
		String table = randomTableName();
		prepareSourceTable(table, 5);
		String taskId = taskService.startMigrateTask("d3-2-update", List.of(table));
		taskService.awaitRunning(taskId, 120);
		taskService.awaitSyncStage(taskId, "CDC", 180);
		taskService.assertDataConsistent(taskId, table, "id");

		// CDC 阶段源端更新一行 → 目标端对应行值更新（增量 update 事件流转）
		updateSourceRow(taskId, table, 3, "updated-row-3");
		awaitTargetValue(taskId, table, 3, "name", "updated-row-3");
	}

	/** D3.3：CDC 阶段源端删除 → 增量 delete 事件流转到目标端（行消失） */
	@Test
	@DisplayName("D3.3 增量删除：源端删除行在目标端消失")
	void should_incremental_delete() throws Exception {
		String table = randomTableName();
		prepareSourceTable(table, 5);
		String taskId = taskService.startMigrateTask("d3-3-delete", List.of(table));
		taskService.awaitRunning(taskId, 120);
		taskService.awaitSyncStage(taskId, "CDC", 180);
		taskService.assertDataConsistent(taskId, table, "id");

		// CDC 阶段源端删除一行 → 目标端行消失（增量 delete 事件流转）
		deleteSourceRow(taskId, table, 2);
		awaitTargetRowCount(taskId, table, 4);
	}

	/** D3.4：目标端写入后 syncProgress offset 推进（连续两批写入全部到达后进度变化） */
	@Test
	@DisplayName("D3.4 位点推进：增量写入到达目标端后 offset 推进")
	void should_offset_advance_after_write() throws Exception {
		String table = randomTableName();
		prepareSourceTable(table, 3);
		String taskId = taskService.startMigrateTask("d3-4-offset", List.of(table));
		taskService.awaitRunning(taskId, 120);
		taskService.awaitSyncStage(taskId, "CDC", 180);
		taskService.assertDataConsistent(taskId, table, "id");

		String progressBefore = readSyncProgress(taskId);
		assertNotNull(progressBefore, "syncProgress must be reported after CDC started");
		assertTrue(progressBefore.toLowerCase().contains("offset"),
				"syncProgress must contain stream offset: " + progressBefore);

		// 连续写两批，每批都到达目标端 → offset 必然推进（否则后续批次读不到）
		insertSourceRows(taskId, table, 4, 3);
		taskService.assertDataConsistent(taskId, table, "id");
		insertSourceRows(taskId, table, 7, 3);
		taskService.assertDataConsistent(taskId, table, "id");

		// 等待引擎将推进后的进度上报到 MockTM（flush 周期）
		awaitProgressAdvance(taskId, progressBefore);
	}

	// ---------- D4 目标端写入 ----------

	/** D4.12：端到端数据一致性——50 行全量 + 20 行增量两条路径写入目标端，逐行逐列一致 */
	@Test
	@DisplayName("D4.12 端到端一致：全量+增量双路径写入，逐行逐列一致")
	void should_data_consistent_end_to_end() throws Exception {
		String table = randomTableName();
		prepareSourceTable(table, 50);
		String taskId = taskService.startMigrateTask("d4-1-consistent", List.of(table));
		taskService.awaitRunning(taskId, 120);
		taskService.awaitSyncStage(taskId, "CDC", 180);
		taskService.assertDataConsistent(taskId, table, "id");

		insertSourceRows(taskId, table, 51, 20);
		taskService.assertDataConsistent(taskId, table, "id");
	}

	// ---------- 断点续跑 ----------

	/** 断点续跑：任务停止 → 重启同一任务 → 从持久化 offset 续跑，停机后新增事件不丢不重（updateOrInsert 幂等） */
	@Test
	@DisplayName("断点续跑：停止后重启，从 offset 续跑不丢不重")
	void should_restart_replay_no_loss() throws Exception {
		// 第一阶段：migrate 任务全量写完进入 CDC，基线一致
		String table = randomTableName();
		prepareSourceTable(table, 5);
		TaskDto taskDto = TaskDtoBuilder.buildMigrateTask(null, "bp-1-restart", SOURCE_CONN_ID, TARGET_CONN_ID, List.of(table));
		String taskId = taskService.startTask(taskDto, List.of(table));
		taskService.awaitRunning(taskId, 120);
		taskService.awaitSyncStage(taskId, "CDC", 180);
		taskService.assertDataConsistent(taskId, table, "id");

		// 第二阶段：停止任务（模拟故障/维护窗口）。
		// 注：停机期间无法经任务节点验证器写源端——引擎 doClose 会经
		// PDKIntegration.releaseAssociateId 销毁 connector（连接池随之关闭）。
		// 改为重启后立即写入：事件仍产生于停机时持久化的 offset 之后，
		// 同样验证"续跑从 offset 拾取未消费事件、不丢不重"的断点语义。
		taskService.stopTask(taskId);
		taskService.awaitStatus(taskId, 120, "stopped");

		// 第三阶段：重启同一任务（引擎重新认领 wait_run，从持久化 offset 续跑），
		// 新连接器就绪后写入增量行（位于停机时 offset 之后，必须经续跑拾取）
		taskService.startTask(taskDto, List.of(table));
		insertSourceRows(taskId, table, 6, 5);
		taskService.awaitRunning(taskId, 120);
		taskService.awaitSyncStage(taskId, "CDC", 180);
		taskService.assertDataConsistent(taskId, table, "id");
	}

	// ===================== 通用用例私有辅助 =====================

	/** 轮询等待条件成立（超时抛 AssertionError） */
	private static void waitUntil(String description, long timeoutMillis, BooleanSupplier condition)
			throws InterruptedException {
		long deadline = System.currentTimeMillis() + timeoutMillis;
		while (System.currentTimeMillis() < deadline) {
			if (condition.getAsBoolean()) {
				return;
			}
			Thread.sleep(500);
		}
		throw new AssertionError("Timed out waiting for: " + description);
	}

	/** 经任务目标节点 connector 的验证器断言目标表行数与内容（不直连特定数据库） */
	private void assertTargetTable(String taskId, String table, int expectedRows) throws Exception {
		ConnectorVerifier verifier = targetVerifier(taskId);
		// 等待目标行数达标（引擎写数据有延迟，轮询兜底）
		long deadline = System.currentTimeMillis() + 60_000;
		long count = -1;
		while (System.currentTimeMillis() < deadline) {
			count = verifier.count(table);
			if (count == expectedRows) {
				break;
			}
			Thread.sleep(1000);
		}
		assertEquals(expectedRows, count, "target table " + table + " row count");

		List<Map<String, Object>> rows = verifier.selectAll(table);
		assertEquals(expectedRows, rows.size(), "target rows");
		for (Map<String, Object> row : rows) {
			// 字段存在性按子类提供的表字段规格断言（不硬编码字段名）
			for (TestFieldSpec field : testTableFields()) {
				assertNotNull(row.get(field.getName()), "row should have field " + field.getName() + ": " + row);
			}
		}
	}

	/** 轮询目标端某行某列的值变为期望值（经目标节点 connector 验证器） */
	private void awaitTargetValue(String taskId, String table, int id, String column, String expected) throws Exception {
		ConnectorVerifier verifier = targetVerifier(taskId);
		long deadline = System.currentTimeMillis() + 120_000;
		while (System.currentTimeMillis() < deadline) {
			for (Map<String, Object> row : verifier.selectAll(table)) {
				if (row.get(primaryKey()) != null && ((Number) row.get(primaryKey())).intValue() == id
						&& expected.equals(String.valueOf(row.get(column)))) {
					return;
				}
			}
			sleepQuietly(1000L);
		}
		fail("target value not updated: " + table + " id=" + id + " " + column + " expected=" + expected
				+ " actual=" + verifier.selectAll(table));
	}

	/** 轮询目标端行数变为期望值（经目标节点 connector 验证器） */
	private void awaitTargetRowCount(String taskId, String table, long expected) throws Exception {
		ConnectorVerifier verifier = targetVerifier(taskId);
		long deadline = System.currentTimeMillis() + 120_000;
		while (System.currentTimeMillis() < deadline) {
			if (verifier.count(table) == expected) {
				return;
			}
			sleepQuietly(1000L);
		}
		fail("target row count not reached: " + table + " expected=" + expected + " actual=" + verifier.count(table));
	}

	/** 读取引擎上报到 Task 文档的 syncProgress 原始 JSON（nodePair → SyncProgress JSON 字符串） */
	private String readSyncProgress(String taskId) {
		Map<String, Object> task = taskService.getTask(taskId);
		Object syncProgress = task != null ? task.get("syncProgress") : null;
		if (syncProgress instanceof Map) {
			for (Object value : ((Map<?, ?>) syncProgress).values()) {
				if (value != null && String.valueOf(value).length() > 10) {
					return String.valueOf(value);
				}
			}
		}
		return null;
	}

	/** 轮询等待 syncProgress 内容变化（offset 推进后的上报） */
	private void awaitProgressAdvance(String taskId, String before) {
		long deadline = System.currentTimeMillis() + 120_000;
		while (System.currentTimeMillis() < deadline) {
			String now = readSyncProgress(taskId);
			if (now != null && !now.equals(before)) {
				return;
			}
			sleepQuietly(1000L);
		}
		fail("syncProgress did not advance after target write: " + taskId);
	}

	private static void sleepQuietly(long millis) {
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}
}
