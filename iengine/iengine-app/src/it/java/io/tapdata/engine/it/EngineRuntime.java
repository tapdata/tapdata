package io.tapdata.engine.it;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.core.Hazelcast;
import com.sun.net.httpserver.HttpServer;
import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.Application;
import io.tapdata.common.MonitorConfigListener;
import io.tapdata.engine.it.mock.MockTM;
import io.tapdata.flow.engine.V2.schedule.TapdataTaskScheduler;
import io.tapdata.pdk.core.runtime.TapRuntime;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 引擎集成测试公共组件：模拟「引擎 + 管理端（TM）」的最小闭环，供各 IT 用例快速启动任务并断言各个环节状态。
 * <p>
 * 真实部署形态（DAAS）：引擎的 TM 交互（登录、下载 connector、获取任务配置、加载模型、上报心跳、
 * 更新任务状态/指标）全部经 REST 访问管理端；任务状态存储（PdkStateMap）落在外部存储（生产为
 * Tapdata MongoDB External Storage）。本组件用 {@link MockTM} 在进程内 mock TM 侧交互：
 * <ol>
 *   <li>启动 MockTM（固定端口，默认 18080，可用 -Dengine.it.tm.port 覆盖）并预置基础数据
 *       （Settings/User/Workers——引擎启动必须项）</li>
 *   <li>以 {@code app_type=DAAS + isCloud=false} 启动 Spring 上下文：集合操作仍经
 *       HttpClientMongoOperator 走 MockTM 的 REST 代理（与生产一致）</li>
 *   <li>下发任务：{@link #submitTask(TaskDto)} 将任务以 {@code wait_run} 写入 MockTM 的 Task 集合，
 *       引擎 TapdataTaskScheduler 轮询认领并启动（模拟 TM 调度下发）</li>
 *   <li>断言：{@link #getTaskStatus}/{@link #awaitTaskStatus}/{@link #getTaskSyncProgress} 从 MockTM
 *       读取引擎上报的任务状态/进度/指标</li>
 * </ol>
 * <p>
 * 使用方式：
 * <pre>
 * try (EngineRuntime runtime = EngineRuntime.start()) {
 *     runtime.submitTask(taskDto);
 *     runtime.awaitTaskStatus(taskId, TaskDto.STATUS_RUNNING, 60);
 *     ...
 * }
 * </pre>
 * 注意：环境变量（app_type/backend_url/TAPDATA_WORK_DIR 等）由 maven-failsafe-plugin 的
 * environmentVariables 提供（{@code System.getenv} 在 JVM 内只读）；本组件内部只设置 system property。
 */
public class EngineRuntime implements AutoCloseable {

	private static final Logger logger = LoggerFactory.getLogger(EngineRuntime.class);

	public static final String DEFAULT_TM_PORT_PROP = "engine.it.tm.port";
	public static final int DEFAULT_TM_PORT = 18080;
	public static final String DEFAULT_INSTANCE_NO = "engine-it-agent";
	public static final String DEFAULT_ACCESS_CODE = "it-access-code";
	public static final String DEFAULT_USER_ID = "it-user-id";
	public static final String TASK_COLLECTION = "Task";

	private final ObjectMapper objectMapper = new ObjectMapper();
	private final MockTM mockTM;
	private final ConfigurableApplicationContext context;
	private final String instanceNo;
	private final String baseUrl;
	private final boolean startedByThis;

	private EngineRuntime(MockTM mockTM, ConfigurableApplicationContext context, String instanceNo, boolean startedByThis) {
		this.mockTM = mockTM;
		this.context = context;
		this.instanceNo = instanceNo;
		this.baseUrl = mockTM.getBaseUrl();
		this.startedByThis = startedByThis;
	}

	/** 启动一个完整引擎实例（MockTM + Spring 上下文） */
	public static EngineRuntime start() throws Exception {
		return start(new EngineRuntimeConfig());
	}

	public static EngineRuntime start(EngineRuntimeConfig config) throws Exception {
		checkEnv();
		int port = config.port > 0 ? config.port : Integer.parseInt(System.getProperty(DEFAULT_TM_PORT_PROP, String.valueOf(DEFAULT_TM_PORT)));
		String instanceNo = StringUtils.isNotBlank(config.instanceNo) ? config.instanceNo : DEFAULT_INSTANCE_NO;

		// 1. 启动 MockTM 并预置引擎启动必需数据（含 connector jar 下载源：
		//    引擎不做本地预置，运行任务时按任务配置经 pdk/jar/v2 从 MockTM 下载）
		MockTM mockTM = new MockTM(port);
		mockTM.setAccessCode(DEFAULT_ACCESS_CODE);
		mockTM.setConnectorJarDir(config.connectorJarDir);
		seedBaseData(mockTM, instanceNo);
		mockTM.start();
		logger.info("Engine IT: MockTM started at {}", mockTM.getBaseUrl());

		// 2. 公共 system property：不设置 pdk_external_jar_path（无本地预置目录），
		//    connector 由引擎按任务配置（DatabaseType 的 pdkHash/jarFile/jarRid）经
		//    PdkUtil.downloadPdkFileIfNeed 从 MockTM 下载到 {user.dir}/dist 后加载
		cleanDownloadedConnectorJars();
		System.setProperty("pdk_load_new_jar_at_runtime", "true");
		// 必须为 true：ExternalJarManager 的 firstTime 标记只对第一次 loadJars（第一个 refreshJars 的 jar）
		// 生效，后续 jar 的 refreshJars 依赖 loadNewJarAtRuntime=true 才会注册 TapConnector；
		// 置 false 时源/目标两个 connector 只有先下载的那个能加载，另一个报 Source not found
		System.setProperty("pdk_update_jar_when_idle_at_runtime", "false");
		System.setProperty("tap_verbose", "true");

		// 3. 启动引擎 Spring 上下文（与 Application.main 相同配置，但不 System.exit）
		ConfigurableApplicationContext context = new SpringApplicationBuilder(Application.class)
				.allowCircularReferences(true)
				.listeners(new MonitorConfigListener())
				.web(WebApplicationType.NONE)
				.run();
		TapRuntime.getInstance();
		BeanUtil.configurableApplicationContext = context;
		ConfigurationCenter configurationCenter = context.getBean(ConfigurationCenter.class);
		configurationCenter.putConfig("version", "-");
		configurationCenter.putConfig("gitCommitId", "-");

		// 4. 引擎就绪后，显式启动 HTTP 轮询调度（模拟真实部署中 WS 断连后的 fallback），
		//    使 wait_run 任务可被引擎轮询认领
		TapdataTaskScheduler taskScheduler = context.getBean(TapdataTaskScheduler.class);
		taskScheduler.startScheduleTask(TapdataTaskScheduler.SCHEDULE_START_TASK_NAME);
		taskScheduler.startScheduleTask(TapdataTaskScheduler.SCHEDULE_STOP_TASK_NAME);
		logger.info("Engine IT: engine started, instanceNo={}, backend={}", instanceNo, mockTM.getBaseUrl());

		return new EngineRuntime(mockTM, context, instanceNo, true);
	}

	/** 清理上次运行下载残留的 connector jar（PdkUtil 固定落盘 {user.dir}/dist/{fileName}__{jarRid}__.jar），
	 *  保证每次运行都真实走 pdk/jar/v2 下载链路（否则文件存在时引擎直接复用不下载） */
	private static void cleanDownloadedConnectorJars() {
		File dist = new File(System.getProperty("user.dir") + File.separator + "dist");
		File[] jars = dist.listFiles((dir, name) -> name.endsWith(".jar"));
		if (jars == null) {
			return;
		}
		for (File jar : jars) {
			if (!jar.delete()) {
				logger.warn("Engine IT: clean downloaded connector jar failed: {}", jar);
			}
		}
	}

	/**
	 * 校验运行环境（环境变量由 failsafe environmentVariables 提供）：app_type/isCloud/backend_url 与 JDK 版本。
	 * <p>
	 * JDK 必须为 17（与引擎生产镜像 eclipse-temurin:17-jdk 一致）：JDK 18+ 上 chronicle-core 2.21.91 的
	 * {@code OS.<clinit>} 取不到 {@code sun.nio.ch.FileChannelImpl.unmap0(long,long)} 会直接抛
	 * AssertionError（不看 -ea），任务日志缓存 TaskLogger/Chronicle Queue 初始化失败
	 * → 所有任务启动即 runError（失败表现与用例/引擎逻辑无关，故在此提前拦住并给出原因）。
	 */
	private static void checkEnv() {
		int jdkVersion = Runtime.version().feature();
		if (jdkVersion != 17) {
			throw new IllegalStateException("Engine IT must run on JDK 17 (same as the production engine image), current: "
					+ jdkVersion + " —— set JAVA_HOME to a JDK 17 and re-run; on JDK 18+ chronicle-core fails to initialize"
					+ " (missing sun.nio.ch.FileChannelImpl.unmap0(long,long)) and every task turns runError immediately");
		}
		// JVM 断言必须关闭（与生产引擎 JVM、failsafe enableAssertions=false 一致）：
		// IDEA 的 JUnit 运行配置默认追加 -ea，会激活 mysql connector（debezium fork）
		// ChangeEventQueue.disableBuffering 的 assert，增量阶段抛 AssertionError: Buffer must be flushed
		// → CDC 事件永不进目标端，表现为 should_incremental_update / should_offset_advance_after_write
		// 在 120s 轮询后超时失败（去掉 -ea 才是真实的引擎行为），故在此快速失败并给出原因
		if (EngineRuntime.class.desiredAssertionStatus()) {
			throw new IllegalStateException("JVM assertions (-ea) must be disabled for engine ITs, current JVM enables them"
					+ " —— running from IntelliJ? Uncheck 'Enable assertions' (or remove -ea from VM options) in the"
					+ " Run Configuration / JUnit run config template, then re-run. With -ea the mysql connector's"
					+ " ChangeEventQueue.disableBuffering assert trips ('Buffer must be flushed') and every CDC task"
					+ " fails in source_stream_read, which is not the production behaviour (production runs without -ea)");
		}
		String appType = System.getenv("app_type");
		if (!"DAAS".equalsIgnoreCase(appType)) {
			throw new IllegalStateException("app_type env must be DAAS (set via failsafe environmentVariables), current: " + appType);
		}
		if (!"false".equals(System.getenv("isCloud"))) {
			throw new IllegalStateException("isCloud env must be false (set via failsafe environmentVariables), current: " + System.getenv("isCloud"));
		}
		String backendUrl = System.getenv("backend_url");
		if (StringUtils.isBlank(backendUrl)) {
			throw new IllegalStateException("backend_url env is blank (set via failsafe environmentVariables)");
		}
		String mongoUri = System.getenv("TAPDATA_MONGO_URI");
		if (StringUtils.isBlank(mongoUri)) {
			throw new IllegalStateException("TAPDATA_MONGO_URI env is blank (set via failsafe environmentVariables):"
					+ " DAAS 形态下任务状态存储（PdkStateMap）走 external storage 而非 TM HTTP 代理，需提供 MongoDB URI");
		}
	}

	/** 预置引擎启动必需数据：Settings（buildProfile 等）/User（登录）/Workers（注册 worker 信息） */
	private static void seedBaseData(MockTM mockTM, String instanceNo) {
		// Settings：loadSettings 查 Settings?decode=1，buildProfile 缺失会导致 NPE
		List<Map<String, Object>> settings = new ArrayList<>();
		settings.add(setting("buildProfile", "DAAS", "DAAS"));
		settings.add(setting("threshold", "1", "1"));
		settings.add(setting("jobHeartTimeout", "60000", "60000"));
		settings.add(setting("file.defaultCharset", "UTF-8", "UTF-8"));
		settings.add(setting("logLevel", "INFO", "INFO"));
		mockTM.put("Settings", settings);

		// User：login 成功后引擎按 users/{userId} 查 User（ConnectorManager.login 拼接 "users/"）
		Map<String, Object> user = new LinkedHashMap<>();
		user.put("_id", DEFAULT_USER_ID);
		user.put("id", DEFAULT_USER_ID);
		user.put("role", 1);
		user.put("accesscode", DEFAULT_ACCESS_CODE);
		user.put("email", "engine-it@tapdata.io");
		user.put("name", "engine-it");
		user.put("isDeleted", false);
		mockTM.put("users", Collections.singletonList(user));

		// Workers：ConnectorManager.init 查 Workers 并回写平台信息；DRS 模式下空集合会抛异常退出
		Map<String, Object> worker = new LinkedHashMap<>();
		worker.put("_id", "worker-" + instanceNo);
		worker.put("id", "worker-" + instanceNo);
		worker.put("process_id", instanceNo);
		worker.put("worker_type", "connector");
		worker.put("running_thread", 0);
		worker.put("ping_time", System.currentTimeMillis());
		worker.put("isDeleted", false);
		worker.put("stopping", false);
		mockTM.put("Workers", Collections.singletonList(worker));

		// ExternalStorage：DAAS 形态的任务状态存储（PdkStateMap.initConstructMap 非 cloud 分支），
		// 按生产 DAAS 的 "Tapdata MongoDB External Storage" 配 type=mongodb，uri 取 failsafe 注入的 TAPDATA_MONGO_URI。
		// 不能配 type=httptm：HttpTMIMap 未实现 PersistenceStorageStore.isEmpty()（基类直接抛
		// UnsupportedOperationException），而 DAAS 分支 initNodeStateMap 必调 isEmpty() 探测 V1/V2，
		// 节点 init 即失败→任务 runError（mongodb/rocksdb 存储已实现 isEmpty）。
		// 注意：_id/id 必须是 24 位 hex（ExternalStorageDto.id 为 ObjectId，
		// 引擎反序列化用 ObjectIdDeserialize 校验 ^[0-9a-fA-F]{24}$，非 hex 会反序列化为 null
		// 导致 ExternalStorageUtil.getExternalStorageMap 的 getId().toHexString() NPE）
		String externalStorageId = "507f1f77bcf86cd799439011";
		Map<String, Object> externalStorage = new LinkedHashMap<>();
		externalStorage.put("_id", externalStorageId);
		externalStorage.put("id", externalStorageId);
		externalStorage.put("name", "Tapdata MongoDB External Storage");
		externalStorage.put("type", "mongodb");
		externalStorage.put("defaultStorage", true);
		externalStorage.put("uri", System.getenv("TAPDATA_MONGO_URI"));
		externalStorage.put("canEdit", false);
		externalStorage.put("canDelete", false);
		mockTM.put("ExternalStorage", Collections.singletonList(externalStorage));
	}

	private static Map<String, Object> setting(String key, String value, String defaultValue) {
		Map<String, Object> map = new LinkedHashMap<>();
		map.put("_id", "setting-" + key);
		map.put("key", key);
		map.put("value", value);
		map.put("default_value", defaultValue);
		return map;
	}

	// ==================== 引擎访问 ====================

	public MockTM tm() {
		return mockTM;
	}

	public String getBaseUrl() {
		return baseUrl;
	}

	public String getInstanceNo() {
		return instanceNo;
	}

	public ConfigurableApplicationContext getContext() {
		return context;
	}

	public <T> T getBean(Class<T> clazz) {
		return context.getBean(clazz);
	}

	// ==================== 任务下发与状态 ====================

	/**
	 * 模拟 TM 下发任务：以 wait_run 写入 MockTM Task 集合，引擎 TapdataTaskScheduler
	 * 轮询认领（findAndModify 置 running）后自动启动。
	 *
	 * @return taskId（hex 字符串）
	 */
	public String submitTask(TaskDto taskDto) {
		Map<String, Object> doc = toMap(taskDto);
		ObjectId id = taskDto.getId();
		String taskId = id != null ? id.toHexString() : String.valueOf(doc.get("_id"));
		doc.put("_id", taskId);
		doc.put("status", TaskDto.STATUS_WAIT_RUN);
		doc.put("agentId", instanceNo);
		doc.put("pingTime", System.currentTimeMillis());
		// 按 _id upsert（不能用 put：put 会清空 Task 集合，使其它仍在运行的任务文档消失，
		// 引擎 ping 更新匹配不到 → modifiedCount=0 → DAAS 形态立即自停那个健康任务）
		// markTaskDispatched 必须在 upsert 前：重启同一任务时，上一轮停机流程的第二条 stopped 回调
		// 可能晚于本次下发落地，不标记会被它把新下发的 wait_run 覆盖回 stopped（任务再也不被认领）
		mockTM.markTaskDispatched(taskId);
		mockTM.upsert(TASK_COLLECTION, doc);
		logger.info("Engine IT: task {}[{}] submitted as wait_run", taskDto.getName(), taskId);
		return taskId;
	}

	/** 直接调用引擎 TaskService 启动任务（绕过 TM 调度下发，适用于调试/单任务断言） */
	public void startTaskDirect(TaskDto taskDto) {
		io.tapdata.flow.engine.V2.task.TaskService<TaskDto> taskService = getBean(io.tapdata.flow.engine.V2.task.TaskService.class);
		taskService.startTask(taskDto);
	}

	/** 读取任务当前状态（status 字段，null 表示任务不存在） */
	public String getTaskStatus(String taskId) {
		Map<String, Object> task = mockTM.findById(TASK_COLLECTION, taskId);
		return task != null ? String.valueOf(task.get("status")) : null;
	}

	/** 读取任务完整文档（引擎上报到 MockTM 的最新状态） */
	public Map<String, Object> getTask(String taskId) {
		return mockTM.findById(TASK_COLLECTION, taskId);
	}

	/** 等待任务进入指定状态集合之一，超时抛 IllegalStateException */
	public Map<String, Object> awaitTaskStatus(String taskId, long timeoutSeconds, String... expectedStatuses) {
		List<String> expected = Arrays.asList(expectedStatuses);
		long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(timeoutSeconds);
		Map<String, Object> lastTask = null;
		while (System.currentTimeMillis() < deadline) {
			Map<String, Object> task = mockTM.findById(TASK_COLLECTION, taskId);
			if (task != null) {
				lastTask = task;
				String status = String.valueOf(task.get("status"));
				if (expected.contains(status)) {
					logger.info("Engine IT: task {} reached status {}", taskId, status);
					return task;
				}
			}
			sleepQuietly(1000L);
		}
		throw new IllegalStateException("Task " + taskId + " did not reach status " + expected
				+ " within " + timeoutSeconds + "s, last status: " + (lastTask != null ? lastTask.get("status") : "<not found>"));
	}

	/** 等待任务达到指定同步阶段（syncProgress 中任意节点对的 syncStage），供断言全量/增量进度 */
	public void awaitSyncStage(String taskId, String syncStage, long timeoutSeconds) {
		long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(timeoutSeconds);
		while (System.currentTimeMillis() < deadline) {
			Map<String, Object> task = mockTM.findById(TASK_COLLECTION, taskId);
			Object syncProgress = task != null ? task.get("syncProgress") : null;
			if (syncProgress instanceof Map) {
				// 引擎上报 key = 节点列表 toString（如 "[source-db-node, target-db-node]"），
				// value = SyncProgress 的 JSON 字符串（含 syncStage 字段）
				for (Object value : ((Map<?, ?>) syncProgress).values()) {
					if (String.valueOf(value).contains("\"syncStage\":\"" + syncStage + "\"")) {
						logger.info("Engine IT: task {} reached syncStage {}", taskId, syncStage);
						return;
					}
				}
			}
			sleepQuietly(1000L);
		}
		throw new IllegalStateException("Task " + taskId + " did not reach syncStage "
				+ syncStage + " within " + timeoutSeconds + "s");
	}

	/** 模拟 TM 下发停止指令：任务置 stopping，引擎 forceStoppingTask 轮询认领后停止。
	 *  注意必须就地更新 MockTM 内部集合（findById 返回深拷贝副本，改副本不生效），
	 *  否则引擎 findStopTask 查询永远看不到 stopping 状态 */
	public void requestStopTask(String taskId) {
		mockTM.updateField(TASK_COLLECTION, taskId, "status", TaskDto.STATUS_STOPPING);
		logger.info("Engine IT: task {} stop requested", taskId);
	}

	/** 直接调用引擎调度器停止任务 */
	public void stopTaskDirect(String taskId) {
		getBean(TapdataTaskScheduler.class).sendStopTask(taskId);
	}

	// ==================== 生命周期 ====================

	@Override
	public void close() {
		if (startedByThis && context != null) {
			try {
				context.close();
			} catch (Exception e) {
				logger.warn("Engine IT: close spring context failed: {}", e.getMessage());
			}
		}
		// Hazelcast 实例由 HazelcastTaskService @PostConstruct 创建，但其 @PreDestroy 只关闭
		// 缓存失效服务不关闭实例——context.close() 后实例仍留在静态注册表并占用 5701 端口。
		// 同一 JVM 内多次启停引擎（如冒烟类 try-with-resources 连跑）时，后续引擎
		// Hazelcast.newHazelcastInstance(同名 agentId) 会因实例已存在/端口占用而失败。
		try {
			Hazelcast.shutdownAll();
		} catch (Exception e) {
			logger.warn("Engine IT: shutdown hazelcast failed: {}", e.getMessage());
		}
		if (mockTM != null) {
			mockTM.stop();
		}
		logger.info("Engine IT: engine runtime closed");
	}

	// ==================== 工具 ====================

	/** 将对象转成扁平 Map（ObjectId 转 hex 字符串），供写入 MockTM 集合 */
	@SuppressWarnings("unchecked")
	public Map<String, Object> toMap(Object obj) {
		if (obj == null) {
			return new LinkedHashMap<>();
		}
		try {
			Map<String, Object> map = objectMapper.convertValue(obj, new TypeReference<Map<String, Object>>() {
			});
			return normalize(map);
		} catch (IllegalArgumentException e) {
			return toMapByReflection(obj);
		}
	}

	private Map<String, Object> normalize(Map<String, Object> map) {
		Map<String, Object> result = new LinkedHashMap<>();
		map.forEach((k, v) -> result.put(k, normalizeValue(v)));
		return result;
	}

	@SuppressWarnings("unchecked")
	private Object normalizeValue(Object value) {
		if (value instanceof ObjectId) {
			return ((ObjectId) value).toHexString();
		}
		if (value instanceof Map) {
			return normalize((Map<String, Object>) value);
		}
		if (value instanceof List) {
			List<Object> list = new ArrayList<>();
			for (Object item : (List<Object>) value) {
				list.add(normalizeValue(item));
			}
			return list;
		}
		return value;
	}

	private Map<String, Object> toMapByReflection(Object obj) {
		Map<String, Object> map = new LinkedHashMap<>();
		for (Field field : obj.getClass().getDeclaredFields()) {
			try {
				field.setAccessible(true);
				Object value = field.get(obj);
				if (value instanceof ObjectId) {
					value = ((ObjectId) value).toHexString();
				}
				map.put(field.getName(), value);
			} catch (IllegalAccessException ignore) {
			}
		}
		return map;
	}

	private static void sleepQuietly(long millis) {
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	/** 配置项 */
	public static class EngineRuntimeConfig {
		/** MockTM 端口，默认取 -Dengine.it.tm.port，再默认 18080 */
		public int port;
		/** 引擎实例号（agentId/process_id），需与 failsafe environmentVariables 的 process_id 一致 */
		public String instanceNo;
		/** connector jar 源目录（MockTM pdk/jar/v2 的下载源，模拟 TM 侧连接器包库；引擎侧不做本地预置加载） */
		public File connectorJarDir;
	}
}
