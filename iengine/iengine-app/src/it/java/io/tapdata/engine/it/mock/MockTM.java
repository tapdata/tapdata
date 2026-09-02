package io.tapdata.engine.it.mock;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.GZIPInputStream;

/**
 * Mock 管理端（TM）：模拟引擎部署形态中 TM 对外提供的 MongoDB REST 代理。
 * <p>
 * 引擎通过 {@code RestTemplateOperator}/{@code HttpClientMongoOperator} 与 TM 交互：
 * 任意集合的 CRUD 均映射为 {@code /api/{collection}[?filter=&where=]} 的 HTTP 请求，
 * 响应统一包装为 {@code {"code":"ok","data":...}}。本组件用内存 Map 模拟集合存储，
 * 实现引擎启动与任务运行所需的全部端点：
 * <ul>
 *   <li>登录：POST users/generatetoken（返回 LoginResp）、GET user/{id}（返回 User）</li>
 *   <li>通用集合：GET 查询（filter/where）、POST 插入/更新（update/upsertWithWhere/{id}）、DELETE</li>
 *   <li>特殊端点：Workers/singleton-lock、Workers/health、{collection}/count、
 *       {collection}/transformAllParam/{taskId}、DatabaseTypes/pdkHash/{pdkHash}、pdk/jar/v2</li>
 * </ul>
 * 测试可通过 {@link #put(String, Map)} 预置数据（Settings/User/Workers/Connections/Task 等），
 * 通过 {@link #find(String)} 读取引擎上报的状态（任务状态/心跳/指标），用于断言。
 * <p>
 * 依赖：JDK 内置 HttpServer（jdk.httpserver）+ Jackson，无其他第三方依赖。
 */
public class MockTM {

	public static final String SUCCESS_CODE = "ok";

	/** 调试开关：打印收到的每个请求与响应摘要（定位引擎与 TM 交互问题） */
	private static final boolean LOG_REQUESTS = true;

	private final ObjectMapper objectMapper = new ObjectMapper();
	private final HttpServer server;
	private final int port;
	private final ExecutorService executor = Executors.newCachedThreadPool();
	/** 集合名 -> 文档列表（文档为扁平 Map，主键 _id） */
	private final ConcurrentHashMap<String, List<Map<String, Object>>> collections = new ConcurrentHashMap<>();
	/** taskId -> transformAllParam 文档（Task/transformAllParam/{taskId} 独立存储，避免与 Task 文档互相覆盖） */
	private final ConcurrentHashMap<String, Map<String, Object>> transformAllParams = new ConcurrentHashMap<>();
	/** 注入 transformAllParam 获取失败的任务（GET 返回 500，模拟 TM 下发任务配置失败） */
	private final java.util.Set<String> failedTransformAllParams = ConcurrentHashMap.newKeySet();
	/** taskId -> 最近一次下发（{@link #markTaskDispatched}）时间，用于丢弃停机流程尾部的过期状态回调 */
	private final ConcurrentHashMap<String, Long> taskDispatchedAt = new ConcurrentHashMap<>();

	private volatile String accessCode = "it-access-code";
	private volatile File connectorJarDir;

	public MockTM() throws IOException {
		this(0);
	}

	public MockTM(int port) throws IOException {
		try {
			this.server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 0);
		} catch (java.net.BindException e) {
			// 常见于上次 failsafe fork JVM 残留（引擎为 JVM 级单例 + shutdown hook，
			// SIGTERM 可能被挂起的 hook 阻塞）：kill -9 占用进程后重跑，或换 -Dengine.it.tm.port
			throw new IllegalStateException("MockTM 端口 " + port + " 已被占用（可能有残留的 IT fork JVM，"
					+ "lsof -nP -iTCP:" + port + " 定位后 kill -9，或换 -Dengine.it.tm.port）", e);
		}
		this.port = server.getAddress().getPort();
		server.createContext("/", this::handle);
		server.setExecutor(executor);
	}

	/** baseURL，形如 http://127.0.0.1:port/api/ */
	public String getBaseUrl() {
		return "http://127.0.0.1:" + port + "/api/";
	}

	public int getPort() {
		return port;
	}

	public void start() {
		server.start();
	}

	public void stop() {
		server.stop(0);
		executor.shutdownNow();
	}

	public void setAccessCode(String accessCode) {
		this.accessCode = accessCode;
	}

	/** 设置 pdk/jar/v2 下载目录（connector jar 所在目录，可为 null 表示不提供下载） */
	public void setConnectorJarDir(File connectorJarDir) {
		this.connectorJarDir = connectorJarDir;
	}

	// ==================== 测试辅助 API ====================

	/** 预置/覆盖集合数据（清空该集合后写入） */
	public void put(String collection, List<Map<String, Object>> docs) {
		List<Map<String, Object>> copy = new ArrayList<>();
		for (Map<String, Object> doc : docs) {
			copy.add(deepCopy(doc));
		}
		collections.put(collection, copy);
	}

	/** 追加一条文档 */
	public void insert(String collection, Map<String, Object> doc) {
		collections.computeIfAbsent(collection, k -> new ArrayList<>()).add(deepCopy(doc));
	}

	/** 标记任务刚被下发（在下发文档之前调用）：保护新下发的 wait_run 不被上一轮停机流程的
	 *  尾部状态回调（引擎停止流程会上报两次 stopped，第二条可晚于文档变为 wait_run 之后落地）覆盖 */
	public void markTaskDispatched(String taskId) {
		taskDispatchedAt.put(taskId, System.currentTimeMillis());
	}

	/** 按 {@code _id} upsert 单条文档：已存在则整体替换该条，不存在则追加。
	 *  <p>下发任务必须用本方法而不是 {@link #put}：put 的语义是“清空集合后写入”，会把其它
	 *  仍在运行的任务文档一并删掉——引擎 TaskPingTimeMonitor 的 ping 更新按
	 *  {@code _id + status $nin + agentId} 匹配不到文档 → 返回 modifiedCount=0 →
	 *  DAAS（isCloud=false）形态引擎立即自停该任务（cloud 形态只告警继续跑），
	 *  表现为“上一个用例的任务被下一次任务下发悄悄停掉”的用例间串扰。 */
	public void upsert(String collection, Map<String, Object> doc) {
		List<Map<String, Object>> list = collections.computeIfAbsent(collection, k -> new ArrayList<>());
		String id = String.valueOf(doc.get("_id"));
		for (int i = 0; i < list.size(); i++) {
			if (id.equals(String.valueOf(list.get(i).get("_id")))) {
				list.set(i, deepCopy(doc));
				return;
			}
		}
		list.add(deepCopy(doc));
	}

	/** 就地更新集合内指定文档的字段。注意 {@link #findById} 返回深拷贝副本，
	 *  对副本 put 不生效——必须用本方法直接改内部集合（如引擎停止信号认领） */
	public void updateField(String collection, String id, String field, Object value) {
		List<Map<String, Object>> list = collections.get(collection);
		if (list == null) {
			return;
		}
		for (Map<String, Object> doc : list) {
			if (id.equals(String.valueOf(doc.get("_id")))) {
				doc.put(field, value);
				return;
			}
		}
	}

	/** 预置 Task/transformAllParam/{taskId} 文档（任务模型推演所需全部参数） */
	public void putTransformAllParam(String taskId, Map<String, Object> doc) {
		transformAllParams.put(taskId, deepCopy(doc));
	}

	/** 注入 Task/transformAllParam/{taskId} 获取失败（GET 返回 200 + 空 data）：
	 *  模拟 TM 下发任务配置缺失，引擎 HazelcastTaskService.engineTransformSchema 拿到 null
	 *  → transformerWsMessageDto.getOptions() NPE → 抛 TASK_FAILED_TO_LOAD_TABLE_STRUCTURE
	 *  → startTask 同步阶段失败 → 任务落 runError。
	 *  <p>注意不能用 HTTP 500：RestException 会被引擎识别为 TmUnavailableException
	 *  （TM 不可用分支只告警不置 runError，任务停在 running 等待恢复）。 */
	public void failTransformAllParam(String taskId) {
		failedTransformAllParams.add(taskId);
	}

	/** 读取集合当前全部文档（用于断言引擎上报的状态） */
	public List<Map<String, Object>> find(String collection) {
		List<Map<String, Object>> list = collections.get(collection);
		if (list == null) {
			return Collections.emptyList();
		}
		List<Map<String, Object>> copy = new ArrayList<>();
		for (Map<String, Object> doc : list) {
			copy.add(deepCopy(doc));
		}
		return copy;
	}

	/** 按 _id 读取单条文档 */
	public Map<String, Object> findById(String collection, String id) {
		for (Map<String, Object> doc : find(collection)) {
			Object docId = doc.get("_id");
			if (id.equals(String.valueOf(docId))) {
				return doc;
			}
		}
		return null;
	}

	/** 按条件读取（简易等值匹配） */
	public List<Map<String, Object>> find(String collection, Map<String, Object> where) {
		List<Map<String, Object>> result = new ArrayList<>();
		for (Map<String, Object> doc : find(collection)) {
			if (matches(where, doc)) {
				result.add(doc);
			}
		}
		return result;
	}

	// ==================== HTTP 处理 ====================

	private void handle(HttpExchange exchange) throws IOException {
		try {
			URI uri = exchange.getRequestURI();
			String path = uri.getPath();
			// 去掉 /api/ 前缀
			String resource = path;
			if (resource.startsWith("/api/")) {
				resource = resource.substring("/api/".length());
			} else if (resource.startsWith("/api")) {
				resource = resource.substring("/api".length());
			}
			Map<String, String> query = parseQuery(uri.getRawQuery());
			byte[] body = readBody(exchange);

			byte[] response;
			if ("GET".equals(exchange.getRequestMethod())) {
				response = handleGet(resource, query);
			} else if ("POST".equals(exchange.getRequestMethod())) {
				response = handlePost(resource, query, body);
			} else if ("DELETE".equals(exchange.getRequestMethod())) {
				response = handleDelete(resource, query);
			} else {
				response = error(405, "Method not supported: " + exchange.getRequestMethod());
			}
			if (LOG_REQUESTS) {
				if (resource.startsWith("pdk/jar")) {
					// 二进制响应不打印内容，只打长度
					System.out.println("[MockTM] " + exchange.getRequestMethod() + " " + resource + " -> " + response.length + " bytes");
				} else {
					String respStr = new String(response, StandardCharsets.UTF_8);
					// transformAllParam 响应含完整模型，放大截断长度便于排查模型加载
					int maxLen = resource.contains("transformAllParam") ? 6000 : 300;
					System.out.println("[MockTM] " + exchange.getRequestMethod() + " " + resource + (query.isEmpty() ? "" : " ?" + exchange.getRequestURI().getRawQuery())
							+ " -> " + respStr.replaceAll("\\s+", " ").substring(0, Math.min(maxLen, respStr.length())));
				}
			}
			// pdk/jar 下载返回 jar 二进制流，其余统一 JSON（引擎按 Content-Type 选择响应转换器）
			exchange.getResponseHeaders().set("Content-Type", resource.startsWith("pdk/jar") ? "application/octet-stream" : "application/json");
			exchange.sendResponseHeaders(200, response.length);
			exchange.getResponseBody().write(response);
		} catch (Exception e) {
			byte[] response = error(500, "MockTM internal error: " + e.getMessage());
			try {
				exchange.getResponseHeaders().set("Content-Type", "application/json");
				exchange.sendResponseHeaders(500, response.length);
				exchange.getResponseBody().write(response);
			} catch (IOException ignore) {
			}
		} finally {
			exchange.close();
		}
	}

	// ==================== GET ====================

	private byte[] handleGet(String resource, Map<String, String> query) throws IOException {
		if ("users/generatetoken".equals(resource)) {
			return error(404, "generatetoken requires POST");
		}
		if (resource.startsWith("user/") || resource.startsWith("users/")) {
			// 登录后引擎按 users/{userId} 查 User（login 中 new StringBuilder("users").append("/")）
			String coll = resource.startsWith("users/") ? "users" : "user";
			String uid = resource.substring(resource.indexOf('/') + 1);
			Map<String, Object> doc = findById(coll, uid);
			if (doc == null && !"user".equals(coll)) {
				doc = findById("user", uid);
			}
			return ok(doc);
		}
		if (resource.startsWith("pdk/jar/v2")) {
			return handlePdkJarDownload(query);
		}
		// pdk/checkMd5/v3：引擎 PdkUtil.reDownloadIfNeed 下载后校验本地 jar 的 md5，
		// 与本端提供的 jar md5 一致则无需重下；不一致会删除本地 jar 重下（最多 3 轮）。
		// 返回与 PdkSourceUtils.getFileMD5 相同格式的 md5（BigInteger(1, digest).toString(16)）
		if (resource.startsWith("pdk/checkMd5")) {
			File jar = resolveConnectorJar(query);
			return jar == null ? ok(null) : ok(fileMd5(jar));
		}
		String[] parts = resource.split("/", 2);
		String collection = parts[0];
		String sub = parts.length > 1 ? parts[1] : null;

		// {collection}/count
		if ("count".equals(sub)) {
			JsonNode where = parseCollectionWhere(query);
			long count = find(collection).stream().filter(doc -> matches(where, doc)).count();
			return ok(Collections.singletonMap("count", count));
		}
		// {collection}/transformAllParam/{taskId}
		if (sub != null && sub.startsWith("transformAllParam/")) {
			String taskId = sub.substring("transformAllParam/".length());
			if (failedTransformAllParams.contains(taskId)) {
				// 返回 200 + 空 data（跳过下方回退逻辑）：引擎 findOne 得到 null → 模型加载 NPE → runError
				return ok(null);
			}
			Map<String, Object> doc = transformAllParams.get(taskId);
			if (doc == null) {
				// 兼容：回退到集合内以 _id/id 存储的文档
				doc = findById(collection, taskId);
				if (doc == null) {
					for (Map<String, Object> d : find(collection)) {
						if (taskId.equals(String.valueOf(d.get("id")))) {
							doc = d;
							break;
						}
					}
				}
			}
			return ok(doc);
		}
		// {collection}/pdkHash/{pdkHash}
		if (sub != null && sub.startsWith("pdkHash/")) {
			String pdkHash = sub.substring("pdkHash/".length());
			for (Map<String, Object> doc : find(collection)) {
				if (pdkHash.equals(String.valueOf(doc.get("pdkHash")))) {
					return ok(doc);
				}
			}
			return ok(null);
		}
		// {collection}/health：心跳集合查询（独立存储于 collection + "/health"）
		if ("health".equals(sub)) {
			List<Map<String, Object>> docs = queryCollection(collection + "/health", query);
			return ok(docs);
		}
		// HazelcastPersistence：引擎状态存储（httptm → HttpTMIMap）的 find/findOne 查询。
		// 查询参数为 filter（嵌套结构 {"where":"{...inner json...}"}，HttpTMIMap.filterQuery 构造），
		// find 响应需 data.items 列表、findOne 响应 data 为单文档（HttpTMIMap 按 ResponseBody 解析）
		if ("HazelcastPersistence".equals(collection) && (sub == null || "findOne".equals(sub))) {
			JsonNode where = parseFilterWhere(query);
			List<Map<String, Object>> docs = find(collection).stream()
					.filter(doc -> matches(where, doc))
					.collect(java.util.stream.Collectors.toList());
			if ("findOne".equals(sub)) {
				return ok(docs.isEmpty() ? null : docs.get(0));
			}
			return ok(Collections.singletonMap("items", docs));
		}
		// {collection}/{id}
		if (sub != null && !sub.isEmpty() && !sub.contains("/")) {
			Map<String, Object> doc = findById(collection, sub);
			return ok(doc);
		}

		// 通用列表查询
		List<Map<String, Object>> docs = queryCollection(collection, query);
		return ok(docs);
	}

	private List<Map<String, Object>> queryCollection(String collection, Map<String, String> query) throws IOException {
		JsonNode where = parseCollectionWhere(query);
		JsonNode filter = parseFilter(query.get("filter"));
		List<Map<String, Object>> docs = find(collection).stream()
				.filter(doc -> matches(where, doc))
				.sorted(comparator(filter))
				.collect(java.util.stream.Collectors.toList());
		// 分页
		JsonNode limitNode = filter != null ? filter.get("limit") : null;
		JsonNode skipNode = filter != null ? filter.get("skip") : null;
		int skip = skipNode != null ? skipNode.asInt() : 0;
		int limit = limitNode != null ? limitNode.asInt() : Integer.MAX_VALUE;
		if (skip > 0 && skip < docs.size()) {
			docs = new ArrayList<>(docs.subList(skip, docs.size()));
		}
		if (limit < docs.size()) {
			docs = new ArrayList<>(docs.subList(0, limit));
		}
		return docs;
	}

	private Comparator<Map<String, Object>> comparator(JsonNode filter) {
		if (filter == null || !filter.has("order")) {
			return (a, b) -> 0;
		}
		JsonNode order = filter.get("order");
		List<Comparator<Map<String, Object>>> comparators = new ArrayList<>();
		for (JsonNode o : order) {
			String spec = o.asText().trim();
			boolean asc = !spec.toUpperCase().endsWith("DESC");
			String field = spec.replaceAll("\\s+(ASC|DESC)$", "").trim();
			comparators.add((a, b) -> {
				Object va = getByPath(a, field);
				Object vb = getByPath(b, field);
				int cmp = compareValues(va, vb);
				return asc ? cmp : -cmp;
			});
		}
		return (a, b) -> {
			for (Comparator<Map<String, Object>> c : comparators) {
				int r = c.compare(a, b);
				if (r != 0) {
					return r;
				}
			}
			return 0;
		};
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	private int compareValues(Object a, Object b) {
		if (a == null && b == null) {
			return 0;
		}
		if (a == null) {
			return -1;
		}
		if (b == null) {
			return 1;
		}
		if (a instanceof Number && b instanceof Number) {
			return Double.compare(((Number) a).doubleValue(), ((Number) b).doubleValue());
		}
		if (a instanceof Comparable && a.getClass().isInstance(b)) {
			return ((Comparable) a).compareTo(b);
		}
		return String.valueOf(a).compareTo(String.valueOf(b));
	}

	// ==================== POST ====================

	private byte[] handlePost(String resource, Map<String, String> query, byte[] body) throws IOException {
		// 登录
		if ("users/generatetoken".equals(resource)) {
			return handleLogin(body);
		}
		String[] parts = resource.split("/", 2);
		String collection = parts[0];
		String sub = parts.length > 1 ? parts[1] : null;

		// Workers/singleton-lock（含 /upsertWithWhere 子路径）：将 singletonLock 写入 Workers（upsert 语义）并返回 "ok"
		if (sub != null && ("singleton-lock".equals(sub) || sub.startsWith("singleton-lock/"))) {
			JsonNode where = parseWhere(query);
			JsonNode update = parseJson(body);
			if (where != null && update != null) {
				List<Map<String, Object>> workers = collections.computeIfAbsent("Workers", k -> new ArrayList<>());
				boolean matched = false;
				for (Map<String, Object> w : workers) {
					if (matches(where, w)) {
						update.fields().forEachRemaining(e -> setByPath(w, e.getKey(), convertJsonNode(e.getValue())));
						matched = true;
					}
				}
				if (!matched) {
					// upsert：无匹配时以 where + update 插入新 worker 记录
					Map<String, Object> newWorker = new HashMap<>();
					where.fields().forEachRemaining(e -> setByPath(newWorker, e.getKey(), convertJsonNode(e.getValue())));
					update.fields().forEachRemaining(e -> setByPath(newWorker, e.getKey(), convertJsonNode(e.getValue())));
					if (!newWorker.containsKey("_id")) {
						newWorker.put("_id", UUID.randomUUID().toString().replace("-", ""));
					}
					workers.add(newWorker);
				}
			}
			return okString("ok");
		}
		// {collection}/health：心跳集合（WorkerHeatBeatReports insertOne "Workers/health"），独立存储
		if ("health".equals(sub)) {
			String healthCollection = collection + "/health";
			List<Map<String, Object>> healthDocs = collections.computeIfAbsent(healthCollection, k -> new ArrayList<>());
			JsonNode bodyNode = parseJson(body);
			if (bodyNode != null && bodyNode.isObject()) {
				Map<String, Object> doc = objectMapper.convertValue(bodyNode, new com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>>() {
				});
				insert(healthCollection, doc);
				return ok(doc);
			}
			return ok(Collections.emptyList());
		}
		// {collection}/update：按 where 更新，返回 {count: n}
		if ("update".equals(sub)) {
			JsonNode where = parseWhere(query);
			JsonNode update = parseJson(body);
			return handleUpdate(collection, where, update, false);
		}
		// {collection}/upsertWithWhere：按 where 更新，无匹配则插入
		if ("upsertWithWhere".equals(sub)) {
			JsonNode where = parseWhere(query);
			JsonNode update = parseJson(body);
			return handleUpdate(collection, where, update, true);
		}
		// Task/syncProgress/{taskId}：引擎 HazelcastTargetPdkBaseNode 每轮 flush 上报全量/增量进度，
		// body = {nodePair: syncProgressJson}（nodePair 如 "[source-db-node, target-db-node]"，
		// value 为 SyncProgress 的 JSON 字符串）。真实 TM 将其更新到 Task 文档的 syncProgress 字段，
		// awaitSyncStage 据此断言 syncStage（如 CDC）
		if ("Task".equals(collection) && sub != null && sub.startsWith("syncProgress/")) {
			String taskId = sub.substring("syncProgress/".length());
			Map<String, Object> progress = new LinkedHashMap<>();
			JsonNode bodyNode = parseJson(body);
			if (bodyNode != null && bodyNode.isObject()) {
				bodyNode.fields().forEachRemaining(e -> progress.put(e.getKey(), convertJsonNode(e.getValue())));
			}
			for (Map<String, Object> doc : collections.computeIfAbsent(collection, k -> new ArrayList<>())) {
				if (taskId.equals(String.valueOf(doc.get("_id")))) {
					doc.put("syncProgress", progress);
					break;
				}
			}
			return ok(progress);
		}
		// Task 状态回调端点（TM TaskController 的 running/{id}/runError/{id}/stopped/{id}/complete/{id} 等）：
		// 更新任务状态并返回 {successIds:[id]}（引擎以 TaskOpRespDto 解析，successIds 为 null 会 NPE）
		if ("Task".equals(collection) && sub != null && sub.contains("/")) {
			String action = sub.substring(0, sub.indexOf('/'));
			String taskId = sub.substring(sub.indexOf('/') + 1);
			String statusValue = TASK_STATUS_ACTIONS.get(action);
			if (statusValue != null && !taskId.contains("/")) {
				List<Map<String, Object>> list = collections.computeIfAbsent(collection, k -> new ArrayList<>());
				boolean found = false;
				for (Map<String, Object> doc : list) {
					if (taskId.equals(String.valueOf(doc.get("_id")))) {
						// 重启竞态保护：任务仍处于刚下发的 wait_run（未被认领）且回调是上一轮停机/失败流程
						// 的尾部上报时，不能把新下发的 wait_run 覆盖回终态——否则调度器永远看不到待运行任务，
						// 表现为重启的用例等不到连接器就绪（真实 TM 靠作业实例代次区分，MockTM 以下发时间近似）
						if (isStaleTerminalCallback(doc, statusValue, taskId)) {
							System.out.println("[MockTM] drop stale " + action + " callback for freshly dispatched task " + taskId);
							return ok(Collections.singletonMap("successIds", Collections.singletonList(taskId)));
						}
						doc.put("status", statusValue);
						found = true;
					}
				}
				List<String> successIds = found ? Collections.singletonList(taskId) : Collections.emptyList();
				return ok(Collections.singletonMap("successIds", successIds));
			}
		}
		// {collection}/deleteAll：body {where: json}
		if ("deleteAll".equals(sub)) {
			JsonNode bodyNode = parseJson(body);
			JsonNode where = bodyNode != null && bodyNode.has("where") ? parseJsonString(bodyNode.get("where").asText()) : null;
			List<Map<String, Object>> list = collections.computeIfAbsent(collection, k -> new ArrayList<>());
			list.removeIf(doc -> matches(where, doc));
			return ok(Collections.emptyList());
		}
		// {collection}/batch：批量插入
		if ("batch".equals(sub)) {
			JsonNode bodyNode = parseJson(body);
			if (bodyNode != null && bodyNode.isArray()) {
				for (JsonNode item : bodyNode) {
					insert(collection, objectMapper.convertValue(item, new com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>>() {
					}));
				}
			}
			return ok(Collections.emptyList());
		}
		// {collection}/customCount：返回 {count}
		if ("customCount".equals(sub)) {
			JsonNode bodyNode = parseJson(body);
			Map<String, Object> whereMap = new HashMap<>();
			if (bodyNode != null && bodyNode.isObject()) {
				bodyNode.fields().forEachRemaining(e -> whereMap.put(e.getKey(), convertJsonNode(e.getValue())));
			}
			long count = find(collection).stream().filter(doc -> matches(whereMap, doc)).count();
			return ok(Collections.singletonMap("count", count));
		}
		// {collection}/logSetting/{level}/{taskId}：按 id 更新
		if (sub != null && sub.startsWith("logSetting/")) {
			String taskId = sub.substring("logSetting/".length()).split("/")[1];
			JsonNode update = parseJson(body);
			return handleUpdateById(collection, taskId, update);
		}
		// {collection}/{id}：按 id 更新（updateById / findAndModify 更新）
		if (sub != null && !sub.isEmpty() && !sub.contains("/")) {
			JsonNode update = parseJson(body);
			return handleUpdateById(collection, sub, update);
		}

		// 通用插入/upsert：有 where 参数则按 where 更新，否则插入
		JsonNode bodyNode = parseJson(body);
		JsonNode where = parseWhere(query);
		if (where != null && where.size() > 0) {
			return handleUpdate(collection, where, bodyNode, true);
		}
		if (bodyNode != null && bodyNode.isArray()) {
			for (JsonNode item : bodyNode) {
				insert(collection, objectMapper.convertValue(item, new com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>>() {
				}));
			}
		} else if (bodyNode != null && bodyNode.isObject()) {
			Map<String, Object> doc = objectMapper.convertValue(bodyNode, new com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>>() {
			});
			insert(collection, doc);
			return ok(doc);
		}
		return ok(Collections.emptyList());
	}

	private byte[] handleLogin(byte[] body) throws IOException {
		JsonNode bodyNode = parseJson(body);
		String requestedCode = bodyNode != null ? bodyNode.path("accesscode").asText(null) : null;
		if (requestedCode != null && !requestedCode.isEmpty() && !accessCode.equals(requestedCode)) {
			return error(401, "Invalid accesscode");
		}
		String userId = "it-user-id";
		Map<String, Object> loginResp = new LinkedHashMap<>();
		loginResp.put("id", UUID.randomUUID().toString().replace("-", ""));
		loginResp.put("created", DateTimeFormatter.ISO_INSTANT.format(Instant.now().atOffset(ZoneOffset.UTC)));
		loginResp.put("userId", userId);
		loginResp.put("ttl", 24L * 60 * 60);
		return ok(loginResp);
	}

	/** pdk/jar/v2：引擎 PdkUtil.downloadPdkFileIfNeed 按任务配置（DatabaseType 的
	 *  jarFile/jarRid/pdkHash）下载 connector jar，返回 jar 二进制流 */
	private byte[] handlePdkJarDownload(Map<String, String> query) throws IOException {
		File jar = resolveConnectorJar(query);
		if (jar == null) {
			return error(404, "No connector jar found for pdkHash=" + query.get("pdkHash")
					+ ", fileName=" + query.get("fileName") + " in " + connectorJarDir);
		}
		return Files.readAllBytes(jar.toPath());
	}

	/** 按下载参数定位要提供的 connector jar：优先 fileName 精确匹配（DatabaseTypes 文档的 jarFile，
	 *  如 mysql-connector-1.0-SNAPSHOT.jar），回退 pdkHash 数据库类型前缀（首个 - 之前）匹配 */
	private File resolveConnectorJar(Map<String, String> query) {
		if (connectorJarDir == null || !connectorJarDir.isDirectory()) {
			return null;
		}
		// 递归扫描：maven 仓库布局为 {artifactId}/{version}/{jar}，顶层无 jar 文件
		List<File> jars = new ArrayList<>();
		try (java.util.stream.Stream<Path> stream = Files.walk(connectorJarDir.toPath())) {
			stream.filter(p -> Files.isRegularFile(p) && p.getFileName().toString().endsWith(".jar"))
					.forEach(p -> jars.add(p.toFile()));
		} catch (IOException e) {
			return null;
		}
		if (jars.isEmpty()) {
			return null;
		}
		String fileName = query.get("fileName");
		if (fileName != null && !fileName.isEmpty()) {
			for (File f : jars) {
				if (f.getName().equals(fileName)) {
					return f;
				}
			}
		}
		String pdkHash = query.get("pdkHash");
		if (pdkHash != null) {
			// pdkHash（如 mysql-pdk-hash）与 jar 文件名（mysql-connector-1.0-SNAPSHOT.jar）
			// 无直接包含关系，取数据库类型前缀 + -connector 约定匹配：
			// 不能只匹配前缀（mongodb- 会先命中 mongodb-storage-module 等非连接器构件）
			String prefix = pdkHash.contains("-") ? pdkHash.substring(0, pdkHash.indexOf('-')) : pdkHash;
			for (File f : jars) {
				if (f.getName().startsWith(prefix + "-connector")) {
					return f;
				}
			}
		}
		return null;
	}

	/** 文件 MD5（格式与 TM 侧 PdkSourceUtils.getFileMD5 一致：BigInteger(1, digest).toString(16) 小写 hex） */
	private String fileMd5(File file) {
		try (InputStream in = new FileInputStream(file)) {
			MessageDigest digest = MessageDigest.getInstance("MD5");
			byte[] buffer = new byte[8192];
			int len;
			while ((len = in.read(buffer)) != -1) {
				digest.update(buffer, 0, len);
			}
			return new BigInteger(1, digest.digest()).toString(16);
		} catch (Exception e) {
			return null;
		}
	}

	// ==================== DELETE ====================

	private byte[] handleDelete(String resource, Map<String, String> query) throws IOException {
		String[] parts = resource.split("/", 2);
		String collection = parts[0];
		String sub = parts.length > 1 ? parts[1] : null;
		List<Map<String, Object>> list = collections.computeIfAbsent(collection, k -> new ArrayList<>());
		// {collection}/deleteAll：按 where 参数删除（HttpTMIMap.delete 的 DELETE HazelcastPersistence/deleteAll）
		if ("deleteAll".equals(sub)) {
			JsonNode where = parseWhere(query);
			list.removeIf(doc -> matches(where, doc));
			return ok(Collections.emptyList());
		}
		if (sub != null && !sub.isEmpty() && !sub.contains("/")) {
			list.removeIf(doc -> sub.equals(String.valueOf(doc.get("_id"))));
		} else {
			JsonNode where = parseWhere(query);
			list.removeIf(doc -> matches(where, doc));
		}
		return ok(Collections.emptyList());
	}

	// ==================== 更新语义 ====================

	private byte[] handleUpdate(String collection, JsonNode where, JsonNode update, boolean upsert) {
		List<Map<String, Object>> list = collections.computeIfAbsent(collection, k -> new ArrayList<>());
		int matched = 0;
		for (Map<String, Object> doc : list) {
			if (matches(where, doc)) {
				applyUpdate(doc, update, false);
				matched++;
			}
		}
		if (matched == 0 && upsert) {
			Map<String, Object> newDoc = new HashMap<>();
			if (where != null) {
				where.fields().forEachRemaining(e -> setByPath(newDoc, e.getKey(), convertJsonNode(e.getValue())));
			}
			applyUpdate(newDoc, update, true);
			if (!newDoc.containsKey("_id")) {
				newDoc.put("_id", UUID.randomUUID().toString().replace("-", ""));
			}
			list.add(newDoc);
			matched = 1;
		}
		return ok(Collections.singletonMap("count", matched));
	}

	private byte[] handleUpdateById(String collection, String id, JsonNode update) {
		List<Map<String, Object>> list = collections.computeIfAbsent(collection, k -> new ArrayList<>());
		for (Map<String, Object> doc : list) {
			if (id.equals(String.valueOf(doc.get("_id")))) {
				applyUpdate(doc, update, false);
				return ok(doc);
			}
		}
		return ok(null);
	}

	/** 刚下发后保护其 wait_run 不被尾部终态回调覆盖的时间窗口（毫秒） */
	private static final long STALE_CALLBACK_WINDOW_MS = 3_000L;
	/** 需要判定的终态回调状态。只保护 stopped（日志已实测停机流程会连报两次 stopped）：
	 *  runError / complete 不能保护——它们可能就在下发后几百毫秒内发生（错误注入、全量自然完成），
	 *  误丢会把真实失败藏起来 */
	private static final java.util.Set<String> STALE_CALLBACK_TERMINAL_STATUS = java.util.Set.of("stopped");

	/** 判定是否为上一轮流程的尾部终态回调：任务文档仍是刚下发的 wait_run（未被认领）
	 *  且本次下发距今在窗口期内。
	 *  <p>引擎停止流程会上报两次 stopped，第二条可晚于测试重新下发 wait_run 落地，直接覆盖会使
	 *  调度器再也看不到该任务（重启续跑用例表现为等不到连接器就绪而超时）。真实 TM 按作业实例
	 *  代次区分，MockTM 无代次概念，故以下发时间近似；窗口期后到达的终态回调按真实语义生效。 */
	private boolean isStaleTerminalCallback(Map<String, Object> doc, String statusValue, String taskId) {
		if (!STALE_CALLBACK_TERMINAL_STATUS.contains(statusValue)) {
			return false;
		}
		if (!"wait_run".equals(String.valueOf(doc.get("status")))) {
			return false;
		}
		Long dispatchedAt = taskDispatchedAt.get(taskId);
		return dispatchedAt != null && System.currentTimeMillis() - dispatchedAt < STALE_CALLBACK_WINDOW_MS;
	}

	/** Task 状态回调动作 → 任务状态值（与 TM TaskService.running/runError/... 语义一致） */
	private static final Map<String, String> TASK_STATUS_ACTIONS = new HashMap<>();

	static {
		TASK_STATUS_ACTIONS.put("running", "running");
		TASK_STATUS_ACTIONS.put("runError", "runError");
		TASK_STATUS_ACTIONS.put("stopped", "stopped");
		TASK_STATUS_ACTIONS.put("complete", "complete");
		TASK_STATUS_ACTIONS.put("renew", "renewing");
		TASK_STATUS_ACTIONS.put("systemStop", "stopping");
		TASK_STATUS_ACTIONS.put("stop", "stopping");
	}


	/** 应用 mongo update 操作符：$set/$setOnInsert/$inc/$unset/$push（点路径展开） */
	private void applyUpdate(Map<String, Object> doc, JsonNode update, boolean isInsert) {
		if (update == null || !update.isObject()) {
			return;
		}
		update.fields().forEachRemaining(entry -> {
			String op = entry.getKey();
			JsonNode value = entry.getValue();
			if ("$set".equals(op)) {
				value.fields().forEachRemaining(e -> setByPath(doc, e.getKey(), convertJsonNode(e.getValue())));
			} else if ("$setOnInsert".equals(op)) {
				if (isInsert) {
					value.fields().forEachRemaining(e -> setByPath(doc, e.getKey(), convertJsonNode(e.getValue())));
				}
			} else if ("$inc".equals(op)) {
				value.fields().forEachRemaining(e -> {
					Object old = getByPath(doc, e.getKey());
					long delta = e.getValue().asLong();
					long base = old instanceof Number ? ((Number) old).longValue() : 0;
					setByPath(doc, e.getKey(), base + delta);
				});
			} else if ("$unset".equals(op)) {
				value.fields().forEachRemaining(e -> removeByPath(doc, e.getKey()));
			} else if ("$push".equals(op)) {
				value.fields().forEachRemaining(e -> {
					Object old = getByPath(doc, e.getKey());
					List<Object> listValue;
					if (old instanceof List) {
						listValue = (List<Object>) old;
					} else {
						listValue = new ArrayList<>();
						setByPath(doc, e.getKey(), listValue);
					}
					listValue.add(convertJsonNode(e.getValue()));
				});
			} else {
				// 非操作符（如普通字段覆盖）直接赋值
				setByPath(doc, op, convertJsonNode(value));
			}
		});
	}

	// ==================== where/filter 匹配 ====================

	private JsonNode parseWhere(Map<String, String> query) throws IOException {
		String whereStr = query.get("where");
		if (whereStr == null || whereStr.isEmpty()) {
			return null;
		}
		JsonNode where = parseLenient(whereStr);
		// HttpTMIMap.whereQuery 构造的 where 参数为 {where: "{...inner json...}", access_token: xxx}
		// （外层是 Java Map toString，内层是合法 JSON 字符串），取 where 子节点作为真正查询条件
		if (where != null && where.has("where")) {
			JsonNode inner = where.get("where");
			if (inner.isTextual()) {
				return parseLenient(inner.asText());
			}
			if (inner.isObject()) {
				return inner;
			}
		}
		return where;
	}

	/** 普通集合 GET 查询统一用 filter 参数（引擎 MongoTemplate 的 find 走 GET {collection}?filter={where:...}），
	 *  兼容直接 where 参数 */
	private JsonNode parseCollectionWhere(Map<String, String> query) throws IOException {
		if (query.containsKey("filter") && !query.get("filter").isEmpty()) {
			return parseFilterWhere(query);
		}
		return parseWhere(query);
	}

	private JsonNode parseFilter(String filterStr) throws IOException {
		if (filterStr == null || filterStr.isEmpty()) {
			return null;
		}
		return parseLenient(filterStr);
	}

	/** 解析 HttpTMIMap.filterQuery 的嵌套 filter 参数：{"where":"{...inner json...}"} → 真正的 where JsonNode */
	private JsonNode parseFilterWhere(Map<String, String> query) throws IOException {
		String filterStr = query.get("filter");
		if (filterStr == null || filterStr.isEmpty()) {
			return null;
		}
		JsonNode filter = parseLenient(filterStr);
		if (filter != null && filter.has("where")) {
			JsonNode whereNode = filter.get("where");
			if (whereNode.isTextual()) {
				return parseLenient(whereNode.asText());
			}
			return whereNode;
		}
		return filter;
	}

	/**
	 * 容错 JSON 解析：标准 JSON 失败时兼容 Java Map.toString 格式
	 * （如 HttpTMIMap 把 {filter: {where: json}} 直接 toString 后作为 query 参数发送，
	 * 真实 TM 依赖 Spring MVC 对嵌套 Map 的容错解析，MockTM 需对齐）。
	 */
	private JsonNode parseLenient(String json) throws IOException {
		if (json == null || json.isEmpty()) {
			return null;
		}
		try {
			return objectMapper.readTree(json);
		} catch (JsonProcessingException e) {
			// Java Map.toString 格式：{k=v, k2=v2}，value 可能是嵌套 JSON 字符串/对象
			if (json.startsWith("{") && json.endsWith("}")) {
				String inner = json.substring(1, json.length() - 1);
				ObjectNode node = objectMapper.createObjectNode();
				for (String part : splitTopLevel(inner, ',')) {
					int eq = part.indexOf('=');
					if (eq > 0) {
						String key = part.substring(0, eq).trim();
						String value = part.substring(eq + 1).trim();
						node.set(key, parseLenientValue(value));
					}
				}
				if (!node.isEmpty()) {
					return node;
				}
			}
			throw new IOException("Invalid JSON: " + json, e);
		}
	}

	/** 解析 lenient value：嵌套 JSON 字符串/对象/数组/数字/布尔/普通文本 */
	private JsonNode parseLenientValue(String value) {
		if (value == null || value.isEmpty()) {
			return objectMapper.nullNode();
		}
		try {
			return objectMapper.readTree(value);
		} catch (JsonProcessingException e) {
			// 普通文本：去掉可能的引号
			String v = value;
			if (v.length() >= 2 && ((v.startsWith("\"") && v.endsWith("\"")) || (v.startsWith("'") && v.endsWith("'")))) {
				v = v.substring(1, v.length() - 1);
			}
			return objectMapper.getNodeFactory().textNode(v);
		}
	}

	/** 按顶层分隔符拆分（跟踪 {} [] "" 深度，忽略嵌套内容中的分隔符） */
	private static List<String> splitTopLevel(String s, char sep) {
		List<String> parts = new ArrayList<>();
		int depth = 0;
		boolean inString = false;
		char quote = 0;
		int start = 0;
		for (int i = 0; i < s.length(); i++) {
			char c = s.charAt(i);
			if (inString) {
				if (c == '\\') {
					i++;
				} else if (c == quote) {
					inString = false;
				}
			} else if (c == '"' || c == '\'') {
				inString = true;
				quote = c;
			} else if (c == '{' || c == '[') {
				depth++;
			} else if (c == '}' || c == ']') {
				depth--;
			} else if (c == sep && depth == 0) {
				parts.add(s.substring(start, i).trim());
				start = i + 1;
			}
		}
		if (start < s.length()) {
			parts.add(s.substring(start).trim());
		}
		return parts;
	}

	private boolean matches(JsonNode where, Map<String, Object> doc) {
		if (where == null || where.size() == 0) {
			return true;
		}
		// 顶层 $or/$and 可与普通字段混合（如 TapdataTaskScheduler.findStopTask 的
		// {"_id":xxx,"$or":[{status:stopping},{status:stop}]}）：先逐字段匹配普通条件，
		// 再按 $or（任一子条件命中）/ $and（全部命中）合并逻辑条件结果
		JsonNode orNode = where.get("$or");
		JsonNode andNode = where.get("$and");
		if (orNode != null || andNode != null) {
			Iterator<Map.Entry<String, JsonNode>> it = where.fields();
			while (it.hasNext()) {
				Map.Entry<String, JsonNode> entry = it.next();
				if ("$or".equals(entry.getKey()) || "$and".equals(entry.getKey())) {
					continue;
				}
				if (!matchField(entry.getKey(), entry.getValue(), doc)) {
					return false;
				}
			}
			if (orNode != null && orNode.isArray()) {
				for (JsonNode cond : orNode) {
					if (matches(cond, doc)) {
						return true;
					}
				}
				return false;
			}
			if (andNode != null && andNode.isArray()) {
				for (JsonNode cond : andNode) {
					if (!matches(cond, doc)) {
						return false;
					}
				}
				return true;
			}
		}
		Iterator<Map.Entry<String, JsonNode>> it = where.fields();
		while (it.hasNext()) {
			Map.Entry<String, JsonNode> entry = it.next();
			if (!matchField(entry.getKey(), entry.getValue(), doc)) {
				return false;
			}
		}
		return true;
	}

	private boolean matches(Map<String, Object> where, Map<String, Object> doc) {
		if (where == null || where.isEmpty()) {
			return true;
		}
		for (Map.Entry<String, Object> entry : where.entrySet()) {
			if (!matchField(entry.getKey(), entry.getValue(), doc)) {
				return false;
			}
		}
		return true;
	}

	private boolean matchField(String key, Object condition, Map<String, Object> doc) {
		Object actual = getByPath(doc, key);
		return compareCondition(actual, condition) == 0;
	}

	private boolean matchField(String key, JsonNode condition, Map<String, Object> doc) {
		Object actual = getByPath(doc, key);
		if (condition.isObject() && !condition.isEmpty()) {
			// 运算符条件 {"$ne":..., "$in":..., "$gte":...}
			boolean allMatch = true;
			boolean hasOperator = false;
			Iterator<Map.Entry<String, JsonNode>> it = condition.fields();
			while (it.hasNext()) {
				Map.Entry<String, JsonNode> entry = it.next();
				if (entry.getKey().startsWith("$") || entry.getKey().startsWith("inq")) {
					hasOperator = true;
					if (!matchOperator(entry.getKey(), entry.getValue(), actual)) {
						allMatch = false;
						break;
					}
				}
			}
			if (hasOperator) {
				return allMatch;
			}
			return compareCondition(actual, convertJsonNode(condition)) == 0;
		}
		return compareCondition(actual, convertJsonNode(condition)) == 0;
	}

	private boolean matchOperator(String op, JsonNode expected, Object actual) {
		switch (op) {
			case "$ne":
				return !valuesEqual(actual, convertJsonNode(expected));
			case "$in":
			case "inq":
				if (expected.isArray()) {
					for (JsonNode item : expected) {
						if (valuesEqual(actual, convertJsonNode(item))) {
							return true;
						}
					}
					return false;
				}
				return false;
			case "$nin":
				if (expected.isArray()) {
					for (JsonNode item : expected) {
						if (valuesEqual(actual, convertJsonNode(item))) {
							return false;
						}
					}
					return true;
				}
				return false;
			case "$gt":
				return compareCondition(actual, convertJsonNode(expected)) > 0;
			case "$gte":
				return compareCondition(actual, convertJsonNode(expected)) >= 0;
			case "$lt":
				return compareCondition(actual, convertJsonNode(expected)) < 0;
			case "$lte":
				return compareCondition(actual, convertJsonNode(expected)) <= 0;
			case "$exists": {
				boolean exists = actual != null;
				return expected.asBoolean() ? exists : !exists;
			}
			case "$regex":
				try {
					return actual != null && String.valueOf(actual).matches(expected.asText());
				} catch (Exception e) {
					return false;
				}
			default:
				return compareCondition(actual, convertJsonNode(expected)) == 0;
		}
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	private int compareCondition(Object actual, Object expected) {
		if (actual == null && expected == null) {
			return 0;
		}
		if (actual == null) {
			return -1;
		}
		if (expected == null) {
			return 1;
		}
		if (actual instanceof Number && expected instanceof Number) {
			return Double.compare(((Number) actual).doubleValue(), ((Number) expected).doubleValue());
		}
		if (actual instanceof Comparable && actual.getClass().isInstance(expected)) {
			return ((Comparable) actual).compareTo(expected);
		}
		if (expected instanceof Number && actual instanceof String) {
			try {
				return Double.compare(Double.parseDouble((String) actual), ((Number) expected).doubleValue());
			} catch (NumberFormatException ignore) {
			}
		}
		return String.valueOf(actual).compareTo(String.valueOf(expected));
	}

	private boolean valuesEqual(Object actual, Object expected) {
		return compareCondition(actual, expected) == 0;
	}

	private boolean compareCondition(JsonNode expected, Object actual) {
		return compareCondition(actual, convertJsonNode(expected)) == 0;
	}

	// ==================== 工具 ====================

	/** 按点路径取值（stats.total.source_received） */
	@SuppressWarnings("unchecked")
	private Object getByPath(Map<String, Object> doc, String path) {
		if (!path.contains(".")) {
			return doc.get(path);
		}
		String[] keys = path.split("\\.");
		Object current = doc;
		for (String key : keys) {
			if (current instanceof Map) {
				current = ((Map<String, Object>) current).get(key);
			} else {
				return null;
			}
		}
		return current;
	}

	@SuppressWarnings("unchecked")
	private void setByPath(Map<String, Object> doc, String path, Object value) {
		if (!path.contains(".")) {
			doc.put(path, value);
			return;
		}
		String[] keys = path.split("\\.");
		Map<String, Object> current = doc;
		for (int i = 0; i < keys.length - 1; i++) {
			Object next = current.get(keys[i]);
			if (!(next instanceof Map)) {
				Map<String, Object> child = new HashMap<>();
				current.put(keys[i], child);
				current = child;
			} else {
				current = (Map<String, Object>) next;
			}
		}
		current.put(keys[keys.length - 1], value);
	}

	@SuppressWarnings("unchecked")
	private void removeByPath(Map<String, Object> doc, String path) {
		if (!path.contains(".")) {
			doc.remove(path);
			return;
		}
		String[] keys = path.split("\\.");
		Map<String, Object> current = doc;
		for (int i = 0; i < keys.length - 1; i++) {
			Object next = current.get(keys[i]);
			if (!(next instanceof Map)) {
				return;
			}
			current = (Map<String, Object>) next;
		}
		current.remove(keys[keys.length - 1]);
	}

	private Object convertJsonNode(JsonNode node) {
		if (node == null || node.isNull()) {
			return null;
		}
		if (node.isObject()) {
			Map<String, Object> map = new LinkedHashMap<>();
			node.fields().forEachRemaining(e -> map.put(e.getKey(), convertJsonNode(e.getValue())));
			return map;
		}
		if (node.isArray()) {
			List<Object> list = new ArrayList<>();
			node.forEach(item -> list.add(convertJsonNode(item)));
			return list;
		}
		if (node.isTextual()) {
			return node.asText();
		}
		if (node.isBoolean()) {
			return node.asBoolean();
		}
		if (node.isIntegralNumber()) {
			return node.asLong();
		}
		if (node.isFloatingPointNumber()) {
			return node.asDouble();
		}
		return node.asText();
	}

	private Map<String, Object> deepCopy(Map<String, Object> doc) {
		try {
			return objectMapper.convertValue(doc, new com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>>() {
			});
		} catch (IllegalArgumentException e) {
			return new LinkedHashMap<>(doc);
		}
	}

	private Map<String, String> parseQuery(String rawQuery) {
		Map<String, String> map = new HashMap<>();
		if (rawQuery == null || rawQuery.isEmpty()) {
			return map;
		}
		for (String pair : rawQuery.split("&")) {
			int idx = pair.indexOf('=');
			if (idx > 0) {
				map.put(pair.substring(0, idx), java.net.URLDecoder.decode(pair.substring(idx + 1), StandardCharsets.UTF_8));
			} else if (idx < 0) {
				map.put(pair, "");
			}
		}
		return map;
	}

	private JsonNode parseJson(byte[] body) throws IOException {
		if (body == null || body.length == 0) {
			return null;
		}
		return parseJsonString(new String(body, StandardCharsets.UTF_8));
	}

	private JsonNode parseJsonString(String json) throws IOException {
		try {
			return objectMapper.readTree(json);
		} catch (JsonProcessingException e) {
			throw new IOException("Invalid JSON: " + json, e);
		}
	}

	private byte[] readBody(HttpExchange exchange) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		try (InputStream in = exchange.getRequestBody()) {
			// 引擎 RestTemplateOperator 对 >1024B 的请求体会 gzip 压缩
			InputStream source = in;
			String encoding = exchange.getRequestHeaders().getFirst("Content-Encoding");
			if (encoding != null && encoding.toLowerCase().contains("gzip")) {
				source = new GZIPInputStream(in);
			}
			byte[] buffer = new byte[8192];
			int n;
			while ((n = source.read(buffer)) != -1) {
				out.write(buffer, 0, n);
			}
		}
		return out.toByteArray();
	}

	private byte[] ok(Object data) {
		return buildResponse(SUCCESS_CODE, data, null);
	}

	private byte[] okString(String data) {
		return buildResponse(SUCCESS_CODE, data, null);
	}

	private byte[] error(int code, String message) {
		return buildResponse(String.valueOf(code), null, message);
	}

	private byte[] buildResponse(String code, Object data, String message) {
		Map<String, Object> resp = new LinkedHashMap<>();
		resp.put("code", code);
		if (message != null) {
			resp.put("message", message);
		}
		if (data != null) {
			resp.put("data", data);
		}
		resp.put("reqId", UUID.randomUUID().toString());
		resp.put("ts", System.currentTimeMillis());
		try {
			return objectMapper.writeValueAsBytes(resp);
		} catch (IOException e) {
			throw new UncheckedIOException("MockTM: serialize response failed", e);
		}
	}
}
