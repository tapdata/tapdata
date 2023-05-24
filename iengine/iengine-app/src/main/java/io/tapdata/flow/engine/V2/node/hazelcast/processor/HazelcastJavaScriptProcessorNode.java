package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSDownloadStream;
import com.oracle.truffle.js.scriptengine.GraalJSScriptEngine;
import com.tapdata.cache.ICacheGetter;
import com.tapdata.cache.scripts.ScriptCacheService;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.MapUtil;
import com.tapdata.entity.AppType;
import com.tapdata.entity.Connections;
import com.tapdata.entity.JavaScriptFunctions;
import com.tapdata.entity.OperationType;
import com.tapdata.entity.SyncStage;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.processor.ScriptConnection;
import com.tapdata.processor.ScriptUtil;
import com.tapdata.processor.constant.JSEngineEnum;
import com.tapdata.processor.context.ProcessContext;
import com.tapdata.processor.context.ProcessContextEvent;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.process.CacheLookupProcessorNode;
import com.tapdata.tm.commons.dag.process.JsProcessorNode;
import com.tapdata.tm.commons.dag.process.MigrateJsProcessorNode;
import com.tapdata.tm.commons.dag.process.StandardJsProcessorNode;
import com.tapdata.tm.commons.dag.process.StandardMigrateJsProcessorNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.ProcessorNodeType;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.logger.TapLog;
import io.tapdata.entity.script.ScriptFactory;
import io.tapdata.entity.script.ScriptOptions;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.error.TaskProcessorExCode_11;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.node.hazelcast.processor.util.JsUtil;
import io.tapdata.flow.engine.V2.script.ObsScriptLogger;
import io.tapdata.flow.engine.V2.script.ScriptExecutorsManager;
import io.tapdata.flow.engine.V2.util.GraphUtil;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import io.tapdata.pdk.core.utils.CommonUtils;
import lombok.SneakyThrows;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.types.ObjectId;
import org.jetbrains.annotations.NotNull;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Query;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.springframework.data.mongodb.core.query.Criteria.where;

public class HazelcastJavaScriptProcessorNode extends HazelcastProcessorBaseNode {

	private static final Logger logger = LogManager.getLogger(HazelcastJavaScriptProcessorNode.class);
	public static final String TAG = HazelcastJavaScriptProcessorNode.class.getSimpleName();

	private final Invocable engine;

	private ScriptExecutorsManager scriptExecutorsManager;

	private final ThreadLocal<Map<String, Object>> processContextThreadLocal;
	private ScriptExecutorsManager.ScriptExecutor source;
	private ScriptExecutorsManager.ScriptExecutor target;

	/**
	 * standard js
	 */
	private boolean standard;

	private boolean finalJs = false;

	@SneakyThrows
	public HazelcastJavaScriptProcessorNode(ProcessorBaseContext processorBaseContext) {
		super(processorBaseContext);
		Node<?> node = getNode();
		String script;
		if (node instanceof JsProcessorNode) {
			script = ((JsProcessorNode) node).getScript();
		} else if (node instanceof MigrateJsProcessorNode) {
			MigrateJsProcessorNode processorNode = (MigrateJsProcessorNode) node;
			script = processorNode.getScript();
		} else if (node instanceof CacheLookupProcessorNode) {
			script = ((CacheLookupProcessorNode) node).getScript();
		} else {
			throw new RuntimeException("unsupported node " + node.getClass().getName());
		}

		if (node instanceof StandardJsProcessorNode || node instanceof StandardMigrateJsProcessorNode) {
			this.standard = true;
		}
		if (node instanceof JsProcessorNode){
			JsProcessorNode processorNode = (JsProcessorNode) node;
			int jsType = Optional.ofNullable(processorNode.getJsType()).orElse(ProcessorNodeType.DEFAULT.type());
			finalJs = !standard && ProcessorNodeType.Standard_JS.contrast(jsType);
		}

		List<JavaScriptFunctions> javaScriptFunctions;
		if (standard) {
			javaScriptFunctions = null;
		} else {
			javaScriptFunctions = clientMongoOperator.find(new Query(where("type").ne("system")).with(Sort.by(Sort.Order.asc("last_update"))),
					ConnectorConstant.JAVASCRIPT_FUNCTION_COLLECTION, JavaScriptFunctions.class);
		}

		ScriptCacheService scriptCacheService = new ScriptCacheService(clientMongoOperator, (DataProcessorContext) processorBaseContext);
		this.engine = finalJs ?
			getScriptEngine(
					JSEngineEnum.GRAALVM_JS.getEngineName(),
					script,
					javaScriptFunctions,
					clientMongoOperator,
					null,
					null,
					scriptCacheService,
					new ObsScriptLogger(obsLogger, logger),
					this.standard)
			: ScriptUtil.getScriptEngine(
				JSEngineEnum.GRAALVM_JS.getEngineName(),
				script,
				javaScriptFunctions,
				clientMongoOperator,
				null,
				null,
				scriptCacheService,
				new ObsScriptLogger(obsLogger, logger),
				this.standard);

		this.processContextThreadLocal = ThreadLocal.withInitial(HashMap::new);
	}

	public Invocable getScriptEngine(
		String jsEngineName,
		String script,
		List<JavaScriptFunctions> javaScriptFunctions,
		ClientMongoOperator clientMongoOperator,
		ScriptConnection source,
		ScriptConnection target,
		ICacheGetter memoryCacheGetter,
		Log logger,
		boolean standard
	) throws ScriptException {
		if (StringUtils.isBlank(script)) {
			script = "function process(record){\n\treturn record;\n}";
		}
		ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
		try {
			final ScriptFactory scriptFactory = InstanceFactory.instance(ScriptFactory.class, "tapdata");
			ScriptEngine e = scriptFactory.create(ScriptFactory.TYPE_JAVASCRIPT, new ScriptOptions().engineName(jsEngineName));
			final ClassLoader[] externalClassLoader = new ClassLoader[1];
			String buildInMethod = initBuildInMethod(javaScriptFunctions, clientMongoOperator, urlClassLoader -> externalClassLoader[0] = urlClassLoader, standard);
			Optional.ofNullable(externalClassLoader[0]).ifPresent(s -> Thread.currentThread().setContextClassLoader(s));
			if (Thread.currentThread().getContextClassLoader() == null) {
				Thread.currentThread().setContextClassLoader(ScriptUtil.class.getClassLoader());
			}
			String scripts = buildInMethod + System.lineSeparator() + script;

			e.put("tapUtil", new JsUtil());
			e.put("tapLog", logger);
			evalJs(e, "js/csvUtils.js");
			evalJs(e, "js/arrayUtils.js");
			evalJs(e, "js/dateUtils.js");
			evalJs(e, "js/exceptionUtils.js");
			evalJs(e, "js/stringUtils.js");
			evalJs(e, "js/mapUtils.js");
			evalJs(e, "js/log.js");

			try {
				e.eval(scripts);
			} catch (Throwable ex) {
				throw new CoreException(String.format("Incorrect JS code, syntax error found: %s, please check your javascript code", ex.getMessage()));
			}
			Optional.ofNullable(source).ifPresent(s -> e.put("source", s));
			Optional.ofNullable(target).ifPresent(s -> e.put("target", s));
			Optional.ofNullable(memoryCacheGetter).ifPresent(s -> e.put("CacheService", s));
			Optional.ofNullable(logger).ifPresent(s -> e.put("log", s));
			return (Invocable) e;
		} finally {
			Thread.currentThread().setContextClassLoader(contextClassLoader);
		}
	}

	@Override
	protected void doInit(@NotNull Context context) throws Exception {
		super.doInit(context);
		Node node = getNode();
		if (!this.standard) {
			this.scriptExecutorsManager = new ScriptExecutorsManager(new ObsScriptLogger(obsLogger), clientMongoOperator, jetContext.hazelcastInstance(),
					node.getTaskId(), node.getId(),
					StringUtils.equalsAnyIgnoreCase(processorBaseContext.getTaskDto().getSyncType(),
							TaskDto.SYNC_TYPE_TEST_RUN, TaskDto.SYNC_TYPE_DEDUCE_SCHEMA));
			((ScriptEngine) this.engine).put("ScriptExecutorsManager", scriptExecutorsManager);

			List<Node<?>> predecessors = GraphUtil.predecessors(node, Node::isDataNode);
			List<Node<?>> successors = GraphUtil.successors(node, Node::isDataNode);

			this.source = getDefaultScriptExecutor(predecessors, "source");
			this.target = getDefaultScriptExecutor(successors, "target");
			((ScriptEngine) this.engine).put("source", source);
			((ScriptEngine) this.engine).put("target", target);
		}

	}

	private ScriptExecutorsManager.ScriptExecutor getDefaultScriptExecutor(List<Node<?>> nodes, String flag) {
		if (nodes != null && nodes.size() > 0) {
			Node<?> node = nodes.get(0);
			if (node instanceof DataParentNode) {
				String connectionId = ((DataParentNode) node).getConnectionId();
				Connections connections = clientMongoOperator.findOne(new Query(where("_id").is(connectionId)),
						ConnectorConstant.CONNECTION_COLLECTION, Connections.class);
				if (connections != null) {
					if (nodes.size() > 1) {
						obsLogger.warn("Use the first node as the default script executor, please use it with caution.");
					}
					return this.scriptExecutorsManager.create(connections, clientMongoOperator, jetContext.hazelcastInstance(), new ObsScriptLogger(obsLogger));
				}
			}
		}
		obsLogger.warn("The " + flag + " could not build the executor, please check");
		return null;
	}

	@SneakyThrows
	@Override
	protected void tryProcess(TapdataEvent tapdataEvent, BiConsumer<TapdataEvent, ProcessResult> consumer) {
		TapEvent tapEvent = tapdataEvent.getTapEvent();
		String tableName = TapEventUtil.getTableId(tapEvent);
		ProcessResult processResult = getProcessResult(tableName);

		if (!(tapEvent instanceof TapRecordEvent)) {
			consumer.accept(tapdataEvent, processResult);
			return;
		}

		Map<String, Object> record = TapEventUtil.getAfter(tapEvent);
		if (MapUtils.isEmpty(record) && MapUtils.isNotEmpty(TapEventUtil.getBefore(tapEvent))) {
			record = TapEventUtil.getBefore(tapEvent);
		}

		String op = TapEventUtil.getOp(tapEvent);
		ProcessContext processContext = new ProcessContext(op, tableName, null, null, null, tapdataEvent.getOffset());

		Long referenceTime = ((TapRecordEvent) tapEvent).getReferenceTime();
		long eventTime = referenceTime == null ? 0 : referenceTime;
		processContext.setEventTime(eventTime);
		processContext.setTs(eventTime);
		SyncStage syncStage = tapdataEvent.getSyncStage();
		processContext.setType(syncStage == null ? SyncStage.INITIAL_SYNC.name() : syncStage.name());
		processContext.setSyncType(getProcessorBaseContext().getTaskDto().getSyncType());

		ProcessContextEvent processContextEvent = processContext.getEvent();
		if (processContextEvent == null) {
			processContextEvent = new ProcessContextEvent(op, tableName, processContext.getSyncType(), eventTime);
		}
		Map<String, Object> before = TapEventUtil.getBefore(tapEvent);
		if (null != before) {
			processContextEvent.setBefore(before);
		}
		processContextEvent.setType(processContext.getType());
		Map<String, Object> eventMap = MapUtil.obj2Map(processContextEvent);
		Map<String, Object> contextMap = MapUtil.obj2Map(processContext);
		contextMap.put("event", eventMap);
		contextMap.put("before", before);
		contextMap.put("info", tapEvent.getInfo());
		Map<String, Object> context = this.processContextThreadLocal.get();
		context.putAll(contextMap);
		((ScriptEngine) this.engine).put("context", context);
		AtomicReference<Object> scriptInvokeResult = new AtomicReference<>();
		if (StringUtils.equalsAnyIgnoreCase(processorBaseContext.getTaskDto().getSyncType(),
				TaskDto.SYNC_TYPE_TEST_RUN,
				TaskDto.SYNC_TYPE_DEDUCE_SCHEMA)) {
			Map<String, Object> finalRecord = record;
			CountDownLatch countDownLatch = new CountDownLatch(1);
			AtomicReference<Throwable> errorAtomicRef = new AtomicReference<>();
			Thread thread = new Thread(() -> {
				Thread.currentThread().setName("Javascript-Test-Runner");
				try {
					scriptInvokeResult.set(engine.invokeFunction(ScriptUtil.FUNCTION_NAME, finalRecord));
				} catch (Throwable throwable) {
					errorAtomicRef.set(throwable);
				} finally {
					countDownLatch.countDown();
				}
			});
			thread.start();
			boolean threadFinished = countDownLatch.await(10L, TimeUnit.SECONDS);
			if (!threadFinished) {
				thread.stop();
			}
			if (errorAtomicRef.get() != null) {
				throw new TapCodeException(TaskProcessorExCode_11.JAVA_SCRIPT_PROCESS_FAILED, errorAtomicRef.get());
			}

		} else {
			scriptInvokeResult.set(engine.invokeFunction(ScriptUtil.FUNCTION_NAME, record));
		}

		if (StringUtils.isNotEmpty((CharSequence) context.get("op"))) {
			op = (String) context.get("op");
		}

		context.clear();

		if (null == scriptInvokeResult.get()) {
			if (logger.isDebugEnabled()) {
				logger.debug("The event does not need to continue to be processed {}", tapdataEvent);
			}
		} else if (scriptInvokeResult.get() instanceof List) {
			for (Object o : (List) scriptInvokeResult.get()) {
				Map<String, Object> recordMap = new HashMap<>();
				MapUtil.copyToNewMap((Map<String, Object>) o, recordMap);
				TapdataEvent cloneTapdataEvent = (TapdataEvent) tapdataEvent.clone();
				TapEvent returnTapEvent = getTapEvent(cloneTapdataEvent.getTapEvent(), op);
				setRecordMap(returnTapEvent, op, recordMap);
				cloneTapdataEvent.setTapEvent(returnTapEvent);
				consumer.accept(cloneTapdataEvent, processResult);
			}
		} else {
			Map<String, Object> recordMap = new HashMap<>();
			MapUtil.copyToNewMap((Map<String, Object>) scriptInvokeResult.get(), recordMap);
			TapEvent returnTapEvent = getTapEvent(tapEvent, op);
			setRecordMap(returnTapEvent, op, recordMap);
			tapdataEvent.setTapEvent(returnTapEvent);
			consumer.accept(tapdataEvent, processResult);
		}
	}

	private TapEvent getTapEvent(TapEvent tapEvent, String op) {
		if (StringUtils.equals(TapEventUtil.getOp(tapEvent), op)) {
			return tapEvent;
		}
		OperationType operationType = OperationType.fromOp(op);
		TapEvent result;

		switch (operationType) {
			case INSERT:
				result = TapInsertRecordEvent.create();
				break;
			case UPDATE:
				result = TapUpdateRecordEvent.create();
				break;
			case DELETE:
				result = TapDeleteRecordEvent.create();
				break;
			default:
				throw new IllegalArgumentException("Unsupported operation type: " + op);
		}
		tapEvent.clone(result);

		return result;
	}

	private static void setRecordMap(TapEvent tapEvent, String op, Map<String, Object> recordMap) {
		if (ConnectorConstant.MESSAGE_OPERATION_DELETE.equals(op)) {
			TapEventUtil.setBefore(tapEvent, recordMap);
		} else {
			TapEventUtil.setAfter(tapEvent, recordMap);
		}
	}

	@Override
	protected void doClose() throws Exception {
		try {
			CommonUtils.ignoreAnyError(() -> Optional.ofNullable(this.source).ifPresent(ScriptExecutorsManager.ScriptExecutor::close), TAG);
			CommonUtils.ignoreAnyError(() -> Optional.ofNullable(this.target).ifPresent(ScriptExecutorsManager.ScriptExecutor::close), TAG);
			CommonUtils.ignoreAnyError(() -> Optional.ofNullable(this.scriptExecutorsManager).ifPresent(ScriptExecutorsManager::close), TAG);
			CommonUtils.ignoreAnyError(() -> {
				if (this.engine instanceof GraalJSScriptEngine) {
					((GraalJSScriptEngine) this.engine).close();
				}
			}, TAG);
		} finally {
			super.doClose();
		}
	}

	public String initBuildInMethod(List<JavaScriptFunctions> javaScriptFunctions, ClientMongoOperator clientMongoOperator, Consumer<URLClassLoader> consumer, boolean standard) {
		StringBuilder buildInMethod = new StringBuilder();

		//Expired, will be ignored in the near future
		buildInMethod.append("var DateUtil = Java.type(\"com.tapdata.constant.DateUtil\");\n");
		buildInMethod.append("var UUIDGenerator = Java.type(\"com.tapdata.constant.UUIDGenerator\");\n");
		buildInMethod.append("var idGen = Java.type(\"com.tapdata.constant.UUIDGenerator\");\n");
		buildInMethod.append("var HashMap = Java.type(\"java.util.HashMap\");\n");
		buildInMethod.append("var ArrayList = Java.type(\"java.util.ArrayList\");\n");
		buildInMethod.append("var uuid = UUIDGenerator.uuid;\n");
		buildInMethod.append("var JSONUtil = Java.type('com.tapdata.constant.JSONUtil');\n");
		buildInMethod.append("var HanLPUtil = Java.type(\"com.tapdata.constant.HanLPUtil\");\n");
		buildInMethod.append("var split_chinese = HanLPUtil.hanLPParticiple;\n");
		buildInMethod.append("var util = Java.type(\"com.tapdata.processor.util.Util\");\n");
		buildInMethod.append("var MD5Util = Java.type(\"com.tapdata.constant.MD5Util\");\n");
		buildInMethod.append("var MD5 = function(str){return MD5Util.crypt(str, true);};\n");
		buildInMethod.append("var Collections = Java.type(\"java.util.Collections\");\n");
		buildInMethod.append("var MapUtils = Java.type(\"com.tapdata.constant.MapUtil\");\n");


		buildInMethod.append("var sleep = function(ms){\n" +
				"var Thread = Java.type(\"java.lang.Thread\");\n" +
				"Thread.sleep(ms);\n" +
				"}\n");
		if (standard) {
			return buildInMethod.toString();
		}
		buildInMethod.append("var networkUtil = Java.type(\"com.tapdata.constant.NetworkUtil\");\n");
		buildInMethod.append("var rest = Java.type(\"com.tapdata.processor.util.CustomRest\");\n");
		buildInMethod.append("var httpUtil = Java.type(\"cn.hutool.http.HttpUtil\");\n");
		buildInMethod.append("var tcp = Java.type(\"com.tapdata.processor.util.CustomTcp\");\n");
		buildInMethod.append("var mongo = Java.type(\"com.tapdata.processor.util.CustomMongodb\");\n");

		if (CollectionUtils.isNotEmpty(javaScriptFunctions)) {
			List<URL> urlList = new ArrayList<>();
			for (JavaScriptFunctions javaScriptFunction : javaScriptFunctions) {
				if (javaScriptFunction.isSystem()) {
					continue;
				}
				String jsFunction = javaScriptFunction.getJSFunction();
				if (StringUtils.isNotBlank(jsFunction)) {
					buildInMethod.append(jsFunction).append("\n");
					if (javaScriptFunction.isJar() && AppType.init().isDaas()) {
						//定义类加载器
						String fileId = javaScriptFunction.getFileId();
						final Path filePath = Paths.get(System.getenv("TAPDATA_WORK_DIR"), "lib", fileId);
						if (Files.notExists(filePath)) {
							if (clientMongoOperator instanceof HttpClientMongoOperator) {
								File file = ((HttpClientMongoOperator) clientMongoOperator).downloadFile(null, "file/" + fileId, filePath.toString(), true);
								if (null == file) {
									throw new RuntimeException("not found");
								}
							} else {
								GridFSBucket gridFSBucket = clientMongoOperator.getGridFSBucket();
								try (GridFSDownloadStream gridFSDownloadStream = gridFSBucket.openDownloadStream(new ObjectId(javaScriptFunction.getFileId()))) {
									if (Files.notExists(filePath.getParent())) {
										Files.createDirectories(filePath.getParent());
									}
									Files.createFile(filePath);
									Files.copy(gridFSDownloadStream, filePath, StandardCopyOption.REPLACE_EXISTING);
								} catch (Exception e) {
									throw new RuntimeException(String.format("create function jar file '%s' error: %s", filePath, e.getMessage()), e);
								}
							}
						}
						try {
							URL url = filePath.toUri().toURL();
							urlList.add(url);
						} catch (Exception e) {
							throw new RuntimeException(String.format("create function jar file '%s' error: %s", filePath, e.getMessage()), e);
						}
					}
				}
			}
			if (CollectionUtils.isNotEmpty(urlList)) {
				logger.debug("urlClassLoader will load: {}", urlList);
				final URLClassLoader urlClassLoader = new ScriptUtil.CustomerClassLoader(urlList.toArray(new URL[0]), ScriptUtil.class.getClassLoader());
				if (consumer != null) {
					consumer.accept(urlClassLoader);
				}
			}
		}
		return buildInMethod.toString();
	}

	public static void main(String[] args)throws FileNotFoundException, ScriptException {
		final ScriptFactory scriptFactory = InstanceFactory.instance(ScriptFactory.class, "tapdata");
		ScriptEngine e = scriptFactory.create(ScriptFactory.TYPE_JAVASCRIPT, new ScriptOptions().engineName(JSEngineEnum.GRAALVM_JS.getEngineName()));
		e.put("tapUtil", new JsUtil());
		e.put("tapLog", new TapLog());
		e.eval(new FileReader("iengine/iengine-app/src/main/resources/js/csvUtils.js"));
		e.eval(new FileReader("iengine/iengine-app/src/main/resources/js/arrayUtils.js"));
		e.eval(new FileReader("iengine/iengine-app/src/main/resources/js/dateUtils.js"));
		e.eval(new FileReader("iengine/iengine-app/src/main/resources/js/exceptionUtils.js"));
		e.eval(new FileReader("iengine/iengine-app/src/main/resources/js/stringUtils.js"));
		e.eval(new FileReader("iengine/iengine-app/src/main/resources/js/mapUtils.js"));
		e.eval(new FileReader("iengine/iengine-app/src/main/resources/js/log.js"));

		Object invoker = e.eval("dateUtils.timeStamp2Date(new Date().getTime(), \"yyyy-MM-dd'T'HH:mm:ssXXX\");");
		System.out.println(invoker + " ---- " + new JsUtil().timeStamp2Date(System.currentTimeMillis(), "yyyy-MM-dd'T'HH:mm:ssXXX"));
		e.eval("log.warn(\"Hello Log, i'm %s\", 'Gavin');");
	}

	private void evalJs(ScriptEngine engine, String fileClassPath){
		try {
			ClassPathResource classPathResource = new ClassPathResource(fileClassPath);
			engine.eval(IOUtils.toString(classPathResource.getInputStream(), StandardCharsets.UTF_8));
		}catch (Throwable ex){
			throw new RuntimeException(String.format("script eval js util error: %s, %s", fileClassPath, ex.getMessage()), ex);
		}
	}
}
