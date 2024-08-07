package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import com.oracle.truffle.js.scriptengine.GraalJSScriptEngine;
import com.tapdata.cache.scripts.ScriptCacheService;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.MapUtil;
import com.tapdata.entity.Connections;
import com.tapdata.entity.JavaScriptFunctions;
import com.tapdata.entity.OperationType;
import com.tapdata.entity.SyncStage;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.processor.ScriptUtil;
import com.tapdata.processor.constant.JSEngineEnum;
import com.tapdata.processor.context.ProcessContext;
import com.tapdata.processor.context.ProcessContextEvent;
import com.tapdata.processor.standard.ScriptStandardizationUtil;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.process.CacheLookupProcessorNode;
import com.tapdata.tm.commons.dag.process.JsProcessorNode;
import com.tapdata.tm.commons.dag.process.MigrateJsProcessorNode;
import com.tapdata.tm.commons.dag.process.StandardJsProcessorNode;
import com.tapdata.tm.commons.dag.process.StandardMigrateJsProcessorNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.ProcessorNodeType;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.error.TaskProcessorExCode_11;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.script.ObsScriptLogger;
import io.tapdata.flow.engine.V2.script.ScriptExecutorsManager;
import io.tapdata.flow.engine.V2.util.GraphUtil;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import io.tapdata.pdk.core.utils.CommonUtils;
import lombok.SneakyThrows;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Query;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static org.springframework.data.mongodb.core.query.Criteria.where;

public class HazelcastJavaScriptProcessorNode extends HazelcastProcessorBaseNode {

	private static final Logger logger = LogManager.getLogger(HazelcastJavaScriptProcessorNode.class);
	public static final String TAG = HazelcastJavaScriptProcessorNode.class.getSimpleName();
	public static final String BEFORE = "before";

	private Invocable engine;

	private ScriptExecutorsManager scriptExecutorsManager;

	private ThreadLocal<Map<String, Object>> processContextThreadLocal;
	private Map<String, Object> globalTaskContent;
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
	}

	@Override
	protected void doInit(@NotNull Context context) throws TapCodeException {
		super.doInit(context);
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
		} else if (node instanceof MigrateJsProcessorNode){
			MigrateJsProcessorNode processorNode = (MigrateJsProcessorNode)node;
			int jsType = Optional.ofNullable(processorNode.getJsType()).orElse(ProcessorNodeType.DEFAULT.type());
			finalJs = !standard && ProcessorNodeType.Standard_JS.contrast(jsType);
		}

		List<JavaScriptFunctions> javaScriptFunctions = standard ?
				null
				: clientMongoOperator.find( new Query(where("type")
						.ne("system"))
						.with(Sort.by(Sort.Order.asc("last_update"))),
				ConnectorConstant.JAVASCRIPT_FUNCTION_COLLECTION,
				JavaScriptFunctions.class
		);

		ScriptCacheService scriptCacheService = new ScriptCacheService(clientMongoOperator, (DataProcessorContext) processorBaseContext);
		try {
			this.engine = finalJs ?
					ScriptStandardizationUtil.getScriptStandardizationEngine(
							JSEngineEnum.GRAALVM_JS.getEngineName(),
							script,
							javaScriptFunctions,
							clientMongoOperator,
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
		} catch (ScriptException e) {
			throw new TapCodeException(TaskProcessorExCode_11.INIT_SCRIPT_ENGINE_FAILED, e);
		}
		this.processContextThreadLocal = ThreadLocal.withInitial(HashMap::new);
		this.globalTaskContent = new HashMap<>();
		if (!this.standard) {
			this.scriptExecutorsManager = new ScriptExecutorsManager(new ObsScriptLogger(obsLogger), clientMongoOperator, jetContext.hazelcastInstance(),
					node.getTaskId(), node.getId(),
					StringUtils.equalsAnyIgnoreCase(processorBaseContext.getTaskDto().getSyncType(),
							TaskDto.SYNC_TYPE_TEST_RUN, TaskDto.SYNC_TYPE_DEDUCE_SCHEMA));
			((ScriptEngine) this.engine).put("ScriptExecutorsManager", scriptExecutorsManager);
			List<Node<?>> predecessors = GraphUtil.predecessors(node, Node::isDataNode);
			List<Node<?>> successors = GraphUtil.successors(node, Node::isDataNode);

			this.source = getDefaultScriptExecutor(predecessors, SOURCE_TAG);
			this.target = getDefaultScriptExecutor(successors, TARGET_TAG);
			((ScriptEngine) this.engine).put(SOURCE_TAG, source);
			((ScriptEngine) this.engine).put(TARGET_TAG, target);
		}
	}

	private ScriptExecutorsManager.ScriptExecutor getDefaultScriptExecutor(List<Node<?>> nodes, String flag) {
		TaskDto taskDto = processorBaseContext.getTaskDto();
		if (TARGET_TAG.equals(flag) && taskDto.isTestTask()) {
			return this.scriptExecutorsManager.createDummy();
		}
		if (nodes != null && !nodes.isEmpty()) {
			Node<?> node = nodes.get(0);
			if (node instanceof DataParentNode) {
				String connectionId = ((DataParentNode<?>) node).getConnectionId();
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
		obsLogger.warn("The {} could not build the executor, please check", flag);
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

		Map<String, Object> afterMapInRecord = TapEventUtil.getAfter(tapEvent);
		if (MapUtils.isEmpty(afterMapInRecord) && MapUtils.isNotEmpty(TapEventUtil.getBefore(tapEvent))) {
			afterMapInRecord = TapEventUtil.getBefore(tapEvent);
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
		contextMap.put(BEFORE, before);
		contextMap.put("info", tapEvent.getInfo());
		contextMap.put("global", this.globalTaskContent);
		Map<String, Object> context = this.processContextThreadLocal.get();
		context.putAll(contextMap);
		((ScriptEngine) this.engine).put("context", context);


		AtomicReference<Object> scriptInvokeResult = new AtomicReference<>();
		AtomicReference<Object> scriptInvokeBeforeResult = new AtomicReference<>();
		if (StringUtils.equalsAnyIgnoreCase(processorBaseContext.getTaskDto().getSyncType(),
				TaskDto.SYNC_TYPE_TEST_RUN,
				TaskDto.SYNC_TYPE_DEDUCE_SCHEMA)) {
			Map<String, Object> finalRecord = afterMapInRecord;
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
				thread.interrupt();
			}
			if (errorAtomicRef.get() != null) {
				throw new TapCodeException(TaskProcessorExCode_11.JAVA_SCRIPT_PROCESS_FAILED, errorAtomicRef.get());
			}

		} else {
			scriptInvokeResult.set(engine.invokeFunction(ScriptUtil.FUNCTION_NAME, afterMapInRecord));
			// handle before
			if (standard && TapUpdateRecordEvent.TYPE == tapEvent.getType() && MapUtils.isNotEmpty(before)) {
				scriptInvokeBeforeResult.set(engine.invokeFunction(ScriptUtil.FUNCTION_NAME, before));
			}
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
			Integer beforeIndex = 0;
			for (Object o : (List) scriptInvokeResult.get()) {
				Map<String, Object> recordMap = new HashMap<>();
				MapUtil.copyToNewMap((Map<String, Object>) o, recordMap);
				TapdataEvent cloneTapdataEvent = (TapdataEvent) tapdataEvent.clone();
				TapEvent returnTapEvent = getTapEvent(cloneTapdataEvent.getTapEvent(), op);
				setRecordMap(returnTapEvent, op, recordMap);
				flushBeforeIfNeed(scriptInvokeBeforeResult.get(), returnTapEvent, beforeIndex);
				cloneTapdataEvent.setTapEvent(returnTapEvent);
				consumer.accept(cloneTapdataEvent, processResult);
				beforeIndex++;
			}
		} else {
			Map<String, Object> recordMap = new HashMap<>();
			MapUtil.copyToNewMap((Map<String, Object>) scriptInvokeResult.get(), recordMap);
			TapEvent returnTapEvent = getTapEvent(tapEvent, op);
			setRecordMap(returnTapEvent, op, recordMap);
			flushBeforeIfNeed(scriptInvokeBeforeResult.get(), returnTapEvent, null);
			tapdataEvent.setTapEvent(returnTapEvent);
			consumer.accept(tapdataEvent, processResult);
		}
	}

	protected void flushBeforeIfNeed (Object processedBefore, TapEvent returnTapEvent, Integer index) {
		if (null == processedBefore) return;
		if (processedBefore instanceof List && null != index) {
			if (index >= ((List<?>) processedBefore).size()) return;
			Object beforeMap = ((List<?>) processedBefore).get(index);
			if (beforeMap instanceof Map) {
				TapEventUtil.setBefore(returnTapEvent, (Map<String, Object>) beforeMap);
			}
		} else if (processedBefore instanceof Map) {
			TapEventUtil.setBefore(returnTapEvent, (Map<String, Object>) processedBefore);
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
	protected void doClose() throws TapCodeException {
		try {
			CommonUtils.ignoreAnyError(() -> Optional.ofNullable(this.source).ifPresent(ScriptExecutorsManager.ScriptExecutor::close), TAG);
			CommonUtils.ignoreAnyError(() -> Optional.ofNullable(this.target).ifPresent(ScriptExecutorsManager.ScriptExecutor::close), TAG);
			CommonUtils.ignoreAnyError(() -> Optional.ofNullable(this.scriptExecutorsManager).ifPresent(ScriptExecutorsManager::close), TAG);
			CommonUtils.ignoreAnyError(() -> {
				if (this.engine instanceof GraalJSScriptEngine) {
					((GraalJSScriptEngine) this.engine).close();
				}
			}, TAG);
			if (null != processContextThreadLocal) {
				processContextThreadLocal.remove();
			}
		} finally {
			super.doClose();
		}
	}

}
