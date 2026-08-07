package io.tapdata.flow.engine.V2.node.hazelcast.processor;

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
import com.tapdata.processor.error.ScriptProcessorExCode_30;
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
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.script.ObsScriptLogger;
import io.tapdata.flow.engine.V2.script.ScriptExecutorsManager;
import io.tapdata.threadgroup.CpuMemoryCollector;
import io.tapdata.flow.engine.V2.util.GraphUtil;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import io.tapdata.observable.metric.handler.HandlerUtil;
import io.tapdata.pdk.core.utils.CommonUtils;
import lombok.SneakyThrows;
import org.apache.commons.collections4.CollectionUtils;
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
import java.io.Closeable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.springframework.data.mongodb.core.query.Criteria.where;

public class HazelcastJavaScriptProcessorNode extends HazelcastProcessorBaseNode {

	private static final Logger logger = LogManager.getLogger(HazelcastJavaScriptProcessorNode.class);
	public static final String TAG = HazelcastJavaScriptProcessorNode.class.getSimpleName();
	public static final String BEFORE = "before";

	private ScriptExecutorsManager scriptExecutorsManager;

	private ThreadLocal<Map<String, Object>> processContextThreadLocal;
	private Map<String, Object> globalTaskContent;

	/**
	 * standard js
	 */
	private boolean standard;

	private boolean finalJs = false;

	private volatile boolean sharedInitDone = false;
	private String script;
	private List<JavaScriptFunctions> javaScriptFunctions;
	private ScriptCacheService scriptCacheService;

	private final Map<String, Invocable> engineMap;
	private final Map<String, ScriptExecutorsManager.ScriptExecutor> sourceMap;
	private final Map<String, ScriptExecutorsManager.ScriptExecutor> targetMap;

	@SneakyThrows
	public HazelcastJavaScriptProcessorNode(ProcessorBaseContext processorBaseContext) {
		super(processorBaseContext);
		this.engineMap = new ConcurrentHashMap<>();
		this.sourceMap = new ConcurrentHashMap<>();
		this.targetMap = new ConcurrentHashMap<>();
	}

	@Override
	protected void doInit(@NotNull Context context) throws TapCodeException {
		super.doInit(context);
		this.processContextThreadLocal = ThreadLocal.withInitial(HashMap::new);
		this.globalTaskContent = new ConcurrentHashMap<>();
	}

	protected Invocable getOrInitEngine() {
		ensureSharedInit();
		String engineKey = getNode().getId() + "-" + Thread.currentThread().getId();
		return engineMap.computeIfAbsent(engineKey, k -> buildEngine());
	}

	private void closeEngine(Invocable engine) {
		if (!(engine instanceof Closeable)) {
			return;
		}
		try {
			((Closeable) engine).close();
		} catch (Exception e) {
			logger.warn("Error closing script engine", e);
		}
	}

	private static TapCodeException wrapScriptProcessException(Throwable throwable) {
		Throwable cause = throwable.getCause() == null ? throwable : throwable.getCause();
		String message = StringUtils.defaultIfBlank(
				throwable.getMessage(),
				StringUtils.defaultIfBlank(cause.getMessage(), cause.getClass().getName())
		);
		return new TapCodeException(
				ScriptProcessorExCode_30.JAVA_SCRIPT_PROCESS_FAILED,
				message,
				cause
		).dynamicDescriptionParameters(message);
	}

	private synchronized void ensureSharedInit() {
		if (sharedInitDone) {
			return;
		}
		Node<?> node = getNode();
		if (node instanceof JsProcessorNode) {
			this.script = ((JsProcessorNode) node).getScript();
		} else if (node instanceof MigrateJsProcessorNode) {
			this.script = ((MigrateJsProcessorNode) node).getScript();
		} else if (node instanceof CacheLookupProcessorNode) {
			this.script = ((CacheLookupProcessorNode) node).getScript();
		} else {
			throw new RuntimeException("unsupported node " + node.getClass().getName());
		}

		if (node instanceof StandardJsProcessorNode || node instanceof StandardMigrateJsProcessorNode) {
			this.standard = true;
		}
		if (node instanceof JsProcessorNode) {
			JsProcessorNode processorNode = (JsProcessorNode) node;
			int jsType = Optional.ofNullable(processorNode.getJsType()).orElse(ProcessorNodeType.DEFAULT.type());
			finalJs = !standard && ProcessorNodeType.Standard_JS.contrast(jsType);
		} else if (node instanceof MigrateJsProcessorNode) {
			MigrateJsProcessorNode processorNode = (MigrateJsProcessorNode) node;
			int jsType = Optional.ofNullable(processorNode.getJsType()).orElse(ProcessorNodeType.DEFAULT.type());
			finalJs = !standard && ProcessorNodeType.Standard_JS.contrast(jsType);
		}

		this.javaScriptFunctions = standard ?
				null
				: clientMongoOperator.find(new Query(where("type")
						.ne("system"))
						.with(Sort.by(Sort.Order.asc("last_update"))),
				ConnectorConstant.JAVASCRIPT_FUNCTION_COLLECTION,
				JavaScriptFunctions.class
		);

		this.scriptCacheService = new ScriptCacheService(clientMongoOperator, (DataProcessorContext) processorBaseContext);

		if (!this.standard) {
			this.scriptExecutorsManager = new ScriptExecutorsManager(new ObsScriptLogger(getScriptObsLogger()), clientMongoOperator, jetContext.hazelcastInstance(),
					node.getTaskId(), node.getId(),
					!processorBaseContext.getTaskDto().isNormalTask()
			);
			String nodeId = node.getId();
			List<Node<?>> predecessors = GraphUtil.predecessors(node, Node::isDataNode);
			List<Node<?>> successors = GraphUtil.successors(node, Node::isDataNode);
			sourceMap.computeIfAbsent(nodeId, k -> getDefaultScriptExecutor(predecessors, SOURCE_TAG));
			targetMap.computeIfAbsent(nodeId, k -> getDefaultScriptExecutor(successors, TARGET_TAG));
		}

		this.sharedInitDone = true;
	}

	Invocable buildEngine() {
		Node<?> node = getNode();
		ObsScriptLogger obsScriptLogger;
		if (getProcessorBaseContext().getTaskDto().isNormalTask()) {
			obsScriptLogger = new ObsScriptLogger(getScriptObsLogger(), logger);
		} else {
			obsScriptLogger = new ObsScriptLogger(obsLogger, logger);
		}
		Invocable engine;
		try {
			engine = finalJs ?
					ScriptStandardizationUtil.getScriptStandardizationEngine(
							JSEngineEnum.GRAALVM_JS.getEngineName(),
							script,
							javaScriptFunctions,
							clientMongoOperator,
							scriptCacheService,
							obsScriptLogger,
							this.standard)
					: ScriptUtil.getScriptEngine(
					JSEngineEnum.GRAALVM_JS.getEngineName(),
					script,
					javaScriptFunctions,
					clientMongoOperator,
					null,
					null,
					scriptCacheService,
					obsScriptLogger,
					this.standard);
		} catch (ScriptException e) {
			throw new TapCodeException(ScriptProcessorExCode_30.JAVA_SCRIPT_PROCESSOR_GET_SCRIPT_FAILED, e)
					.dynamicDescriptionParameters(e.getMessage());
		}
		if (!this.standard) {
			((ScriptEngine) engine).put("ScriptExecutorsManager", scriptExecutorsManager);
			((ScriptEngine) engine).put(SOURCE_TAG, sourceMap.get(node.getId()));
			((ScriptEngine) engine).put(TARGET_TAG, targetMap.get(node.getId()));
		}
		((ScriptEngine) engine).put("env", getTaskEnvReadMap());
		return engine;
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
					return this.scriptExecutorsManager.create(connections, clientMongoOperator, jetContext.hazelcastInstance(), new ObsScriptLogger(getScriptObsLogger()));
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
			afterMapInRecord = new HashMap<>();
			MapUtil.copyToNewMap(TapEventUtil.getBefore(tapEvent), afterMapInRecord);
		}
		String op = TapEventUtil.getOp(tapEvent);
		Map<String, Object> before = TapEventUtil.getBefore(tapEvent);
		Map<String, Object> context = buildContextMap(tapdataEvent, tapEvent, before, this.globalTaskContent, this.processContextThreadLocal);
		ensureSharedInit();

		AtomicReference<Object> scriptInvokeResult = new AtomicReference<>();
		AtomicReference<Object> scriptInvokeBeforeResult = new AtomicReference<>();
		if (!processorBaseContext.getTaskDto().isNormalTask()) {
			Map<String, Object> finalRecord = afterMapInRecord;
			CountDownLatch countDownLatch = new CountDownLatch(1);
			AtomicReference<Throwable> errorAtomicRef = new AtomicReference<>();
			Thread thread = new Thread(() -> {
				Thread.currentThread().setName("Javascript-Test-Runner");
				Invocable runnerEngine = null;
				try {
					// A GraalJS Context is single-threaded. Create, bind, invoke, and close
					// the runner engine on this thread instead of sharing a worker-owned Context.
					runnerEngine = buildEngine();
					((ScriptEngine) runnerEngine).put("context", context);
					scriptInvokeResult.set(runnerEngine.invokeFunction(ScriptUtil.FUNCTION_NAME, finalRecord));
				} catch (Throwable throwable) {
					errorAtomicRef.set(throwable);
				} finally {
					closeEngine(runnerEngine);
					countDownLatch.countDown();
				}
			});
			thread.start();
			boolean threadFinished = countDownLatch.await(10L, TimeUnit.SECONDS);
			if (!threadFinished) {
				thread.interrupt();
				throw new TapCodeException(
						ScriptProcessorExCode_30.JAVA_SCRIPT_PROCESS_FAILED,
						"JavaScript execution timed out after 10 seconds"
				);
			}
			if (errorAtomicRef.get() != null) {
				throw wrapScriptProcessException(errorAtomicRef.get());
			}

		} else {
			Invocable engine = getOrInitEngine();
			((ScriptEngine) engine).put("context", context);
			try {
				scriptInvokeResult.set(engine.invokeFunction(ScriptUtil.FUNCTION_NAME, afterMapInRecord));
				// handle before
				if (standard && TapUpdateRecordEvent.TYPE == tapEvent.getType() && MapUtils.isNotEmpty(before)) {
					scriptInvokeBeforeResult.set(engine.invokeFunction(ScriptUtil.FUNCTION_NAME, before));
				}
			} catch (Exception e) {
				throw wrapScriptProcessException(e);
			}
		}

		if (StringUtils.isNotEmpty((CharSequence) context.get("op"))) {
			op = (String) context.get("op");
		}

		Map<String, Object> contextBefore = (Map<String, Object>) context.get("before");
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
				CpuMemoryCollector.listening(getNode().getId(), cloneTapdataEvent);
				TapEvent returnTapEvent = getTapEvent(cloneTapdataEvent.getTapEvent(), op, contextBefore);
				setRecordMap(returnTapEvent, op, recordMap);
				flushBeforeIfNeed(scriptInvokeBeforeResult.get(), returnTapEvent, beforeIndex);
				cloneTapdataEvent.setTapEvent(returnTapEvent);
				consumer.accept(cloneTapdataEvent, processResult);
				beforeIndex++;
			}
		} else {
			Map<String, Object> recordMap = new HashMap<>();
			MapUtil.copyToNewMap((Map<String, Object>) scriptInvokeResult.get(), recordMap);
			TapEvent returnTapEvent = getTapEvent(tapEvent, op, contextBefore);
			CpuMemoryCollector.listening(getNode().getId(), returnTapEvent);
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
				Map<String, Object> recordMap = new HashMap<>();
				MapUtil.copyToNewMap((Map<String, Object>) beforeMap, recordMap);
				TapEventUtil.setBefore(returnTapEvent, recordMap);
			}
		} else if (processedBefore instanceof Map) {
			Map<String, Object> recordMap = new HashMap<>();
			MapUtil.copyToNewMap((Map<String, Object>) processedBefore, recordMap);
			TapEventUtil.setBefore(returnTapEvent, recordMap);
		}
	}

    private TapEvent getTapEvent(TapEvent tapEvent, String op, Map<String, Object> before) {
        if (StringUtils.equals(TapEventUtil.getOp(tapEvent), op)) {
            return tapEvent;
        }
        OperationType operationType = OperationType.fromOp(op);
        TapEvent result;

        if (operationType == null) {
            throw new IllegalArgumentException("Unsupported operation type: " + op);
        }
        switch (operationType) {
            case INSERT:
                result = TapInsertRecordEvent.create();
                tapEvent.clone(result);
                break;
            case UPDATE:
                result = TapUpdateRecordEvent.create();
                tapEvent.clone(result);
				if (before != null) {
					if (StringUtils.equals(TapEventUtil.getOp(tapEvent), OperationType.INSERT.getOp())) {
						TapEventUtil.setBefore(result, before);
					} else {
						TapEventUtil.setAfter(result, before);
					}
				}
                break;
            case DELETE:
                result = TapDeleteRecordEvent.create();
                tapEvent.clone(result);
                break;
            default:
                throw new IllegalArgumentException("Unsupported operation type: " + op);
        }

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
			// Close and clear source executors
			CommonUtils.ignoreAnyError(() -> {
				if (this.sourceMap != null) {
					this.sourceMap.values().forEach(executor -> {
						if (executor != null) {
							executor.close();
						}
					});
					this.sourceMap.clear();
				}
			}, TAG);

			// Close and clear target executors
			CommonUtils.ignoreAnyError(() -> {
				if (this.targetMap != null) {
					this.targetMap.values().forEach(executor -> {
						if (executor != null) {
							executor.close();
						}
					});
					this.targetMap.clear();
				}
			}, TAG);

			// Close script executors manager
			CommonUtils.ignoreAnyError(() -> {
				if (this.scriptExecutorsManager != null) {
					this.scriptExecutorsManager.close();
					this.scriptExecutorsManager = null;
				}
			}, TAG);

			// Close and clear script engines (per-thread engines: GraalJSScriptEngine or Closeable TapJavaScriptEngine)
			CommonUtils.ignoreAnyError(() -> {
				if (this.engineMap != null) {
					this.engineMap.values().forEach(this::closeEngine);
					this.engineMap.clear();
				}
			}, TAG);

			// Clean up ThreadLocal to prevent memory leaks
			CommonUtils.ignoreAnyError(() -> {
				if (this.processContextThreadLocal != null) {
					// Clear the current thread's value first
					Map<String, Object> context = this.processContextThreadLocal.get();
					if (context != null) {
						context.clear();
					}
					// Remove the ThreadLocal reference
					this.processContextThreadLocal.remove();
					this.processContextThreadLocal = null;
				}
			}, TAG);

			// Clear global task content
			CommonUtils.ignoreAnyError(() -> {
				if (this.globalTaskContent != null) {
					this.globalTaskContent.clear();
					this.globalTaskContent = null;
				}
			}, TAG);

			if (logger.isDebugEnabled()) {
				logger.debug("JavaScript processor node resources cleaned up successfully");
			}

		} finally {
			super.doClose();
		}
	}

	@Override
	public boolean supportConcurrentProcess() {
		return true;
	}

	@Override
	protected void reCalcMemorySize(List<BatchEventWrapper> tapdataEvents) {
		if (CollectionUtils.isEmpty(tapdataEvents)) {
			return;
		}
		List<TapEvent> tapEvents = tapdataEvents.stream().map(bew->{
			if (null != bew && null != bew.getTapdataEvent() && null != bew.getTapdataEvent().getTapEvent()) {
				return bew.getTapdataEvent().getTapEvent();
			}
			return null;
		}).collect(Collectors.toList());
		HandlerUtil.sampleMemoryToTapEvent(tapEvents);
	}

	@Override
	protected void handleTransformToTapValueResult(TapdataEvent tapdataEvent) {
		tapdataEvent.setTransformToTapValueResult(null);
	}

	@Override
	public boolean needCopyBatchEventWrapper() {
		return true;
	}
}
