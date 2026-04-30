package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import com.hazelcast.core.HazelcastInstance;
import com.oracle.truffle.js.scriptengine.GraalJSScriptEngine;
import com.tapdata.cache.scripts.ScriptCacheService;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.HazelcastUtil;
import com.tapdata.constant.MapUtil;
import com.tapdata.entity.*;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.processor.ScriptUtil;
import com.tapdata.processor.constant.JSEngineEnum;
import com.tapdata.processor.context.ProcessContext;
import com.tapdata.processor.context.ProcessContextEvent;
import com.tapdata.processor.error.ScriptProcessorExCode_30;
import com.tapdata.tm.commons.customNode.CustomNodeTempDto;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.process.CustomProcessorNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.error.TaskProcessorExCode_11;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.node.NodeTypeEnum;
import io.tapdata.flow.engine.V2.script.ObsScriptLogger;
import io.tapdata.flow.engine.V2.script.ScriptExecutorsManager;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import io.tapdata.pdk.core.utils.CommonUtils;
import lombok.SneakyThrows;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * @author samuel
 * @Description
 * @create 2022-03-17 13:02
 **/
public class HazelcastCustomProcessor extends HazelcastProcessorBaseNode {

	public static final String TAG = HazelcastCustomProcessor.class.getSimpleName();
	public static final String FUNCTION_NAME = "process";

	private Logger logger = LogManager.getLogger(HazelcastCustomProcessor.class);
	private Invocable engine;
	private StateMap stateMap;

	private ThreadLocal<Map<String, Object>> processContextThreadLocal;
	private Map<String, Object> globalTaskContent;
	private ScriptExecutorsManager scriptExecutorsManager;

	public HazelcastCustomProcessor(DataProcessorContext dataProcessorContext) {
		super(dataProcessorContext);
	}

	@Override
	protected void doInit(@NotNull Context context) throws TapCodeException {
		super.doInit(context);
		this.processContextThreadLocal = ThreadLocal.withInitial(HashMap::new);
		this.globalTaskContent = new ConcurrentHashMap<>();
		Node<?> node = processorBaseContext.getNode();
		if (NodeTypeEnum.get(node.getType()).equals(NodeTypeEnum.CUSTOM_PROCESSOR)) {
			String customNodeId = ((CustomProcessorNode) node).getCustomNodeId();
			Query query = new Query(Criteria.where("_id").is(customNodeId));
			CustomNodeTempDto customNodeTempDto = clientMongoOperator.findOne(query, ConnectorConstant.CUSTOMNODETEMP_COLLECTION, CustomNodeTempDto.class,
					n -> !running.get());
			if (null == customNodeTempDto) {
				throw new TapCodeException(TaskProcessorExCode_11.CUSTOM_NODE_NOT_FOUND, "Cannot find custom node template by id: " + customNodeId);
			}
			List<JavaScriptFunctions> javaScriptFunctions = clientMongoOperator.find(new Query(where("type").ne("system")).with(Sort.by(Sort.Order.asc("last_update"))),
					ConnectorConstant.JAVASCRIPT_FUNCTION_COLLECTION, JavaScriptFunctions.class, n -> !running.get());
			try {
				ScriptCacheService scriptCacheService = new ScriptCacheService(clientMongoOperator, (DataProcessorContext) processorBaseContext);
				engine = ScriptUtil.getScriptEngine(
						JSEngineEnum.GRAALVM_JS.getEngineName(),
						customNodeTempDto.getTemplate(),
						javaScriptFunctions,
						clientMongoOperator,
						null,
						null,
						scriptCacheService,
						new ObsScriptLogger(getScriptObsLogger(), logger),
						false);
				stateMap = getStateMap(context.hazelcastInstance(), node.getId());
				((ScriptEngine) engine).put("state", stateMap);
				this.scriptExecutorsManager = new ScriptExecutorsManager(new ObsScriptLogger(getScriptObsLogger()), clientMongoOperator, jetContext.hazelcastInstance(),
						node.getTaskId(), node.getId(),
						!processorBaseContext.getTaskDto().isNormalTask()
				);
				((ScriptEngine) engine).put("ScriptExecutorsManager", scriptExecutorsManager);

			} catch (ScriptException e) {
				throw new TapCodeException(ScriptProcessorExCode_30.CUSTOM_PROCESSOR_GET_SCRIPT_ENGINE_FAILED, "Init script engine failed", e)
						.dynamicDescriptionParameters(e.getMessage());
			}
		}
	}

	private static String getStateMapName(String nodeId) {
		return HazelcastCustomProcessor.class.getSimpleName() + "-" + nodeId;
	}

	protected static StateMap getStateMap(HazelcastInstance hazelcastInstance, String nodeId) {
		StateMap stateMap = new StateMap()
				/*.hazelcastInstance(hazelcastInstance)*/;
		stateMap.init(getStateMapName(nodeId), Object.class);
		return stateMap;
	}

	public static void clearStateMap(String nodeId) {
		HazelcastInstance instance = HazelcastUtil.getInstance();
		if (null == instance) {
			return;
		}
		StateMap map = getStateMap(instance, getStateMapName(nodeId));
		map.clear();
	}

	@SneakyThrows
	@Override
	protected void tryProcess(TapdataEvent tapdataEvent, BiConsumer<TapdataEvent, ProcessResult> consumer) {
		Map<String, Object> context = processContextThreadLocal.get();
		try {
			Object result = executeAndGetResult(tapdataEvent);
			String tableName = TapEventUtil.getTableId(tapdataEvent.getTapEvent());
			ProcessResult processResult = null;
			if (StringUtils.isNotEmpty(tableName)) {
				processResult = getProcessResult(tableName);
			}
			if (null == result) {
				return;
			}
			String defaultOp = resolveOp(context.get("op"), TapEventUtil.getOp(tapdataEvent.getTapEvent()));
			Map<String, Object> contextBefore = (Map<String, Object>) context.get("before");
			if (result instanceof List) {
				List<?> resultList = (List<?>) result;
				List<?> opList = getOpList(context);
				validateOpList(resultList, opList);
				for (int index = 0; index < resultList.size(); index++) {
					Map<String, Object> newMap = new HashMap<>();
					MapUtil.copyToNewMap((Map<String, Object>) resultList.get(index), newMap);
					TapdataEvent cloneTapdataEvent = (TapdataEvent) tapdataEvent.clone();
					applyResult(cloneTapdataEvent, newMap, resolveListOp(opList, index, defaultOp), contextBefore);
					consumer.accept(cloneTapdataEvent, processResult);
				}
			} else {
				Map<String, Object> newMap = new HashMap<>();
				MapUtil.copyToNewMap((Map<String, Object>) result, newMap);
				applyResult(tapdataEvent, newMap, defaultOp, contextBefore);
				consumer.accept(tapdataEvent, processResult);
			}
		} finally {
			context.clear();
		}
	}

	protected Object executeAndGetResult(TapdataEvent tapdataEvent) throws IllegalAccessException {
		Node<?> node = processorBaseContext.getNode();
		TapEvent tapEvent = tapdataEvent.getTapEvent();
		MessageEntity messageEntity = tapdataEvent.getMessageEntity();
		boolean isTapRecordEvent = true;
		Map<String, Object> after;
		Map<String, Object> before;
		if (tapEvent instanceof TapRecordEvent) {
			after = TapEventUtil.getAfter(tapEvent);
			before = TapEventUtil.getBefore(tapEvent);
		} else {
			messageEntity = tapdataEvent.getMessageEntity();
			after = messageEntity.getAfter();
			before = messageEntity.getBefore();
			isTapRecordEvent = false;
		}
		if (MapUtils.isEmpty(after) && MapUtils.isEmpty(before)) {
			return null;
		}
		Map<String, Object> record = null == after ? before : after;

		Map<String, Object> context = buildContextMap(tapdataEvent, tapEvent, before, this.globalTaskContent, this.processContextThreadLocal);

		((ScriptEngine) engine).put("context", context);

		Object result;
		try {
			result = engine.invokeFunction(FUNCTION_NAME, record, ((CustomProcessorNode) node).getForm());
		} catch (ScriptException e) {
			throw new RuntimeException("Execute script error, record: " + record + ", error: " + e.getMessage());
		} catch (NoSuchMethodException e) {
			throw new RuntimeException("Execute script error, cannot found function " + FUNCTION_NAME);
		}
		return result;
	}

	private void applyResult(TapdataEvent tapdataEvent, Map<String, Object> newMap) {
		TapEvent tapEvent = tapdataEvent.getTapEvent();
		MessageEntity messageEntity = tapdataEvent.getMessageEntity();
		boolean isTapRecordEvent = tapEvent instanceof TapRecordEvent;
		Map<String, Object> after;
		Map<String, Object> before;
		if (isTapRecordEvent) {
			after = TapEventUtil.getAfter(tapEvent);
			before = TapEventUtil.getBefore(tapEvent);
		} else {
			messageEntity = tapdataEvent.getMessageEntity();
			after = messageEntity.getAfter();
			before = messageEntity.getBefore();
		}
		boolean isAfter = null != after;
		if (isAfter) {
			after.clear();
			after.putAll(newMap);
			if (isTapRecordEvent) {
				TapEventUtil.setAfter(tapEvent, after);
			} else {
				messageEntity.setAfter(after);
			}
		} else {
			before.clear();
			before.putAll(newMap);
			if (isTapRecordEvent) {
				TapEventUtil.setBefore(tapEvent, before);
			} else {
				messageEntity.setBefore(before);
			}
		}
	}

	private void applyResult(TapdataEvent tapdataEvent, Map<String, Object> newMap, String op, Map<String, Object> before) {
		TapEvent tapEvent = tapdataEvent.getTapEvent();
		if (tapEvent instanceof TapRecordEvent) {
			TapEvent targetTapEvent = getTapEvent(tapEvent, op, before);
			setRecordMap(targetTapEvent, op, newMap);
			tapdataEvent.setTapEvent(targetTapEvent);
			return;
		}
		applyResult(tapdataEvent, newMap);
	}

	private List<?> getOpList(Map<String, Object> context) {
		Object opList = context.get("opList");
		if (null == opList) {
			return null;
		}
		if (!(opList instanceof List)) {
			return null;
		}
		return (List<?>) opList;
	}

	private void validateOpList(List<?> resultList, List<?> opList) {
		if (null != opList && resultList.size() != opList.size()) {
			obsLogger.warn("context.opList size must match the result list size");
		}
	}

	private String resolveListOp(List<?> opList, int index, String defaultOp) {
		if (null == opList) {
			return defaultOp;
		}
		return resolveOp(opList.get(index), defaultOp);
	}

	private String resolveOp(Object op, String defaultOp) {
		if (op instanceof String && StringUtils.isNotBlank((String) op)) {
			return (String) op;
		}
		return defaultOp;
	}

	private TapEvent getTapEvent(TapEvent tapEvent, String op, Map<String, Object> before) {
		if (StringUtils.equals(TapEventUtil.getOp(tapEvent), op)) {
			TapEventUtil.setBefore(tapEvent, before);
			return tapEvent;
		}
		OperationType operationType = OperationType.fromOp(op);
		TapEvent result;
		switch (operationType) {
			case INSERT:
				result = TapInsertRecordEvent.create();
				tapEvent.clone(result);
				break;
			case UPDATE:
				result = TapUpdateRecordEvent.create();
				tapEvent.clone(result);
				TapEventUtil.setBefore(result, before);
				break;
			case DELETE:
				result = TapDeleteRecordEvent.create();
				tapEvent.clone(result);
				break;
			default:
				result =  tapEvent;
				break;
		}
		return result;
	}

	private void setRecordMap(TapEvent tapEvent, String op, Map<String, Object> recordMap) {
		if (ConnectorConstant.MESSAGE_OPERATION_DELETE.equals(op)) {
			TapEventUtil.setBefore(tapEvent, recordMap);
		} else {
			TapEventUtil.setAfter(tapEvent, recordMap);
		}
	}

	@Override
	protected void doClose() throws TapCodeException {
		CommonUtils.ignoreAnyError(() -> {
			if (this.engine instanceof GraalJSScriptEngine) {
				((GraalJSScriptEngine) this.engine).close();
			}
		}, TAG);
		if (null != processContextThreadLocal) {
			processContextThreadLocal.remove();
		}
		// Close script executors manager
		CommonUtils.ignoreAnyError(() -> {
			if (this.scriptExecutorsManager != null) {
				this.scriptExecutorsManager.close();
				this.scriptExecutorsManager = null;
			}
		}, TAG);
		super.doClose();
	}

	private static class StateMap implements KVMap<Object> {
		private HazelcastInstance hazelcastInstance;

		public StateMap hazelcastInstance(HazelcastInstance hazelcastInstance) {
			this.hazelcastInstance = hazelcastInstance;
			return this;
		}

		private Map<String, Object> map;

		@Override
		public void init(String mapKey, Class<Object> valueClass) {
			this.map = new HashMap<>();
		}

		@Override
		public void put(String key, Object o) {
			map.put(key, o);
		}

		@Override
		public Object putIfAbsent(String key, Object o) {
			return map.putIfAbsent(key, o);
		}

		@Override
		public Object remove(String key) {
			return map.remove(key);
		}

		@Override
		public void clear() {
			map.clear();
		}

		@Override
		public void reset() {
			clear();
		}

		@Override
		public Object get(String key) {
			return map.get(key);
		}
	}

	@Override
	public boolean needCopyBatchEventWrapper() {
		return true;
	}

	@Override
	protected void handleTransformToTapValueResult(TapdataEvent tapdataEvent) {
		tapdataEvent.setTransformToTapValueResult(null);
	}
}
