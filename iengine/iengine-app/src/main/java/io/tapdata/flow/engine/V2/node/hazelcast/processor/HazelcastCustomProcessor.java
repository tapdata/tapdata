package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import com.hazelcast.core.HazelcastInstance;
import com.oracle.truffle.js.scriptengine.GraalJSScriptEngine;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.HazelcastUtil;
import com.tapdata.constant.MapUtil;
import com.tapdata.entity.JavaScriptFunctions;
import com.tapdata.entity.MessageEntity;
import com.tapdata.entity.SyncStage;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.processor.ScriptUtil;
import com.tapdata.processor.context.ProcessContext;
import com.tapdata.processor.context.ProcessContextEvent;
import com.tapdata.processor.error.ScriptProcessorExCode_30;
import com.tapdata.tm.commons.customNode.CustomNodeTempDto;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.process.CustomProcessorNode;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.error.TaskProcessorExCode_11;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.node.NodeTypeEnum;
import io.tapdata.flow.engine.V2.script.ObsScriptLogger;
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

	public HazelcastCustomProcessor(DataProcessorContext dataProcessorContext) {
		super(dataProcessorContext);
	}

	@Override
	protected void doInit(@NotNull Context context) throws TapCodeException {
		super.doInit(context);
		this.processContextThreadLocal = ThreadLocal.withInitial(HashMap::new);
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
				engine = ScriptUtil.getScriptEngine(
						customNodeTempDto.getTemplate(),
						javaScriptFunctions,
						clientMongoOperator,
						((DataProcessorContext) processorBaseContext).getCacheService(),
						new ObsScriptLogger(obsLogger, logger));
				stateMap = getStateMap(context.hazelcastInstance(), node.getId());
				((ScriptEngine) engine).put("state", stateMap);
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
		execute(tapdataEvent);
		String tableName = TapEventUtil.getTableId(tapdataEvent.getTapEvent());
		ProcessResult processResult = null;
		if (StringUtils.isNotEmpty(tableName)) {
			processResult = getProcessResult(tableName);
		}
		consumer.accept(tapdataEvent, processResult);
	}

	protected void execute(TapdataEvent tapdataEvent) throws IllegalAccessException {
		Node<?> node = processorBaseContext.getNode();
		TapEvent tapEvent = tapdataEvent.getTapEvent();
		MessageEntity messageEntity = tapdataEvent.getMessageEntity();
		boolean isAfter;
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
			return;
		}
		Map<String, Object> record = null == after ? before : after;
		isAfter = null != after;
		((ScriptEngine) engine).put("log", logger);

		String tableName = TapEventUtil.getTableId(tapEvent);
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
		if (null != before) {
			processContextEvent.setBefore(before);
		}
		processContextEvent.setType(processContext.getType());
		Map<String, Object> eventMap = MapUtil.obj2Map(processContextEvent);
		Map<String, Object> contextMap = MapUtil.obj2Map(processContext);
		contextMap.put("event", eventMap);
		contextMap.put("before", before);
		contextMap.put("info", tapEvent.getInfo());
		contextMap.put("isReplace", tapEvent instanceof TapUpdateRecordEvent && Boolean.TRUE.equals(((TapUpdateRecordEvent) tapEvent).getIsReplaceEvent()));
		contextMap.put("removedFields", TapEventUtil.getRemoveFields(tapEvent));
		Map<String, Object> context = this.processContextThreadLocal.get();
		context.putAll(contextMap);

		((ScriptEngine) engine).put("context", context);

		Object result;
		try {
			result = engine.invokeFunction(FUNCTION_NAME, record, ((CustomProcessorNode) node).getForm());
		} catch (ScriptException e) {
			throw new RuntimeException("Execute script error, record: " + record + ", error: " + e.getMessage());
		} catch (NoSuchMethodException e) {
			throw new RuntimeException("Execute script error, cannot found function " + FUNCTION_NAME);
		}
		if (null == result) {
			return;
		}
		Map<String, Object> newMap = new HashMap<>();
		MapUtil.copyToNewMap((Map<String, Object>) result, newMap);
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

	@Override
	protected void doClose() throws TapCodeException {
		CommonUtils.ignoreAnyError(() -> {
			if (this.engine instanceof GraalJSScriptEngine) {
				((GraalJSScriptEngine) this.engine).close();
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
	protected void handleTransformToTapValueResult(TapdataEvent tapdataEvent) {
		tapdataEvent.setTransformToTapValueResult(null);
	}
}
