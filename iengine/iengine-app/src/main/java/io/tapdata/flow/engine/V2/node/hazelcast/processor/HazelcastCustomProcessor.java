package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import com.hazelcast.core.HazelcastInstance;
import com.oracle.truffle.js.scriptengine.GraalJSScriptEngine;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.HazelcastUtil;
import com.tapdata.constant.MapUtil;
import com.tapdata.entity.JavaScriptFunctions;
import com.tapdata.entity.MessageEntity;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.processor.ScriptUtil;
import com.tapdata.tm.commons.customNode.CustomNodeTempDto;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.process.CustomProcessorNode;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.flow.engine.V2.node.NodeTypeEnum;
import io.tapdata.flow.engine.V2.exception.node.NodeException;
import io.tapdata.flow.engine.V2.script.ObsScriptLogger;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.apache.commons.collections.MapUtils;
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

	public HazelcastCustomProcessor(DataProcessorContext dataProcessorContext) {
		super(dataProcessorContext);
	}

	@Override
	protected void doInit(@NotNull Context context) throws Exception {
		super.doInit(context);
		Node<?> node = processorBaseContext.getNode();
		if (NodeTypeEnum.get(node.getType()).equals(NodeTypeEnum.CUSTOM_PROCESSOR)) {
			String customNodeId = ((CustomProcessorNode) node).getCustomNodeId();
			Query query = new Query(Criteria.where("_id").is(customNodeId));
			CustomNodeTempDto customNodeTempDto = clientMongoOperator.findOne(query, ConnectorConstant.CUSTOMNODETEMP_COLLECTION, CustomNodeTempDto.class,
					n -> !running.get());
			if (null == customNodeTempDto) {
				throw new RuntimeException("Init script engine failed, cannot find custom node template by id: " + customNodeId);
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
				throw new NodeException("Init script engine error: " + e.getMessage(), e).context(getProcessorBaseContext());
			}
		}
	}

	private static String getStateMapName(String nodeId) {
		return HazelcastCustomProcessor.class.getSimpleName() + "-" + nodeId;
	}

	private static StateMap getStateMap(HazelcastInstance hazelcastInstance, String nodeId) {
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

	private void execute(TapdataEvent tapdataEvent) {
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
	protected void doClose() throws Exception {
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

//		private IMap<String, Object> map;
		private Map<String, Object> map;

		@Override
		public void init(String mapKey, Class<Object> valueClass) {
//			if (null == hazelcastInstance) {
//				throw new IllegalArgumentException("Hazelcast instance cannot be null");
//			}
//			this.map = hazelcastInstance.getMap(mapKey);
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
}
