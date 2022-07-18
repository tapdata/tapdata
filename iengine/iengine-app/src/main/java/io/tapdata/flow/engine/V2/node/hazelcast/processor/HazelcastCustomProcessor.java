package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.MapUtil;
import com.tapdata.entity.JavaScriptFunctions;
import com.tapdata.entity.MessageEntity;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.processor.ScriptUtil;
import com.tapdata.processor.constant.JSEngineEnum;
import com.tapdata.tm.commons.customNode.CustomNodeTempDto;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.process.CustomProcessorNode;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.flow.engine.V2.common.node.NodeTypeEnum;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import org.apache.commons.collections.MapUtils;
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

	public static final String FUNCTION_NAME = "process";

	private Logger logger = LogManager.getLogger(HazelcastCustomProcessor.class);
	private Invocable engine;

	public HazelcastCustomProcessor(DataProcessorContext dataProcessorContext) {
		super(dataProcessorContext);
	}

	@Override
	protected void init(@NotNull Context context) throws Exception {
		super.init(context);
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
						JSEngineEnum.GRAALVM_JS.getEngineName(),
						customNodeTempDto.getTemplate(),
						javaScriptFunctions,
						clientMongoOperator);
			} catch (ScriptException e) {
				throw new RuntimeException("Init script engine error: " + e.getMessage());
			}
		}
	}

	@Override
	protected void tryProcess(TapdataEvent tapdataEvent, BiConsumer<TapdataEvent, ProcessResult> consumer) {
		execute(tapdataEvent);
		consumer.accept(tapdataEvent, null);
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
			int cnt = 1;
			resetInputCounter.inc(cnt);
			inputCounter.inc(cnt);
			inputQPS.add(cnt);
			long start = System.currentTimeMillis();
			result = engine.invokeFunction(FUNCTION_NAME, record, ((CustomProcessorNode) node).getForm());
			timeCostAvg.add(System.currentTimeMillis() - start);
			resetOutputCounter.inc(cnt);
			outputCounter.inc(cnt);
			outputQPS.add(cnt);
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
}
