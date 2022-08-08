package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.HazelcastUtil;
import com.tapdata.entity.*;
import com.tapdata.entity.dataflow.Stage;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.processor.dataflow.*;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.process.*;
import com.tapdata.tm.commons.task.dto.SubTaskDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.codec.ToTapValueCodec;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.schema.value.TapValue;
import io.tapdata.entity.utils.JavaTypesToTapTypes;
import io.tapdata.flow.engine.V2.common.node.NodeTypeEnum;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import io.tapdata.schema.TapTableMap;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Query;

import java.util.*;
import java.util.function.BiConsumer;

import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * @author jackin
 * @date 2021/12/16 10:34 PM
 **/
public class HazelcastProcessorNode extends HazelcastProcessorBaseNode {

	private DataFlowProcessor dataFlowProcessor;

	public HazelcastProcessorNode(DataProcessorContext dataProcessorContext) throws Exception {
		super(dataProcessorContext);
	}

	@Override
	protected void doInit(@NotNull Context context) throws Exception {
		super.doInit(context);
		initDataFlowProcessor();
	}

	@Override
	protected void updateNodeConfig() {
		super.updateNodeConfig();
		try {
			initDataFlowProcessor();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private void initDataFlowProcessor() throws Exception {
		dataFlowProcessor = new ScriptDataFlowProcessor();
		final Stage stage = HazelcastUtil.node2CommonStage(processorBaseContext.getNode());
		dataFlowProcessor = createDataFlowProcessor(processorBaseContext.getNode(), stage);
		Job job = new Job();
		SubTaskDto subTaskDto = processorBaseContext.getSubTaskDto();
		job.setDataFlowId(subTaskDto.getId().toHexString());
		job.setStatus(ConnectorConstant.RUNNING);
		job.setSubTaskId(subTaskDto.getId().toHexString());
		job.setTaskId(subTaskDto.getParentId().toHexString());
		job.setJobErrorNotifier(this::errorHandle);
		job.setUser_id(subTaskDto.getUserId());
		List<JavaScriptFunctions> javaScriptFunctions = clientMongoOperator.find(new Query(where("type").ne("system")).with(Sort.by(Sort.Order.asc("last_update"))), ConnectorConstant.JAVASCRIPT_FUNCTION_COLLECTION, JavaScriptFunctions.class);

		ProcessorContext processorContext = new ProcessorContext(
				null,
				null,
				job,
				clientMongoOperator,
				javaScriptFunctions,
				null,
				((DataProcessorContext) processorBaseContext).getCacheService()
		);
		dataFlowProcessor.initialize(processorContext, stage);
	}

	@Override
	protected void tryProcess(TapdataEvent tapdataEvent, BiConsumer<TapdataEvent, ProcessResult> consumer) {
		TapEvent tapEvent = tapdataEvent.getTapEvent();
		MessageEntity messageEntity = tapEvent2Message((TapRecordEvent) tapEvent);
		messageEntity.setOffset(tapdataEvent.getOffset());
		int cnt = messageEntity.isDml() ? 1 : 0;
		resetInputCounter.inc(cnt);
		inputCounter.inc(cnt);
		inputQPS.add(cnt);
		long start = System.currentTimeMillis();
		final List<MessageEntity> processedMessages = dataFlowProcessor.process(Collections.singletonList(messageEntity));
		timeCostAvg.add(System.currentTimeMillis() - start);
		resetOutputCounter.inc(cnt);
		outputCounter.inc(cnt);
		outputQPS.add(cnt);
		if (CollectionUtils.isNotEmpty(processedMessages)) {
			for (MessageEntity processedMessage : processedMessages) {
				TapdataEvent processedEvent = new TapdataEvent();
				TapRecordEvent tapRecordEvent = message2TapEvent(processedMessage);
				if (tapRecordEvent != null) {
					processedEvent.setTapEvent(tapRecordEvent);
					String tableName;
					if (multipleTables || StringUtils.equalsAnyIgnoreCase(processorBaseContext.getSubTaskDto().getParentTask().getSyncType(),
									TaskDto.SYNC_TYPE_DEDUCE_SCHEMA)) {
						tableName = processedMessage.getTableName();
					} else {
						tableName = processorBaseContext.getNode().getId();
					}
					consumer.accept(processedEvent, ProcessResult.create().tableId(tableName));
				}
			}
		}
	}

	@Override
	protected void transformToTapValue(TapdataEvent tapdataEvent, TapTableMap<String, TapTable> tapTableMap, String tableName) {

		if (null == tapTableMap)
			throw new IllegalArgumentException("Transform to TapValue failed, tapTableMap is empty, table name: " + tableName);
		TapTable tapTable = tapTableMap.get(tableName);
		if (null == tapTable)
			throw new IllegalArgumentException("Transform to TapValue failed, table schema is empty, table name: " + tableName);
		LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
		if (MapUtils.isEmpty(nameFieldMap))
			throw new IllegalArgumentException("Transform to TapValue failed, field map is empty, table name: " + tableName);
		TapEvent tapEvent = tapdataEvent.getTapEvent();
		Map<String, Object> before = TapEventUtil.getBefore(tapEvent);
		if (MapUtils.isNotEmpty(before)) {
			Map<String, io.tapdata.entity.schema.type.TapType> tapTypeMap = getTapTypeMap(before);
			codecsFilterManager.transformToTapValueMap(before, nameFieldMap);
			updateTapType(before, tapTypeMap);
		}
		Map<String, Object> after = TapEventUtil.getAfter(tapEvent);
		if (MapUtils.isNotEmpty(after)) {
			Map<String, TapType> tapTypeMap = getTapTypeMap(after);
			codecsFilterManager.transformToTapValueMap(after, nameFieldMap);
			updateTapType(after, tapTypeMap);
		}

	}

	private Map<String, TapType> getTapTypeMap(Map<String, Object> map) {
		Map<String, TapType> tapTypeMap = new HashMap<>();
		if (org.apache.commons.collections4.MapUtils.isNotEmpty(map)) {
			for (Map.Entry<String, Object> entry : map.entrySet()) {
				if (entry.getValue() instanceof TapValue) {
					tapTypeMap.put(entry.getKey(), ((TapValue<?, ?>) entry.getValue()).getTapType());
				} else {
					TapType tapType = JavaTypesToTapTypes.toTapType(entry.getValue());
					if (tapType != null) {
						tapTypeMap.put(entry.getKey(), tapType);
					}
				}
			}
		}
		return tapTypeMap;
	}

	private void updateTapType(Map<String, Object> map, Map<String, TapType> tapTypeMap) {
		Iterator<Map.Entry<String, Object>> iterator = map.entrySet().iterator();
		while (iterator.hasNext()) {
			Map.Entry<String, Object> entry = iterator.next();
			if (entry.getValue() instanceof TapValue) {
				TapType tapType = tapTypeMap.get(entry.getKey());
				if (tapType == null) {
					continue;
				}
				TapType oldTapType = ((TapValue<?, ?>) entry.getValue()).getTapType();
				if (tapType.getType() != oldTapType.getType()) {
					ToTapValueCodec<?> toTapValueCodec = tapType.toTapValueCodec();
					TapValue newTapValue = toTapValueCodec.toTapValue(((TapValue<?, ?>) entry.getValue()).getValue(), tapType);
					newTapValue.setTapType(tapType);
					entry.setValue(newTapValue);
				}
			}
		}

	}

	private DataFlowProcessor createDataFlowProcessor(Node node, Stage stage) {
		NodeTypeEnum nodeType = NodeTypeEnum.get(node.getType());
		DataFlowProcessor dataFlowProcessor = null;
		switch (nodeType) {
			case CACHE_LOOKUP_PROCESSOR:
			case JS_PROCESSOR:
				dataFlowProcessor = new ScriptDataFlowProcessor();
				stage.setType(Stage.StageTypeEnum.SCRIPT_TYPE.getType());

				JsProcessorNode jsProcessorNode = (JsProcessorNode) node;
				stage.setScript(jsProcessorNode.getScript());
				break;
			case MIGRATE_JS_PROCESSOR:
				dataFlowProcessor = new ScriptDataFlowProcessor();
				stage.setType(Stage.StageTypeEnum.SCRIPT_TYPE.getType());

				MigrateJsProcessorNode migrateJsProcessorNode = (MigrateJsProcessorNode) node;
				stage.setScript(migrateJsProcessorNode.getScript());
				break;
			case TABLE_RENAME_PROCESSOR:
				dataFlowProcessor = new TableRenameProcessor((TableRenameProcessNode) node);
				break;
			case MIGRATE_FIELD_RENAME_PROCESSOR:
				dataFlowProcessor = new MigrateFieldRenameProcessor((MigrateFieldRenameProcessorNode) node);
				break;
			case FIELD_PROCESSOR:
			case FIELD_RENAME_PROCESSOR:
			case FIELD_ADD_DEL_PROCESSOR:
			case FIELD_CALC_PROCESSOR:
			case FIELD_MOD_TYPE_PROCESSOR:
				List<FieldProcess> fieldProcesses = new ArrayList<>();
				FieldProcessorNode fieldProcessor = (FieldProcessorNode) node;
				final List<FieldProcessorNode.Operation> operations = fieldProcessor.getOperations();
				if (null != operations) {
					for (FieldProcessorNode.Operation operation : operations) {
						FieldProcess fieldProcess = new FieldProcess();
						fieldProcess.setField(operation.getField());
						fieldProcess.setOp(operation.getOp());
						fieldProcess.setOperand(StringUtils.isNotBlank(operation.getOperand()) ? operation.getOperand() : operation.getField());
						fieldProcess.setOriginalDataType(operation.getOriginalDataType());
						fieldProcess.setJavaType(operation.getType());
						fieldProcesses.add(fieldProcess);
					}
				}
				List<FieldProcessorNode.Script> scripts = ((FieldProcessorNode) node).getScripts();
				List<FieldScript> fieldScripts = new ArrayList<>();
				if (null != scripts) {
					for (FieldProcessorNode.Script script : scripts) {
						FieldScript fieldScript = new FieldScript();
						fieldScript.setField(script.getField());
						fieldScript.setScript(script.getScript());
						fieldScript.setScriptType(script.getScriptType());
						fieldScript.setTableName(script.getTableName());
						fieldScripts.add(fieldScript);
					}
				}
				stage.setScripts(fieldScripts);
				stage.setOperations(fieldProcesses);
				if (node instanceof FieldRenameProcessorNode) {
					dataFlowProcessor = new FieldDataFlowProcessor(((FieldRenameProcessorNode) node).getFieldsNameTransform());
				} else {
					dataFlowProcessor = new FieldDataFlowProcessor();
				}
				break;
			case ROW_FILTER_PROCESSOR:
				dataFlowProcessor = new RowFilterProcessor();
				RowFilterProcessorNode rowFilterProcessor = (RowFilterProcessorNode) node;

				stage.setAction(rowFilterProcessor.getAction());
				stage.setExpression(rowFilterProcessor.getExpression());
				break;
		}

		return dataFlowProcessor;
	}

	@Override
	public void doClose() throws Exception {
		if (dataFlowProcessor != null) {
			dataFlowProcessor.stop();
		}
		super.doClose();
	}
}
