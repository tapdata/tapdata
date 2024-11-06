package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.HazelcastUtil;
import com.tapdata.constant.MapUtilV2;
import com.tapdata.constant.NotExistsNode;
import com.tapdata.entity.*;
import com.tapdata.entity.dataflow.Capitalized;
import com.tapdata.entity.dataflow.Stage;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.processor.dataflow.*;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.process.*;
import com.tapdata.tm.commons.dag.process.script.py.MigratePyProcessNode;
import com.tapdata.tm.commons.dag.process.script.py.PyProcessNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.codec.ToTapValueCodec;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.control.HeartbeatEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.schema.value.TapValue;
import io.tapdata.entity.utils.JavaTypesToTapTypes;
import io.tapdata.error.TaskProcessorExCode_11;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.node.NodeTypeEnum;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import io.tapdata.schema.TapTableMap;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Query;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * @author jackin
 * @date 2021/12/16 10:34 PM
 **/
public class HazelcastProcessorNode extends HazelcastProcessorBaseNode {

	private DataFlowProcessor dataFlowProcessor;
	private FieldRenameProcessorNode fieldRenameProcessorNode;
	private Capitalized capitalized;
	private Map<String, Map<String, String>> fieldsNameTransformMap;

	public HazelcastProcessorNode(DataProcessorContext dataProcessorContext) throws Exception {
		super(dataProcessorContext);
	}

	@Override
	protected void doInit(@NotNull Context context) throws TapCodeException {
		super.doInit(context);
		initDataFlowProcessor();
		if (getNode() instanceof FieldRenameProcessorNode) {
			fieldRenameProcessorNode = (FieldRenameProcessorNode) getNode();
			String fieldsNameTransform = fieldRenameProcessorNode.getFieldsNameTransform();
			if (StringUtils.isNotBlank(fieldsNameTransform)) {
				capitalized = Capitalized.fromValue(fieldsNameTransform);
				this.fieldsNameTransformMap = new HashMap<>();
			}
		}
	}

	@Override
	protected void updateNodeConfig(TapdataEvent tapdataEvent) {
		super.updateNodeConfig(tapdataEvent);
		try {
			initDataFlowProcessor();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	protected void initDataFlowProcessor() throws TapCodeException {
		final Stage stage = HazelcastUtil.node2CommonStage(processorBaseContext.getNode());
		dataFlowProcessor = createDataFlowProcessor(processorBaseContext.getNode(), stage);
		Job job = new Job();
		TaskDto subTaskDto = processorBaseContext.getTaskDto();
		job.setDataFlowId(subTaskDto.getId().toHexString());
		job.setStatus(ConnectorConstant.RUNNING);
		job.setSubTaskId(subTaskDto.getId().toHexString());
		job.setTaskId(subTaskDto.getId().toHexString());
		job.setJobErrorNotifier(this::errorHandle);
		job.setUser_id(subTaskDto.getUserId());
		job.setStopOnError(true);
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
		try {
			dataFlowProcessor.initialize(processorContext, stage);
			dataFlowProcessor.logListener(logListener());
		} catch (Exception e) {
			throw new TapCodeException(TaskProcessorExCode_11.INIT_DATA_FLOW_PROCESSOR_FAILED, "Init data flow processor failed", e)
					.dynamicDescriptionParameters(processorBaseContext.getNode().getName(),processorBaseContext.getNode().getType());
		}
	}

	@Override
	protected void tryProcess(TapdataEvent tapdataEvent, BiConsumer<TapdataEvent, ProcessResult> consumer) {
		TapEvent tapEvent = tapdataEvent.getTapEvent();
		if (!(tapEvent instanceof TapRecordEvent)) {
			return;
		}
		if (null != fieldRenameProcessorNode && null != capitalized) {
			renameFields(TapEventUtil.getBefore(tapEvent));
			renameFields(TapEventUtil.getAfter(tapEvent));
		}
		TapRecordEvent tapRecordEvent = (TapRecordEvent) tapEvent;
		MessageEntity messageEntity = tapEvent2Message((TapRecordEvent) tapEvent);
		messageEntity.setOffset(tapdataEvent.getOffset());
		final List<MessageEntity> processedMessages = dataFlowProcessor.process(Collections.singletonList(messageEntity));
		if (CollectionUtils.isNotEmpty(processedMessages)) {
			for (MessageEntity processedMessage : processedMessages) {
				if (OperationType.COMMIT_OFFSET.getOp().equals(processedMessage.getOp())) {
					HeartbeatEvent heartbeatEvent = new HeartbeatEvent();
					if (tapdataEvent.getBatchOffset() == null && tapdataEvent.getStreamOffset() == null) {
						continue;
					}
					if (tapRecordEvent.getReferenceTime() == null) {
						continue;
					}
					heartbeatEvent.setReferenceTime(tapRecordEvent.getReferenceTime());
					tapdataEvent.setTapEvent(heartbeatEvent);
				} else {
					TapEventUtil.setBefore(tapRecordEvent, processedMessage.getBefore());
					TapEventUtil.setAfter(tapRecordEvent, processedMessage.getAfter());
					List<String> removedFields = null;
					if (tapRecordEvent instanceof TapUpdateRecordEvent) {
						removedFields = ((TapUpdateRecordEvent) tapRecordEvent).getRemovedFields();
					}
					tapRecordEvent = message2TapEvent(processedMessage);
					if (null != removedFields) {
						TapEventUtil.setRemoveFields(tapRecordEvent, removedFields);
					}
					tapdataEvent.setTapEvent(tapRecordEvent);
				}
				handleRemoveFields(tapdataEvent);
				consumer.accept(tapdataEvent, getProcessResult(processedMessage.getTableName()));
			}
		}
	}

	@Override
	protected void transformToTapValue(TapdataEvent tapdataEvent, TapTableMap<String, TapTable> tapTableMap, String tableName) {
		TapEvent tapEvent = tapdataEvent.getTapEvent();
		Map<String, TapType> afterTapTypeMap = null;
		if (StringUtils.equalsAnyIgnoreCase(processorBaseContext.getTaskDto().getSyncType(), TaskDto.SYNC_TYPE_DEDUCE_SCHEMA)) {
			Map<String, Object> after = TapEventUtil.getAfter(tapEvent);
			afterTapTypeMap = getTapTypeMap(after);
		}
		super.transformToTapValue(tapdataEvent, tapTableMap, tableName);
		if (StringUtils.equalsAnyIgnoreCase(processorBaseContext.getTaskDto().getSyncType(), TaskDto.SYNC_TYPE_DEDUCE_SCHEMA)) {
			Map<String, Object> after = TapEventUtil.getAfter(tapEvent);
			updateTapType(after, afterTapTypeMap);
		}
	}

	private Map<String, TapType> getTapTypeMap(Map<String, Object> map) {
		Map<String, TapType> tapTypeMap = new HashMap<>();
		if (MapUtils.isNotEmpty(map)) {
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
		if (MapUtils.isEmpty(map) || MapUtils.isEmpty(tapTypeMap)) {
			return;
		}
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

	protected DataFlowProcessor createDataFlowProcessor(Node node, Stage stage) {
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
			case PYTHON_PROCESS:
				dataFlowProcessor = new ScriptDataFlowProcessor();
				stage.setType(Stage.StageTypeEnum.SCRIPT_TYPE.getType());
				PyProcessNode pyProcessorNode = (PyProcessNode) node;
				stage.setScript(pyProcessorNode.getScript());
				break;
			case MIGRATE_JS_PROCESSOR:
				dataFlowProcessor = new ScriptDataFlowProcessor();
				stage.setType(Stage.StageTypeEnum.SCRIPT_TYPE.getType());

				MigrateJsProcessorNode migrateJsProcessorNode = (MigrateJsProcessorNode) node;
				stage.setScript(migrateJsProcessorNode.getScript());
				break;
			case MIGRATE_PYTHON_PROCESS:
				dataFlowProcessor = new ScriptDataFlowProcessor();
				stage.setType(Stage.StageTypeEnum.SCRIPT_TYPE.getType());
				MigratePyProcessNode migratePyProcessNode = (MigratePyProcessNode) node;
				stage.setScript(migratePyProcessNode.getScript());
				break;
			case FIELD_PROCESSOR:
			case FIELD_RENAME_PROCESSOR:
			case FIELD_ADD_DEL_PROCESSOR:
			case FIELD_CALC_PROCESSOR:
			case FIELD_MOD_TYPE_PROCESSOR:
			case FIELD_MOD_TYPE_FILTER_PROCESSOR:
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
					dataFlowProcessor = new FieldDataFlowProcessor();
				} else if (node instanceof FieldAddDelProcessorNode) {
					dataFlowProcessor = new FieldDataFlowProcessor(((FieldAddDelProcessorNode) node).isDeleteAllFields());
				} else {
					dataFlowProcessor = new FieldDataFlowProcessor();
				}
				break;
			case ROW_FILTER_PROCESSOR:
				dataFlowProcessor = new RowFilterProcessor();
				RowFilterProcessorNode rowFilterProcessor = (RowFilterProcessorNode) node;

				stage.setAction(rowFilterProcessor.getAction());
				stage.setExpression(rowFilterProcessor.getExpression());
				if (StringUtils.equalsAnyIgnoreCase(processorBaseContext.getTaskDto().getSyncType(), TaskDto.SYNC_TYPE_DEDUCE_SCHEMA)) {
					this.setIgnore(true);
				}
				break;
		}

		return dataFlowProcessor;
	}

	@Override
	public void doClose() throws TapCodeException {
		if (dataFlowProcessor != null) {
			dataFlowProcessor.stop();
		}
		super.doClose();
	}

	@Override
	protected void handleOriginalValueMapIfNeed(AtomicReference<TapValueTransform> tapValueTransform) {
		if (getNode() instanceof FieldRenameProcessorNode) {
			FieldRenameProcessorNode fieldRenameProcessorNode = (FieldRenameProcessorNode) getNode();
			List<FieldProcessorNode.Operation> processorNodeOperations = fieldRenameProcessorNode.getOperations();
			if (null != processorNodeOperations && !processorNodeOperations.isEmpty()) {
				TapValueTransform tapValueTransformTemp = tapValueTransform.get();
				if (null == tapValueTransformTemp) return;
				Map before = tapValueTransformTemp.getBefore();
				Map after = tapValueTransformTemp.getAfter();
				for (FieldProcessorNode.Operation processorNodeOperation : processorNodeOperations) {
					if (null != processorNodeOperation && "RENAME".equals(processorNodeOperation.getOp())) {
						String field = processorNodeOperation.getField();
						String operand = processorNodeOperation.getOperand();
						rename(before, field, operand);
						rename(after, field, operand);
					}
				}
			}
		}
	}

	private void rename(Map data, String field, String operand) {
		if(MapUtils.isEmpty(data)) {
			return;
		}
		Object value = MapUtilV2.getValueByKey(data, field);
		if (value instanceof NotExistsNode) return;
		MapUtilV2.removeValueByKey(data, field);
		try {
			MapUtilV2.putValueInMap(data, operand, value);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public boolean needTransformValue() {
		return false;
	}

	public void renameFields(Map<String, Object> data) {
		if (null == data) {
			return;
		}

		Queue<Object> queue = new LinkedList<>();
		queue.add(data);

		while (!queue.isEmpty()) {
			Object current = queue.poll();

			if (current instanceof Map) {
				Map<String, Object> currentMap = (Map<String, Object>) current;
				List<String> keys = new ArrayList<>(currentMap.keySet());
				for (String key : keys) {
					String newKey = fieldsNameTransformMap
							.computeIfAbsent(Thread.currentThread().getName(), k -> new HashMap<>())
							.computeIfAbsent(key, k -> Capitalized.convert(key, capitalized));
					Object value = currentMap.remove(key);
					currentMap.put(newKey, value);
					if (value instanceof Map || value instanceof List) {
						queue.add(value);
					}
				}
			} else if (current instanceof List) {
				List<Object> currentList = (List<Object>) current;
				for (Object item : currentList) {
					if (item instanceof Map || item instanceof List) {
						queue.add(item);
					}
				}
			}
		}
	}

	/**
	 * Handles the removal of fields from the TapEvent within the given TapdataEvent.
	 *
	 * @param tapdataEvent the TapdataEvent containing the TapEvent to process
	 */
	protected void handleRemoveFields(TapdataEvent tapdataEvent) {
	    // Get the TapEvent object
		TapEvent tapEvent = tapdataEvent.getTapEvent();
	    // Get the list of fields to be removed
		List<String> removeFields = TapEventUtil.getRemoveFields(tapEvent);
		if (CollectionUtils.isEmpty(removeFields)) {
			return;
		}
	    // If there is a field rename processor, perform field renaming
		if (null != fieldRenameProcessorNode && null != capitalized) {
			List<String> newRemoveFields = new ArrayList<>();
			removeFields.forEach(field->{
				String newField = fieldsNameTransformMap
						.computeIfAbsent(Thread.currentThread().getName(), k -> new HashMap<>())
						.computeIfAbsent(field, k -> Capitalized.convert(field, capitalized));
				newRemoveFields.add(newField);
			});
	        // Update the removeFields in TapEvent
			TapEventUtil.setRemoveFields(tapEvent, newRemoveFields);
			removeFields = newRemoveFields;
		}
	    // Get the current node
		Node<?> node = getNode();
	    // If the current node is an instance of FieldProcessorNode, process field operations
		if (node instanceof FieldProcessorNode) {
			List<FieldProcessorNode.Operation> operations = ((FieldProcessorNode) node).getOperations();
			if (CollectionUtils.isEmpty(operations)) {
				return;
			}
	        // Iterate through the operations and update the removeFields list based on the operation type
			for (FieldProcessorNode.Operation operation : operations) {
				String op = operation.getOp();
				switch (op) {
					case "RENAME":
						boolean removed = removeFields.remove(operation.getField());
						if (removed) {
							removeFields.add(operation.getOperand());
						}
						break;
					case "REMOVE":
						removeFields.remove(operation.getField());
						break;
					default:
						break;
				}
			}
		}
	}

	@Override
	public boolean supportConcurrentProcess() {
		return true;
	}

	@Override
	protected void handleTransformToTapValueResult(TapdataEvent tapdataEvent) {
		TransformToTapValueResult transformToTapValueResult = tapdataEvent.getTransformToTapValueResult();
		if (null == transformToTapValueResult || transformToTapValueResult.isEmpty()) {
			return;
		}
		if (null != fieldRenameProcessorNode && null != capitalized) {
			Optional.ofNullable(transformToTapValueResult.getBeforeTransformedToTapValueFieldNames()).ifPresent(ttf -> ttf.forEach(field -> {
				String newField = fieldsNameTransformMap
						.computeIfAbsent(Thread.currentThread().getName(), k -> new HashMap<>())
						.computeIfAbsent(field, k -> Capitalized.convert(field, capitalized));
				ttf.remove(field);
				ttf.add(newField);
			}));
			Optional.ofNullable(transformToTapValueResult.getAfterTransformedToTapValueFieldNames()).ifPresent(ttf -> ttf.forEach(field -> {
				String newField = fieldsNameTransformMap
						.computeIfAbsent(Thread.currentThread().getName(), k -> new HashMap<>())
						.computeIfAbsent(field, k -> Capitalized.convert(field, capitalized));
				ttf.remove(field);
				ttf.add(newField);
			}));
		}
		Node<?> node = getNode();
		if (node instanceof FieldProcessorNode) {
			List<FieldProcessorNode.Operation> operations = ((FieldProcessorNode) node).getOperations();
			if (null != operations) {
				for (FieldProcessorNode.Operation operation : operations) {
					String op = operation.getOp();
					switch (op) {
						case "CREATE":
							Optional.ofNullable(transformToTapValueResult.getBeforeTransformedToTapValueFieldNames()).ifPresent(ttf -> ttf.add(operation.getField()));
							Optional.ofNullable(transformToTapValueResult.getAfterTransformedToTapValueFieldNames()).ifPresent(ttf -> ttf.add(operation.getField()));
							break;
						case "RENAME":
							Optional.ofNullable(transformToTapValueResult.getBeforeTransformedToTapValueFieldNames()).ifPresent(ttf -> {
								boolean remove = ttf.remove(operation.getField());
								if (remove) {
									ttf.add(operation.getOperand());
								}
							});
							Optional.ofNullable(transformToTapValueResult.getAfterTransformedToTapValueFieldNames()).ifPresent(ttf -> {
								boolean remove = ttf.remove(operation.getField());
								if (remove) {
									ttf.add(operation.getOperand());
								}
							});
							break;
						case "REMOVE":
							Optional.ofNullable(transformToTapValueResult.getBeforeTransformedToTapValueFieldNames()).ifPresent(b -> b.remove(operation.getField()));
							Optional.ofNullable(transformToTapValueResult.getAfterTransformedToTapValueFieldNames()).ifPresent(a -> a.remove(operation.getField()));
							break;
					}
				}
			}
		}
	}
}
