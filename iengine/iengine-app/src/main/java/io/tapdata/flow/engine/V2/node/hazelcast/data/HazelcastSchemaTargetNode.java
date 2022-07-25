package io.tapdata.flow.engine.V2.node.hazelcast.data;


import com.hazelcast.jet.core.Inbox;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.dag.Node;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.TapValue;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import io.tapdata.schema.TapTableMap;
import io.tapdata.schema.TapTableUtil;
import org.apache.commons.collections4.MapUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;

public class HazelcastSchemaTargetNode extends HazelcastVirtualTargetNode {

	private final static Logger logger = LogManager.getLogger(HazelcastSchemaTargetNode.class);

	/**
	 * key: subTaskId+jsNodeId
	 */
	private static final Map<String, TapTable> tabTableCacheMap = new ConcurrentHashMap<>();
	private static final Map<String, List<SchemaApplyResult>> schemaApplyResultMap = new ConcurrentHashMap<>();

	private final String schemaKey;

	private final Node<?> prePreNode;

	private final TapTableMap<String, TapTable> prePreNodeTapTableMap;


	public static TapTable getTapTable(String schemaKey) {
		return tabTableCacheMap.remove(schemaKey);
	}
	public static List<SchemaApplyResult> getSchemaApplyResultList(String schemaKey) {
		return schemaApplyResultMap.remove(schemaKey);
	}


	public HazelcastSchemaTargetNode(DataProcessorContext dataProcessorContext) {
		super(dataProcessorContext);
		this.schemaKey = dataProcessorContext.getSubTaskDto().getId().toHexString() + "-" + dataProcessorContext.getNode().getId();

		List<Node<?>> preNodes = getNode().predecessors();
		if (preNodes.size() != 1) {
			throw new IllegalArgumentException("HazelcastSchemaTargetNode only allows one predecessor node");
		}
		Node<?> deductionSchemaNode = preNodes.get(0);
		List<? extends Node<?>> prePreNodes = deductionSchemaNode.predecessors();
		if (prePreNodes.size() != 1) {
			throw new IllegalArgumentException("The front node of HazelcastSchemaTargetNode only allows one front node");
		}
		this.prePreNode = prePreNodes.get(0);
		//js节点之前的节点的模型
		this.prePreNodeTapTableMap = TapTableUtil.getTapTableMapByNodeId(prePreNode.getId());
	}

	@Override
	public void process(int ordinal, @NotNull Inbox inbox) {
		try {
			if (!inbox.isEmpty()) {
				while (isRunning()) {
					List<TapdataEvent> tapdataEvents = new ArrayList<>();
					final int count = inbox.drainTo(tapdataEvents, 1000);
					if (count > 0) {

						TapRecordEvent tapEvent;
						for (TapdataEvent tapdataEvent : tapdataEvents) {
							if (logger.isDebugEnabled()) {
								logger.debug("tapdata event [{}]", tapdataEvent.toString());
							}
							if (null != tapdataEvent.getMessageEntity()) {
								tapEvent = message2TapEvent(tapdataEvent.getMessageEntity());
							} else if (null != tapdataEvent.getTapEvent()) {
								tapEvent = (TapRecordEvent) tapdataEvent.getTapEvent();
							} else {
								continue;
							}
							// 解析模型
							TapTable tapTable = getNewTapTable(tapEvent);
							tabTableCacheMap.put(schemaKey, tapTable);

							// 获取差异模型
							List<SchemaApplyResult> schemaApplyResults = getSchemaApplyResults(tapEvent, tapTable);
							schemaApplyResultMap.put(schemaKey, schemaApplyResults);
						}

					} else {
						break;
					}
				}
			}
		} catch (Exception e) {
			logger.error("Target process failed {}", e.getMessage(), e);
			throw sneakyThrow(e);
		}
	}

	@NotNull
	private TapTable getNewTapTable(TapRecordEvent tapEvent) {
		Map<String, Object> after = TapEventUtil.getAfter(tapEvent);
		if (logger.isDebugEnabled()) {
			logger.info("after map is [{}]", after);
		}
		TapTable tapTable = new TapTable();
		if (MapUtils.isNotEmpty(after)) {
			for (Map.Entry<String, Object> entry : after.entrySet()) {
				if (logger.isDebugEnabled()) {
					logger.debug("entry type: {} - {}", entry.getKey(), entry.getValue().getClass());
				}
				if (entry.getValue() instanceof TapValue) {
					TapValue<?, ?> tapValue = (TapValue<?, ?>) entry.getValue();
					TapField tapField = new TapField(entry.getKey(), tapValue.getOriginType());
					tapField.setTapType(tapValue.getTapType());
					tapTable.add(tapField);
				}
			}
		}
		return tapTable;
	}

	@NotNull
	private List<SchemaApplyResult> getSchemaApplyResults(TapRecordEvent tapEvent, TapTable tapTable) {
		List<SchemaApplyResult> schemaApplyResults = new ArrayList<>();

		LinkedHashMap<String, TapField> newNameFieldMap = tapTable.getNameFieldMap();
		LinkedHashMap<String, TapField> oldNameFieldMap = getOldNameFieldMap(tapEvent);
		if (MapUtils.isNotEmpty(newNameFieldMap) && MapUtils.isNotEmpty(oldNameFieldMap)) {
			for (Map.Entry<String, TapField> entry : newNameFieldMap.entrySet()) {
				String newFieldName = entry.getKey();
				TapField newTapField = entry.getValue();
				if (!oldNameFieldMap.containsKey(newFieldName)) {
					//create field
					schemaApplyResults.add(new SchemaApplyResult(SchemaApplyResult.OP_TYPE_CREATE, newFieldName, newTapField));
					continue;
				}
				TapField oldTapField = oldNameFieldMap.get(newFieldName);
				if (!oldTapField.getTapType().equals(newTapField.getTapType())) {
					//alter field
					schemaApplyResults.add(new SchemaApplyResult(SchemaApplyResult.OP_TYPE_CONVERT, newFieldName, newTapField));
				}
			}

			for (String oldFieldName : oldNameFieldMap.keySet()) {
				if (!newNameFieldMap.containsKey(oldFieldName)) {
					//drop field
					schemaApplyResults.add(new SchemaApplyResult(SchemaApplyResult.OP_TYPE_REMOVE, oldFieldName, null));
				}
			}
		}

		return schemaApplyResults;
	}

	private LinkedHashMap<String, TapField> getOldNameFieldMap(TapRecordEvent tapEvent) {
		TapTable oldTapTable;
		if (multipleTables) {
			oldTapTable = prePreNodeTapTableMap.get(tapEvent.getTableId());
		} else {
			oldTapTable = prePreNodeTapTableMap.get(prePreNode.getId());
		}
		if (oldTapTable == null) {
			return null;
		}
		return oldTapTable.getNameFieldMap();
	}

	public static class SchemaApplyResult {
		public static final String OP_TYPE_CREATE = "CREATE";
		public static final String OP_TYPE_REMOVE = "REMOVE";
		public static final String OP_TYPE_CONVERT = "CONVERT";
		private String op;
		private String fieldName;
		private TapField tapField;

		public SchemaApplyResult() {
		}

		public SchemaApplyResult(String op, String fieldName, TapField tapField) {
			this.op = op;
			this.fieldName = fieldName;
			this.tapField = tapField;
		}

		public String getOp() {
			return op;
		}

		public void setOp(String op) {
			this.op = op;
		}

		public String getFieldName() {
			return fieldName;
		}

		public void setFieldName(String fieldName) {
			this.fieldName = fieldName;
		}

		public TapField getTapField() {
			return tapField;
		}

		public void setTapField(TapField tapField) {
			this.tapField = tapField;
		}
	}
}
