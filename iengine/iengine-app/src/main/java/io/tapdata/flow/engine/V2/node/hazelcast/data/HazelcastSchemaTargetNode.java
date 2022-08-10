package io.tapdata.flow.engine.V2.node.hazelcast.data;


import com.hazelcast.jet.core.Inbox;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.TapValue;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import io.tapdata.schema.TapTableMap;
import io.tapdata.schema.TapTableUtil;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.voovan.tools.collection.CacheMap;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;

public class HazelcastSchemaTargetNode extends HazelcastVirtualTargetNode {

	private final static Logger logger = LogManager.getLogger(HazelcastSchemaTargetNode.class);

	/**
	 * key: subTaskId+jsNodeId
	 */
	private static final CacheMap<String, TapTable> tabTableCacheMap = new CacheMap<>();
	private static final CacheMap<String, List<SchemaApplyResult>> schemaApplyResultMap = new CacheMap<>();

	private final String schemaKey;
	private final TapTableMap oldTapTableMap;

	static {
		tabTableCacheMap.maxSize(100).autoRemove(true).expire(600).interval(60).create();
		schemaApplyResultMap.maxSize(100).autoRemove(true).expire(600).interval(60).create();
	}


	public static TapTable getTapTable(String schemaKey) {
		return tabTableCacheMap.remove(schemaKey);
	}
	public static List<SchemaApplyResult> getSchemaApplyResultList(String schemaKey) {
		return schemaApplyResultMap.remove(schemaKey);
	}


	public HazelcastSchemaTargetNode(DataProcessorContext dataProcessorContext) {
		super(dataProcessorContext);
		this.schemaKey = dataProcessorContext.getTaskDto().getId().toHexString() + "-" + dataProcessorContext.getNode().getId();

		List<Node<Schema>> preNodes = getNode().predecessors();
		if (preNodes.size() != 1) {
			throw new IllegalArgumentException("HazelcastSchemaTargetNode only allows one predecessor node");
		}
		Node<?> deductionSchemaNode = preNodes.get(0);
		this.oldTapTableMap = TapTableUtil.getTapTableMap(deductionSchemaNode, null);
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
							if (!multipleTables) {
								//迁移任务，只有一张表
								tabTableCacheMap.put(schemaKey, tapTable);
							}

							if (multipleTables) {
								// 获取差异模型
								List<SchemaApplyResult> schemaApplyResults = getSchemaApplyResults(tapTable);
								schemaApplyResultMap.put(schemaKey, schemaApplyResults);
							}
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
		TapTable tapTable = new TapTable(tapEvent.getTableId());
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
	private List<SchemaApplyResult> getSchemaApplyResults(TapTable tapTable) {
		List<SchemaApplyResult> schemaApplyResults = new ArrayList<>();

		LinkedHashMap<String, TapField> newNameFieldMap = tapTable.getNameFieldMap();
		LinkedHashMap<String, TapField> oldNameFieldMap = getOldNameFieldMap(tapTable.getName());
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

	private LinkedHashMap<String, TapField> getOldNameFieldMap(String tableName) {
		if (oldTapTableMap == null || !oldTapTableMap.containsKey(tableName)) {
			return null;
		}
		return oldTapTableMap.get(tableName).getNameFieldMap();
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
