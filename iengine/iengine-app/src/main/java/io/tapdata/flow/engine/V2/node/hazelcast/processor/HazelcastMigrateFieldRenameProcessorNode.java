package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import com.tapdata.constant.MapUtilV2;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.tm.commons.dag.process.MigrateFieldRenameProcessorNode;
import com.tapdata.tm.commons.dag.vo.FieldInfo;
import io.tapdata.entity.event.TapBaseEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.entity.FieldAttrChange;
import io.tapdata.entity.event.ddl.index.TapCreateIndexEvent;
import io.tapdata.entity.event.ddl.table.*;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

public class HazelcastMigrateFieldRenameProcessorNode extends HazelcastProcessorBaseNode {

	private static final Logger logger = LogManager.getLogger(HazelcastMigrateFieldRenameProcessorNode.class);

	private final DataExecutor applyConfig;
	private final MigrateFieldRenameProcessorNode.IOperator<Map<String, Object>> dataOperator = new MigrateFieldRenameProcessorNode.IOperator<Map<String, Object>>() {
		@Override
		public void renameField(Map<String, Object> param, String fromName, String toName) {
			MapUtilV2.replaceKey(fromName, param, toName);
		}

		@Override
		public void deleteField(Map<String, Object> param, String originalName) {
			MapUtilV2.removeValueByKey(param, originalName);
		}

		@Override
		public Object renameFieldWithReturn(Map<String, Object> param, String fromName, String toName) {
			Object value = param.remove(fromName);
			param.put(toName, value);
			return value;
		}
	};
	private final MigrateFieldRenameProcessorNode.IOperator<TapAlterFieldAttributesEvent> alterFieldOperator = new MigrateFieldRenameProcessorNode.IOperator<TapAlterFieldAttributesEvent>() {
		@Override
		public void renameField(TapAlterFieldAttributesEvent param, String fromName, String toName) {
			param.setFieldName(toName);
		}
	};
	private final MigrateFieldRenameProcessorNode.IOperator<TapField> newFieldOperator = new MigrateFieldRenameProcessorNode.IOperator<TapField>() {
		@Override
		public void renameField(TapField param, String fromName, String toName) {
			param.setName(toName);
		}
	};
	private final MigrateFieldRenameProcessorNode.IOperator<TapAlterFieldNameEvent> alterFieldNameOperator = new MigrateFieldRenameProcessorNode.IOperator<TapAlterFieldNameEvent>() {
		@Override
		public void renameField(TapAlterFieldNameEvent param, String fromName, String toName) {
			param.getNameChange().setBefore(toName);
		}
	};
	private final MigrateFieldRenameProcessorNode.IOperator<TapDropFieldEvent> dropFieldOperator = new MigrateFieldRenameProcessorNode.IOperator<TapDropFieldEvent>() {
		@Override
		public void renameField(TapDropFieldEvent param, String fromName, String toName) {
			param.setFieldName(toName);
		}
	};
	private final MigrateFieldRenameProcessorNode.IOperator<ListIterator<String>> alterFieldPrimaryKeyOperator = new MigrateFieldRenameProcessorNode.IOperator<ListIterator<String>>() {
		@Override
		public void deleteField(ListIterator<String> param, String originalName) {
			param.remove();
		}

		@Override
		public void renameField(ListIterator<String> param, String fromName, String toName) {
			param.set(toName);
		}
	};
	private final MigrateFieldRenameProcessorNode.IOperator<ListIterator<TapIndexField>> createIndexEventOperator = new MigrateFieldRenameProcessorNode.IOperator<ListIterator<TapIndexField>>() {
		@Override
		public void deleteField(ListIterator<TapIndexField> param, String originalName) {
			param.remove();
		}

		@Override
		public void renameField(ListIterator<TapIndexField> param, String fromName, String toName) {
			TapIndexField current = param.previous();
			param.next(); // reset the cursor

			current.setName(toName);
		}
	};

	public HazelcastMigrateFieldRenameProcessorNode(ProcessorBaseContext processorBaseContext) {
		super(processorBaseContext);
		MigrateFieldRenameProcessorNode nodeConfig = (MigrateFieldRenameProcessorNode) getNode();
		applyConfig = new DataExecutor(nodeConfig);
	}

	@Override
	protected void tryProcess(TapdataEvent tapdataEvent, BiConsumer<TapdataEvent, ProcessResult> consumer) {
		TapEvent tapEvent = tapdataEvent.getTapEvent();
		if (!(tapEvent instanceof TapBaseEvent)) {
			// No processing required
			consumer.accept(tapdataEvent, null);
		}
		AtomicReference<TapdataEvent> processedEvent = new AtomicReference<>();
		String tableId = TapEventUtil.getTableId(tapEvent);
		if (tapEvent instanceof TapRecordEvent) {
			//dml event
			applyConfig.apply(tableId, TapEventUtil.getAfter(tapEvent), dataOperator);
			applyConfig.apply(tableId, TapEventUtil.getBefore(tapEvent), dataOperator);

			processedEvent.set(tapdataEvent);
		} else {
			//ddl event
			boolean validEvent = true;
			if (tapEvent instanceof TapDDLEvent) {
				if (tapEvent instanceof TapAlterFieldAttributesEvent) {
					TapAlterFieldAttributesEvent param = (TapAlterFieldAttributesEvent) tapEvent;
					validEvent = applyConfig.apply(tableId, param.getFieldName(), param, alterFieldOperator);
				} else if (tapEvent instanceof TapNewFieldEvent) {
					TapNewFieldEvent param = (TapNewFieldEvent) tapEvent;
					param.getNewFields().removeIf(tapField -> !applyConfig.apply(tableId, tapField.getName(), tapField, newFieldOperator));
					validEvent = !param.getNewFields().isEmpty();
				} else if (tapEvent instanceof TapAlterFieldNameEvent) {
					TapAlterFieldNameEvent param = (TapAlterFieldNameEvent) tapEvent;
					validEvent = applyConfig.apply(tableId, param.getNameChange().getBefore(), param, alterFieldNameOperator);
				} else if (tapEvent instanceof TapDropFieldEvent) {
					TapDropFieldEvent param = (TapDropFieldEvent) tapEvent;
					validEvent = applyConfig.apply(tableId, param.getFieldName(), param, dropFieldOperator);
				} else if (tapEvent instanceof TapAlterFieldPrimaryKeyEvent) {
					List<FieldAttrChange<List<String>>> primaryKeyChanges = ((TapAlterFieldPrimaryKeyEvent) tapEvent).getPrimaryKeyChanges();
					ListIterator<FieldAttrChange<List<String>>> primaryKeyChangesIter = primaryKeyChanges.listIterator();
					while (primaryKeyChangesIter.hasNext()) {
						FieldAttrChange<List<String>> fieldAttrChange = primaryKeyChangesIter.next();
						List<String> before = fieldAttrChange.getBefore();
						ListIterator<String> fieldsIter = before.listIterator();

						while (fieldsIter.hasNext()) {
							String fieldName = fieldsIter.next();
							applyConfig.apply(tableId, fieldName, fieldsIter, alterFieldPrimaryKeyOperator);
						}
						if (before.isEmpty()) {
							primaryKeyChangesIter.remove();
						}
					}
					validEvent = !primaryKeyChanges.isEmpty();
				} else if (tapEvent instanceof TapCreateIndexEvent) {
					List<TapIndex> indexList = ((TapCreateIndexEvent) tapEvent).getIndexList();
					Iterator<TapIndex> indexIterator = indexList.iterator();
					while (indexIterator.hasNext()) {
						TapIndex tapIndex = indexIterator.next();
						List<TapIndexField> indexFields = tapIndex.getIndexFields();
						ListIterator<TapIndexField> tapIndexFieldListIterator = indexFields.listIterator();
						while (tapIndexFieldListIterator.hasNext()) {
							TapIndexField tapIndexField = tapIndexFieldListIterator.next();
							String fieldName = tapIndexField.getName();

							applyConfig.apply(tableId, fieldName, tapIndexFieldListIterator, createIndexEventOperator);
						}
						if (indexFields.isEmpty()) {
							indexIterator.remove();
						}
					}
					validEvent = !indexList.isEmpty();
				}
			}
			if (validEvent) {
				processedEvent.set(tapdataEvent);
			}
		}

		if (processedEvent.get() != null) {
			consumer.accept(processedEvent.get(), getProcessResult(tableId));
		} else {
			if (logger.isDebugEnabled()) {
				logger.debug("The event does not need to continue to be processed {}", tapdataEvent);
			}
		}
	}

	protected static class DataExecutor extends MigrateFieldRenameProcessorNode.ApplyConfig {
		private final Map<String, Map<String, String>> operationFieldMap;
		private final Map<String, Map<String, Boolean>> needApplyCacheMap;
		public DataExecutor(MigrateFieldRenameProcessorNode node) {
			super(node);
			this.operationFieldMap = new Object2ObjectOpenHashMap<>();
			this.needApplyCacheMap = new Object2ObjectOpenHashMap<>();
		}

		protected boolean needApply(String tableName) {
			if (StringUtils.isBlank(tableName)) {
				return false;
			}
			return needApplyCacheMap
					.computeIfAbsent(Thread.currentThread().getName(), k->new Object2ObjectOpenHashMap<>())
					.computeIfAbsent(
					tableName,
					key -> (null != fieldsOperation && !fieldsOperation.isEmpty())
							|| (null != fieldInfoMaps && MapUtils.isNotEmpty(fieldInfoMaps.get(key)))
			);
		}

		protected <T> void apply(String tableName, T operatorParam, MigrateFieldRenameProcessorNode.IOperator<T> operator) {
			if (null == operatorParam) {
				return;
			}
			if (!needApply(tableName)) {
				return;
			}
			apply(
					tableName, operatorParam, operator,
					this::applyOperation,
					this::applyFieldInfo
			);
		}

		protected <T> void apply(String tableName, T operatorParam, MigrateFieldRenameProcessorNode.IOperator<T> operator,
								 DataExecutorFunction<T>... applyFunctions) {
			if (null == operatorParam || null == applyFunctions) {
				return;
			}
			for (DataExecutorFunction<T> applyFunction : applyFunctions) {
				if (null == applyFunction) {
					continue;
				}
				if (applyFunction.apply(tableName, operatorParam, operator)) {
					break;
				}
			}
		}

		protected <T> boolean applyOperation(String tableName, T operatorParam, MigrateFieldRenameProcessorNode.IOperator<T> operator) {
			if (fieldsOperation.isEmpty()) {
				return false;
			}
			Map<String, FieldInfo> fieldInfoMap = null;
			if (null != fieldInfoMaps) {
				fieldInfoMap = fieldInfoMaps.get(tableName);
			}
			Queue<Object> queue = new LinkedList<>();
			queue.add(operatorParam);

			while (!queue.isEmpty()) {
				Object poll = queue.poll();
				if (poll instanceof Map) {
					Map<String, Object> map = (Map<String, Object>) poll;
					List<String> keys = new ArrayList<>(map.keySet());
					for (String key : keys) {
						if (null != fieldInfoMap && fieldInfoMap.containsKey(key)) {
							// Field info operation
							FieldInfo fieldInfo = fieldInfoMap.get(key);
							if (Boolean.FALSE.equals(fieldInfo.getIsShow())) {
								operator.deleteField((T) map, key);
							} else if (StringUtils.isNotBlank(fieldInfo.getTargetFieldName())) {
								String newKey = fieldInfo.getTargetFieldName();
								Object value = operator.renameFieldWithReturn((T) map, key, newKey);
								if (value instanceof Map || value instanceof List) {
									queue.add(value);
								}
							}
						} else {
							// Rule operation
							String newKey = operationFieldMap
									.computeIfAbsent(Thread.currentThread().getName(), k->new Object2ObjectOpenHashMap<>())
									.computeIfAbsent(key, k -> apply(fieldsOperation, k));
							Object value = operator.renameFieldWithReturn((T) map, key, newKey);
							if (value instanceof Map || value instanceof List) {
								queue.add(value);
							}
						}
					}
				} else if (poll instanceof List) {
					List<Object> list = (List<Object>) poll;
					for (Object item : list) {
						if (item instanceof Map || item instanceof List) {
							queue.add(item);
						}
					}
				}
			}
			return true;
		}

		protected <T> boolean applyFieldInfo(String tableName, T operatorParam, MigrateFieldRenameProcessorNode.IOperator<T> operator) {
			if (MapUtils.isEmpty(fieldInfoMaps) || !fieldInfoMaps.containsKey(tableName)) {
				return false;
			}
			Map<String, FieldInfo> fieldInfoMap = fieldInfoMaps.get(tableName);
			for (Map.Entry<String, FieldInfo> entry : fieldInfoMap.entrySet()) {
				String key = entry.getKey();
				FieldInfo fieldInfo = entry.getValue();
				if (Boolean.FALSE.equals(fieldInfo.getIsShow())) {
					operator.deleteField(operatorParam, key);
				}
				if (StringUtils.isNotBlank(fieldInfo.getTargetFieldName())) {
					operator.renameField(operatorParam, key, fieldInfo.getTargetFieldName());
				}
			}
			return true;
		}
	}

	protected interface DataExecutorFunction<T> {
		boolean apply(String tableName, T operatorParam, MigrateFieldRenameProcessorNode.IOperator<T> operator);
	}

	@Override
	public boolean needTransformValue() {
		return false;
	}

	@Override
	public boolean supportConcurrentProcess() {
		return true;
	}
}
