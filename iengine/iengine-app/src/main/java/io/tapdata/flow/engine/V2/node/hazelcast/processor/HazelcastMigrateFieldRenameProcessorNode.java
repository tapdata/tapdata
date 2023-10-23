package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import com.tapdata.constant.MapUtil;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.tm.commons.dag.process.MigrateFieldRenameProcessorNode;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

public class HazelcastMigrateFieldRenameProcessorNode extends HazelcastProcessorBaseNode {

	private static final Logger logger = LogManager.getLogger(HazelcastMigrateFieldRenameProcessorNode.class);

	private final MigrateFieldRenameProcessorNode.ApplyConfig applyConfig;
	private final MigrateFieldRenameProcessorNode.IOperator<Map<String, Object>> dataOperator = new MigrateFieldRenameProcessorNode.IOperator<Map<String, Object>>() {
		@Override
		public void renameField(Map<String, Object> param, String fromName, String toName) {
			MapUtil.replaceKey(fromName, param, toName);
		}

		@Override
		public void deleteField(Map<String, Object> param, String originalName) {
			param.remove(originalName);
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
		applyConfig = new MigrateFieldRenameProcessorNode.ApplyConfig(nodeConfig);
	}

	@Override
	protected void tryProcess(TapdataEvent tapdataEvent, BiConsumer<TapdataEvent, ProcessResult> consumer) {
		TapEvent tapEvent = tapdataEvent.getTapEvent();
		if (!(tapEvent instanceof TapBaseEvent)) {
			// No processing required
			consumer.accept(tapdataEvent, null);
		}
		String tableId = TapEventUtil.getTableId(tapEvent);
		AtomicReference<TapdataEvent> processedEvent = new AtomicReference<>();
		if (tapEvent instanceof TapRecordEvent) {
			//dml event
			Map<String, Object> after = TapEventUtil.getAfter(tapEvent);
			Map<String, Object> before = TapEventUtil.getBefore(tapEvent);

			TapEventUtil.setAfter(tapEvent, processData(after, tableId));
			TapEventUtil.setBefore(tapEvent, processData(before, tableId));

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

	private Map<String, Object> processData(Map<String, Object> data, String tableName) {
		if (null != data) {
			for (String fieldName : new HashSet<>(data.keySet())) {
				applyConfig.apply(tableName, fieldName, data, dataOperator);
			}
		}
		return data;
	}
}
