package io.tapdata.flow.engine.V2.ddl;

import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.entity.ValueChange;
import io.tapdata.entity.event.ddl.table.TapAlterFieldAttributesEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.event.ddl.table.TapDropFieldEvent;
import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.pretty.BiClassHandlers;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author samuel
 * @Description
 * @create 2022-07-07 21:46
 **/
public class DDLSchemaHandler {
	private static final Logger logger = LogManager.getLogger(DDLSchemaHandler.class);
	private final BiClassHandlers<TapDDLEvent, TapTable, Void> biClassHandlers;

	public DDLSchemaHandler() {
		biClassHandlers = new BiClassHandlers<>();
		biClassHandlers.register(TapNewFieldEvent.class, this::handleNewFieldEvent);
		biClassHandlers.register(TapAlterFieldAttributesEvent.class, this::handleAlterFieldAttrEvent);
		biClassHandlers.register(TapAlterFieldNameEvent.class, this::handleAlterFieldNameEvent);
		biClassHandlers.register(TapDropFieldEvent.class, this::handleDropFieldEvent);
	}

	public void updateSchemaByDDLEvent(TapDDLEvent tapDDLEvent, TapTable tapTable) {
		if (null == tapTable || null == tapTable.getNameFieldMap()) {
			return;
		}
		String tableIdFromTapTable = tapTable.getId();
		String tableIdFromEvent = tapDDLEvent.getTableId();
		if (!tableIdFromTapTable.equals(tableIdFromEvent)) {
			throw new RuntimeException("Table name(" + tableIdFromTapTable + ") in schema is not equals to table name(" + tableIdFromEvent + ") in event");
		}
		LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
		if (null == nameFieldMap) {
			throw new RuntimeException("Tap table field map is null");
		}
		biClassHandlers.handle(tapDDLEvent, tapTable);
	}

	private Void handleNewFieldEvent(TapDDLEvent tapDDLEvent, TapTable tapTable) {
		TapNewFieldEvent tapNewFieldEvent = (TapNewFieldEvent) tapDDLEvent;
		List<TapField> newFields = tapNewFieldEvent.getNewFields();
		for (TapField newField : newFields) {
			tapTable.add(newField);
		}
		return null;
	}

	private Void handleDropFieldEvent(TapDDLEvent tapDDLEvent, TapTable tapTable) {
		LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
		TapDropFieldEvent tapDropFieldEvent = (TapDropFieldEvent) tapDDLEvent;
		String fieldName = tapDropFieldEvent.getFieldName();
		nameFieldMap.remove(fieldName);
		return null;
	}

	private Void handleAlterFieldNameEvent(TapDDLEvent tapDDLEvent, TapTable tapTable) {
		LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
		TapAlterFieldNameEvent tapAlterFieldNameEvent = (TapAlterFieldNameEvent) tapDDLEvent;
		ValueChange<String> nameChange = tapAlterFieldNameEvent.getNameChange();
		if (null == nameChange) {
			return null;
		}
		String before = nameChange.getBefore();
		String after = nameChange.getAfter();
		if (StringUtils.isBlank(before)) {
			throw new RuntimeException("Invalid alter field name event, missing old field name");
		}
		if (StringUtils.isBlank(after)) {
			throw new RuntimeException("Invalid alter field name event, missing new field name");
		}
		TapField tapField = nameFieldMap.get(before);
		if (null == tapField) {
			return null;
		}
		tapField.setName(after);
		nameFieldMap.remove(before);
		nameFieldMap.put(after, tapField);
		return null;
	}

	private Void handleAlterFieldAttrEvent(TapDDLEvent tapDDLEvent, TapTable tapTable) {
		LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
		String fieldName = ((TapAlterFieldAttributesEvent) tapDDLEvent).getFieldName();
		TapField tapField = nameFieldMap.get(fieldName);
		if (null == tapField) {
			throw new RuntimeException("Cannot found field: " + fieldName + " in tap table field list: " + nameFieldMap.keySet());
		}
		TapAlterFieldAttributesEvent tapAlterFieldAttributesEvent = (TapAlterFieldAttributesEvent) tapDDLEvent;
		tapAlterFieldAttributesEvent.isDataTypeChange(this::changeFieldDataType, tapField);
		tapAlterFieldAttributesEvent.isNullableChange(this::changeFieldNullable, tapField);
		tapAlterFieldAttributesEvent.isDefaultValueChange(this::changeFieldDefaultValue, tapField);
		tapAlterFieldAttributesEvent.isCommentChange(this::changeFieldComment, tapField);
		tapAlterFieldAttributesEvent.isPrimaryChange(this::changeFieldPrimary, tapField);
		tapAlterFieldAttributesEvent.isConstraintChange(this::changeFieldConstraint, tapField);
		tapAlterFieldAttributesEvent.isCheckChange(this::changeFieldCheck, tapField);
		return null;
	}

	private void changeFieldCheck(ValueChange<String> stringValueChange, Object o) {
		TapField tapField = (TapField) o;
		String after = stringValueChange.getAfter();
		if (StringUtils.isNotBlank(after)) {
			tapField.setCheck(after);
			if (logger.isDebugEnabled()) {
				logger.debug("Change field[" + tapField.getName() + "], " + tapField.getCheck() + " to " + after);
			}
		}
	}

	private void changeFieldConstraint(ValueChange<String> stringValueChange, Object o) {
		TapField tapField = (TapField) o;
		String after = stringValueChange.getAfter();
		if (StringUtils.isNotBlank(after)) {
			tapField.setConstraint(after);
			if (logger.isDebugEnabled()) {
				logger.debug("Change field[" + tapField.getName() + "], " + tapField.getConstraint() + " to " + after);
			}
		}
	}

	private void changeFieldPrimary(ValueChange<Integer> integerValueChange, Object o) {
		TapField tapField = (TapField) o;
		Integer after = integerValueChange.getAfter();
		if (null != after) {
			tapField.primaryKeyPos(after);
			if (logger.isDebugEnabled()) {
				logger.debug("Change field[" + tapField.getName() + "] primary position, " + tapField.getPrimaryKeyPos() + " to " + after);
			}
		}
	}

	private void changeFieldComment(ValueChange<String> stringValueChange, Object o) {
		TapField tapField = (TapField) o;
		String after = stringValueChange.getAfter();
		if (StringUtils.isNotBlank(after)) {
			tapField.comment(after);
			if (logger.isDebugEnabled()) {
				logger.debug("Change field[" + tapField.getName() + "] comment, " + tapField.getComment() + " to " + after);
			}
		}
	}

	private void changeFieldDefaultValue(ValueChange<Object> objectValueChange, Object o) {
		TapField tapField = (TapField) o;
		Object after = objectValueChange.getAfter();
		if (null != after) {
			tapField.defaultValue(after);
			if (logger.isDebugEnabled()) {
				logger.debug("Change field[" + tapField.getName() + "] default, " + tapField.getDefaultValue() + " to " + after);
			}
		}
	}

	private void changeFieldNullable(ValueChange<Boolean> booleanValueChange, Object o) {
		TapField tapField = (TapField) o;
		Boolean after = booleanValueChange.getAfter();
		if (null != after) {
			tapField.nullable(after);
			if (logger.isDebugEnabled()) {
				logger.debug("Change field[" + tapField.getName() + "] nullable, " + tapField.getNullable() + " to " + after);
			}
		}
	}

	private void changeFieldDataType(ValueChange<String> stringValueChange, Object o) {
		TapField tapField = (TapField) o;
		String after = stringValueChange.getAfter();
		if (StringUtils.isNotBlank(after)) {
			tapField.dataType(after);
			if (logger.isDebugEnabled()) {
				logger.debug("Change field[" + tapField.getName() + "] data type, " + tapField.getDataType() + " to " + after);
			}
		}
	}
}
