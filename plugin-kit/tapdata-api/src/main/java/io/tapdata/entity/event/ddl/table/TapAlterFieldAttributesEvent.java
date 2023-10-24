package io.tapdata.entity.event.ddl.table;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.entity.ValueChange;
import io.tapdata.entity.schema.TapField;

import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public class TapAlterFieldAttributesEvent extends TapFieldBaseEvent {
	public static final int TYPE = 201;
	private String fieldName;
	private ValueChange<String> dataTypeChange;
	private ValueChange<String> checkChange;
	private ValueChange<String> constraintChange;
	private ValueChange<Boolean> nullableChange;
	private ValueChange<String> commentChange;
	private ValueChange<Object> defaultChange;
	private ValueChange<Integer> primaryChange;

	public TapAlterFieldAttributesEvent fieldName(String fieldName) {
		this.fieldName = fieldName;
		return this;
	}

	public TapAlterFieldAttributesEvent primaryChange(ValueChange<Integer> change) {
		primaryChange = change;
		return this;
	}

	public TapAlterFieldAttributesEvent defaultChange(ValueChange<Object> change) {
		defaultChange = change;
		return this;
	}

	public TapAlterFieldAttributesEvent comment(ValueChange<String> change) {
		commentChange = change;
		return this;
	}

	public TapAlterFieldAttributesEvent nullable(ValueChange<Boolean> change) {
		nullableChange = change;
		return this;
	}

	public TapAlterFieldAttributesEvent constraint(ValueChange<String> change) {
		constraintChange = change;
		return this;
	}

	public TapAlterFieldAttributesEvent check(ValueChange<String> change) {
		checkChange = change;
		return this;
	}

	public TapAlterFieldAttributesEvent dataType(ValueChange<String> change) {
		dataTypeChange = change;
		return this;
	}

	public void isDataTypeChange(BiConsumer<ValueChange<String>, Object> consumer, Object object) {
		if (null != dataTypeChange) {
			consumer.accept(dataTypeChange, object);
		}
	}

	public void isNullableChange(BiConsumer<ValueChange<Boolean>, Object> consumer, Object object) {
		if (null != nullableChange) {
			consumer.accept(nullableChange, object);
		}
	}

	public void isDefaultValueChange(BiConsumer<ValueChange<Object>, Object> consumer, Object object) {
		if (null != defaultChange) {
			consumer.accept(defaultChange, object);
		}
	}

	public void isCheckChange(BiConsumer<ValueChange<String>, Object> consumer, Object object) {
		if (null != checkChange && null != consumer) {
			consumer.accept(checkChange, object);
		}
	}

	public void isCommentChange(BiConsumer<ValueChange<String>, Object> consumer, Object object) {
		if (null != commentChange && null != consumer) {
			consumer.accept(commentChange, object);
		}
	}

	public void isPrimaryChange(BiConsumer<ValueChange<Integer>, Object> consumer, Object object) {
		if (null != primaryChange && null != consumer) {
			consumer.accept(primaryChange, object);
		}
	}

	public void isConstraintChange(BiConsumer<ValueChange<String>, Object> consumer, Object object) {
		if (null != constraintChange && null != consumer) {
			consumer.accept(constraintChange, object);
		}
	}

	public TapAlterFieldAttributesEvent() {
		super(TYPE);
	}

	public void clone(TapEvent tapEvent) {
		super.clone(tapEvent);
		if (tapEvent instanceof TapAlterFieldAttributesEvent) {
			TapAlterFieldAttributesEvent alterFieldAttributesEvent = (TapAlterFieldAttributesEvent) tapEvent;
			if (fieldName != null) {
				alterFieldAttributesEvent.fieldName = fieldName;
			}
			if (primaryChange != null) {
				alterFieldAttributesEvent.primaryChange = new ValueChange<>();
				primaryChange.clone(alterFieldAttributesEvent.primaryChange);
			}
			if (dataTypeChange != null) {
				alterFieldAttributesEvent.dataTypeChange = new ValueChange<>();
				dataTypeChange.clone(alterFieldAttributesEvent.dataTypeChange);
			}
			if (checkChange != null) {
				alterFieldAttributesEvent.checkChange = new ValueChange<>();
				checkChange.clone(alterFieldAttributesEvent.checkChange);
			}
			if (nullableChange != null) {
				alterFieldAttributesEvent.nullableChange = new ValueChange<>();
				nullableChange.clone(alterFieldAttributesEvent.nullableChange);
			}
			if (defaultChange != null) {
				alterFieldAttributesEvent.defaultChange = new ValueChange<>();
				defaultChange.clone(alterFieldAttributesEvent.defaultChange);
			}
			if (commentChange != null) {
				alterFieldAttributesEvent.commentChange = new ValueChange<>();
				commentChange.clone(alterFieldAttributesEvent.commentChange);
			}
			if (constraintChange != null) {
				alterFieldAttributesEvent.constraintChange = new ValueChange<>();
				constraintChange.clone(alterFieldAttributesEvent.constraintChange);
			}
		}
	}

	public ValueChange<String> getDataTypeChange() {
		return dataTypeChange;
	}

	public void setDataTypeChange(ValueChange<String> dataTypeChange) {
		this.dataTypeChange = dataTypeChange;
	}

	public ValueChange<String> getCheckChange() {
		return checkChange;
	}

	public void setCheckChange(ValueChange<String> checkChange) {
		this.checkChange = checkChange;
	}

	public ValueChange<String> getConstraintChange() {
		return constraintChange;
	}

	public void setConstraintChange(ValueChange<String> constraintChange) {
		this.constraintChange = constraintChange;
	}

	public ValueChange<Boolean> getNullableChange() {
		return nullableChange;
	}

	public void setNullableChange(ValueChange<Boolean> nullableChange) {
		this.nullableChange = nullableChange;
	}

	public ValueChange<String> getCommentChange() {
		return commentChange;
	}

	public void setCommentChange(ValueChange<String> commentChange) {
		this.commentChange = commentChange;
	}

	public ValueChange<Object> getDefaultChange() {
		return defaultChange;
	}

	public void setDefaultChange(ValueChange<Object> defaultChange) {
		this.defaultChange = defaultChange;
	}

	public ValueChange<Integer> getPrimaryChange() {
		return primaryChange;
	}

	public void setPrimaryChange(ValueChange<Integer> primaryChange) {
		this.primaryChange = primaryChange;
	}

	public String getFieldName() {
		return fieldName;
	}

	public void setFieldName(String fieldName) {
		this.fieldName = fieldName;
	}

	@Override
	public String toString() {
		return "TapAlterFieldAttributesEvent{" +
				"tableId='" + tableId + '\'' +
				", fieldName='" + fieldName + '\'' +
				", dataTypeChange=" + dataTypeChange +
				", checkChange=" + checkChange +
				", constraintChange=" + constraintChange +
				", nullableChange=" + nullableChange +
				", commentChange=" + commentChange +
				", defaultChange=" + defaultChange +
				", primaryChange=" + primaryChange +
				'}';
	}
}
