package io.tapdata.entity.event.ddl.table;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.entity.FieldAttrChange;
import io.tapdata.entity.event.ddl.entity.ValueChange;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

public class TapAlterFieldAttributesEvent extends TapFieldBaseEvent {
    public static final int TYPE = 302;
    private String fieldName;
    private ValueChange<String> dataTypeChange;
    private ValueChange<String> checkChange;
    private ValueChange<String> constraintChange;
    private ValueChange<Boolean> notNullChange;
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
    public TapAlterFieldAttributesEvent notNull(ValueChange<Boolean> change) {
        notNullChange = change;
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
    public TapAlterFieldAttributesEvent() {
        super(TYPE);
    }

    public void clone(TapEvent tapEvent) {
        super.clone(tapEvent);
        if (tapEvent instanceof TapAlterFieldAttributesEvent) {
            TapAlterFieldAttributesEvent alterFieldAttributesEvent = (TapAlterFieldAttributesEvent) tapEvent;
            if(fieldName != null) {
                alterFieldAttributesEvent.fieldName = fieldName;
            }
            if(primaryChange != null) {
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
            if (notNullChange != null) {
                alterFieldAttributesEvent.notNullChange = new ValueChange<>();
                notNullChange.clone(alterFieldAttributesEvent.notNullChange);
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

    public ValueChange<Boolean> getNotNullChange() {
        return notNullChange;
    }

    public void setNotNullChange(ValueChange<Boolean> notNullChange) {
        this.notNullChange = notNullChange;
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
}
