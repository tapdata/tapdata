package io.tapdata.entity.event.ddl.table;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.entity.FieldAttrChange;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

public class TapAlterFieldAttributesEvent extends TapFieldBaseEvent {
    public static final int TYPE = 302;
    private FieldAttrChange<String> dataTypeChange;
    private FieldAttrChange<String> checkChange;
    private FieldAttrChange<String> constraintChange;
    private FieldAttrChange<Boolean> notNullChange;
    private FieldAttrChange<String> commentChange;
    private FieldAttrChange<Object> defaultChange;
    public TapAlterFieldAttributesEvent defaultChange(FieldAttrChange<Object> change) {
        defaultChange = change;
        return this;
    }
    public TapAlterFieldAttributesEvent comment(FieldAttrChange<String> change) {
        commentChange = change;
        return this;
    }
    public TapAlterFieldAttributesEvent notNull(FieldAttrChange<Boolean> change) {
        notNullChange = change;
        return this;
    }
    public TapAlterFieldAttributesEvent constraint(FieldAttrChange<String> change) {
        constraintChange = change;
        return this;
    }
    public TapAlterFieldAttributesEvent check(FieldAttrChange<String> change) {
        checkChange = change;
        return this;
    }
    public TapAlterFieldAttributesEvent dataType(FieldAttrChange<String> change) {
        dataTypeChange = change;
        return this;
    }
    public TapAlterFieldAttributesEvent() {
        super(TYPE);
    }

    public void clone(TapEvent tapEvent) {
        super.clone(tapEvent);
        if (tapEvent instanceof TapAlterFieldAttributesEvent) {
            TapAlterFieldAttributesEvent alterFieldDataTypeEvent = (TapAlterFieldAttributesEvent) tapEvent;
            if (dataTypeChange != null) {
                alterFieldDataTypeEvent.dataTypeChange = new FieldAttrChange<>();
                dataTypeChange.clone(alterFieldDataTypeEvent.dataTypeChange);
            }
            if (checkChange != null) {
                alterFieldDataTypeEvent.checkChange = new FieldAttrChange<>();
                checkChange.clone(alterFieldDataTypeEvent.checkChange);
            }
            if (notNullChange != null) {
                alterFieldDataTypeEvent.notNullChange = new FieldAttrChange<>();
                notNullChange.clone(alterFieldDataTypeEvent.notNullChange);
            }
            if (defaultChange != null) {
                alterFieldDataTypeEvent.defaultChange = new FieldAttrChange<>();
                defaultChange.clone(alterFieldDataTypeEvent.defaultChange);
            }
            if (commentChange != null) {
                alterFieldDataTypeEvent.commentChange = new FieldAttrChange<>();
                commentChange.clone(alterFieldDataTypeEvent.commentChange);
            }
            if (constraintChange != null) {
                alterFieldDataTypeEvent.constraintChange = new FieldAttrChange<>();
                constraintChange.clone(alterFieldDataTypeEvent.constraintChange);
            }
        }
    }

    public FieldAttrChange<String> getDataTypeChange() {
        return dataTypeChange;
    }

    public void setDataTypeChange(FieldAttrChange<String> dataTypeChange) {
        this.dataTypeChange = dataTypeChange;
    }

    public FieldAttrChange<String> getCheckChange() {
        return checkChange;
    }

    public void setCheckChange(FieldAttrChange<String> checkChange) {
        this.checkChange = checkChange;
    }

    public FieldAttrChange<String> getConstraintChange() {
        return constraintChange;
    }

    public void setConstraintChange(FieldAttrChange<String> constraintChange) {
        this.constraintChange = constraintChange;
    }

    public FieldAttrChange<Boolean> getNotNullChange() {
        return notNullChange;
    }

    public void setNotNullChange(FieldAttrChange<Boolean> notNullChange) {
        this.notNullChange = notNullChange;
    }

    public FieldAttrChange<String> getCommentChange() {
        return commentChange;
    }

    public void setCommentChange(FieldAttrChange<String> commentChange) {
        this.commentChange = commentChange;
    }

    public FieldAttrChange<Object> getDefaultChange() {
        return defaultChange;
    }

    public void setDefaultChange(FieldAttrChange<Object> defaultChange) {
        this.defaultChange = defaultChange;
    }
}
