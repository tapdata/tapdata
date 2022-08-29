package io.tapdata.entity.event.ddl.table;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.entity.ValueChange;
import io.tapdata.entity.schema.TapField;

public class TapAlterFieldNameEvent extends TapFieldBaseEvent {
    public static final int TYPE = 202;
    private ValueChange<String> nameChange;
    private TapField oldField;
    public TapAlterFieldNameEvent nameChange(ValueChange<String> change) {
        this.nameChange = change;
        return this;
    }
    public TapAlterFieldNameEvent() {
        super(TYPE);
    }
    public void clone(TapEvent tapEvent) {
        super.clone(tapEvent);
        if (tapEvent instanceof TapAlterFieldNameEvent) {
            TapAlterFieldNameEvent alterFieldDataTypeEvent = (TapAlterFieldNameEvent) tapEvent;
            if (nameChange != null)
                alterFieldDataTypeEvent.nameChange = nameChange;
        }
    }

    public ValueChange<String> getNameChange() {
        return nameChange;
    }

    public void setNameChange(ValueChange<String> nameChange) {
        this.nameChange = nameChange;
    }

    public TapField getOldField() {
        return oldField;
    }

    public void setOldField(TapField oldField) {
        this.oldField = oldField;
    }
}
