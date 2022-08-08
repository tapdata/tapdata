package io.tapdata.entity.event.ddl.table;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.entity.FieldAttrChange;
import io.tapdata.entity.event.ddl.entity.ValueChange;

import java.util.ArrayList;
import java.util.List;

public class TapAlterFieldNameEvent extends TapFieldBaseEvent {
    public static final int TYPE = 202;
    private ValueChange<String> nameChange;
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
}
