package io.tapdata.entity.event.ddl.table;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.entity.FieldAttrChange;

import java.util.ArrayList;
import java.util.List;

public class TapAlterFieldDataTypeEvent extends TapTableEvent {
    public static final int TYPE = 302;
    private List<FieldAttrChange<String>> dataTypeChanges;
    public TapAlterFieldDataTypeEvent change(FieldAttrChange<String> change) {
        if(dataTypeChanges == null)
            dataTypeChanges = new ArrayList<>();
        if(change != null && dataTypeChanges.contains(change))
            dataTypeChanges.add(change);
        return this;
    }
    public TapAlterFieldDataTypeEvent() {
        super(TYPE);
    }

    public void clone(TapEvent tapEvent) {
        super.clone(tapEvent);
        if (tapEvent instanceof TapAlterFieldDataTypeEvent) {
            TapAlterFieldDataTypeEvent alterFieldDataTypeEvent = (TapAlterFieldDataTypeEvent) tapEvent;
            if (dataTypeChanges != null)
                alterFieldDataTypeEvent.dataTypeChanges = new ArrayList<>(dataTypeChanges);
        }
    }

    public List<FieldAttrChange<String>> getDataTypeChanges() {
        return dataTypeChanges;
    }

    public void setDataTypeChanges(List<FieldAttrChange<String>> dataTypeChanges) {
        this.dataTypeChanges = dataTypeChanges;
    }
}
