package io.tapdata.entity.event.ddl.table;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.entity.FieldAttrChange;

import java.util.ArrayList;
import java.util.List;

public class TapAlterFieldDefaultEvent extends TapTableEvent {
    public static final int TYPE = 303;

    private List<FieldAttrChange<Object>> defaultChanges;
    public TapAlterFieldDefaultEvent change(FieldAttrChange<Object> change) {
        if(defaultChanges == null)
            defaultChanges = new ArrayList<>();
        if(change != null && defaultChanges.contains(change))
            defaultChanges.add(change);
        return this;
    }

    public TapAlterFieldDefaultEvent() {
        super(TYPE);
    }
    public void clone(TapEvent tapEvent) {
        super.clone(tapEvent);
        if (tapEvent instanceof TapAlterFieldDefaultEvent) {
            TapAlterFieldDefaultEvent alterFieldDefaultEvent = (TapAlterFieldDefaultEvent) tapEvent;
            if (defaultChanges != null)
                alterFieldDefaultEvent.defaultChanges = new ArrayList<>(defaultChanges);
        }
    }

    public List<FieldAttrChange<Object>> getDefaultChanges() {
        return defaultChanges;
    }

    public void setDefaultChanges(List<FieldAttrChange<Object>> defaultChanges) {
        this.defaultChanges = defaultChanges;
    }
}
