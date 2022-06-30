package io.tapdata.entity.event.ddl.table;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.entity.FieldAttrChange;

import java.util.ArrayList;
import java.util.List;

public class TapAlterFieldNameEvent extends TapTableEvent {
    public static final int TYPE = 304;
    private List<FieldAttrChange<String>> nameChanges;
    public TapAlterFieldNameEvent change(FieldAttrChange<String> change) {
        if(nameChanges == null)
            nameChanges = new ArrayList<>();
        if(change != null && nameChanges.contains(change))
            nameChanges.add(change);
        return this;
    }
    public TapAlterFieldNameEvent() {
        super(TYPE);
    }
    public void clone(TapEvent tapEvent) {
        super.clone(tapEvent);
        if (tapEvent instanceof TapAlterFieldNameEvent) {
            TapAlterFieldNameEvent alterFieldDataTypeEvent = (TapAlterFieldNameEvent) tapEvent;
            if (nameChanges != null)
                alterFieldDataTypeEvent.nameChanges = new ArrayList<>(nameChanges);
        }
    }

    public List<FieldAttrChange<String>> getNameChanges() {
        return nameChanges;
    }

    public void setNameChanges(List<FieldAttrChange<String>> nameChanges) {
        this.nameChanges = nameChanges;
    }
}
