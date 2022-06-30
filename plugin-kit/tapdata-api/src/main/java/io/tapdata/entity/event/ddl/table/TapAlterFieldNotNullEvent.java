package io.tapdata.entity.event.ddl.table;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.entity.FieldAttrChange;

import java.util.ArrayList;
import java.util.List;

public class TapAlterFieldNotNullEvent extends TapTableEvent {
    public static final int TYPE = 309;

    private List<FieldAttrChange<Boolean>> notNullChanges;
    public TapAlterFieldNotNullEvent change(FieldAttrChange<Boolean> change) {
        if(notNullChanges == null)
            notNullChanges = new ArrayList<>();
        if(change != null && notNullChanges.contains(change))
            notNullChanges.add(change);
        return this;
    }

    public TapAlterFieldNotNullEvent() {
        super(TYPE);
    }

    public void clone(TapEvent tapEvent) {
        super.clone(tapEvent);
        if (tapEvent instanceof TapAlterFieldNotNullEvent) {
            TapAlterFieldNotNullEvent alterFieldNotNullEvent = (TapAlterFieldNotNullEvent) tapEvent;
            if (notNullChanges != null)
                alterFieldNotNullEvent.notNullChanges = new ArrayList<>(notNullChanges);
        }
    }

    public List<FieldAttrChange<Boolean>> getNotNullChanges() {
        return notNullChanges;
    }

    public void setNotNullChanges(List<FieldAttrChange<Boolean>> notNullChanges) {
        this.notNullChanges = notNullChanges;
    }
}
