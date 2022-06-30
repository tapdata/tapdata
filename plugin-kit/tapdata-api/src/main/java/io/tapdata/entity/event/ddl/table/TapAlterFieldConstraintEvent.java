package io.tapdata.entity.event.ddl.table;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.entity.FieldAttrChange;

import java.util.ArrayList;
import java.util.List;

public class TapAlterFieldConstraintEvent extends TapTableEvent {
    public static final int TYPE = 301;
    private List<FieldAttrChange<String>> constraintChanges;
    public TapAlterFieldConstraintEvent change(FieldAttrChange<String> change) {
        if(constraintChanges == null)
            constraintChanges = new ArrayList<>();
        if(change != null && constraintChanges.contains(change))
            constraintChanges.add(change);
        return this;
    }
    public TapAlterFieldConstraintEvent() {
        super(TYPE);
    }
    public void clone(TapEvent tapEvent) {
        super.clone(tapEvent);
        if (tapEvent instanceof TapAlterFieldConstraintEvent) {
            TapAlterFieldConstraintEvent alterFieldConstraintEvent = (TapAlterFieldConstraintEvent) tapEvent;
            if (constraintChanges != null)
                alterFieldConstraintEvent.constraintChanges = new ArrayList<>(constraintChanges);
        }
    }

    public List<FieldAttrChange<String>> getConstraintChanges() {
        return constraintChanges;
    }

    public void setConstraintChanges(List<FieldAttrChange<String>> constraintChanges) {
        this.constraintChanges = constraintChanges;
    }
}
