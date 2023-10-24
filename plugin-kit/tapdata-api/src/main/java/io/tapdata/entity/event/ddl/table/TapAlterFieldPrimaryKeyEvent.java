package io.tapdata.entity.event.ddl.table;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.entity.FieldAttrChange;

import java.util.ArrayList;
import java.util.List;

public class TapAlterFieldPrimaryKeyEvent extends TapTableEvent {
    public static final int TYPE = 203;

    private List<FieldAttrChange<List<String>>> primaryKeyChanges;
    public TapAlterFieldPrimaryKeyEvent change(FieldAttrChange<List<String>> change) {
        if(primaryKeyChanges == null)
            primaryKeyChanges = new ArrayList<>();
        if(change != null && primaryKeyChanges.contains(change))
            primaryKeyChanges.add(change);
        return this;
    }
    public TapAlterFieldPrimaryKeyEvent() {
        super(TYPE);
    }

    public void clone(TapEvent tapEvent) {
        super.clone(tapEvent);
        if (tapEvent instanceof TapAlterFieldPrimaryKeyEvent) {
            TapAlterFieldPrimaryKeyEvent alterFieldPrimaryKeyEvent = (TapAlterFieldPrimaryKeyEvent) tapEvent;
            if (primaryKeyChanges != null)
                alterFieldPrimaryKeyEvent.primaryKeyChanges = new ArrayList<>(primaryKeyChanges);
        }
    }

    public List<FieldAttrChange<List<String>>> getPrimaryKeyChanges() {
        return primaryKeyChanges;
    }

    public void setPrimaryKeyChanges(List<FieldAttrChange<List<String>>> primaryKeyChanges) {
        this.primaryKeyChanges = primaryKeyChanges;
    }

    @Override
    public String toString() {
        return "TapAlterFieldPrimaryKeyEvent{" +
                "tableId='" + tableId + '\'' +
                ", primaryKeyChanges=" + primaryKeyChanges +
                '}';
    }
}
