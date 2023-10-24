package io.tapdata.entity.event.ddl.table;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.error.TapAPIErrorCodes;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.entity.ValueChange;

import java.util.ArrayList;
import java.util.List;

public class TapRenameTableEvent extends TapTableEvent {
    public static final int TYPE = 210;
    private List<ValueChange<String>> nameChanges;
    public TapRenameTableEvent change(ValueChange<String> change) {
        if(nameChanges == null)
            nameChanges = new ArrayList<>();
        if(change != null && nameChanges.contains(change)) {
            if(change.getBefore() == null || change.getAfter() == null)
                throw new CoreException(TapAPIErrorCodes.ERROR_ILLEGAL_PARAMETERS, "Missing before {} or after {} while add ValueChange for TapRenameTableEvent", change.getBefore(), change.getAfter());
            nameChanges.add(change);
        }
        return this;
    }
    public TapRenameTableEvent() {
        super(TYPE);
    }

    public void clone(TapEvent tapEvent) {
        super.clone(tapEvent);
        if (tapEvent instanceof TapRenameTableEvent) {
            TapRenameTableEvent renameTableEvent = (TapRenameTableEvent) tapEvent;
            if (nameChanges != null)
                renameTableEvent.nameChanges = new ArrayList<>(nameChanges);
        }
    }

    public List<ValueChange<String>> getNameChanges() {
        return nameChanges;
    }

    public void setNameChanges(List<ValueChange<String>> nameChanges) {
        this.nameChanges = nameChanges;
    }
}
