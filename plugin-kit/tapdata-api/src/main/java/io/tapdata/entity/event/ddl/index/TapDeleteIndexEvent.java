package io.tapdata.entity.event.ddl.index;

import io.tapdata.entity.event.TapEvent;

import java.util.ArrayList;
import java.util.List;

public class TapDeleteIndexEvent extends TapIndexEvent {
    public static final int TYPE = 100;
    private List<String> indexNames;

    public TapDeleteIndexEvent() {
        super(TYPE);
    }

    public TapDeleteIndexEvent indexNames(List<String> indexNames) {
        this.indexNames = indexNames;
        return this;
    }

    public List<String> getIndexNames() {
        return indexNames;
    }

    public void setIndexNames(List<String> indexNames) {
        this.indexNames = indexNames;
    }

    @Override
    public void clone(TapEvent tapEvent) {
        super.clone(tapEvent);
        if(tapEvent instanceof TapDeleteIndexEvent) {
            TapDeleteIndexEvent tapDeleteIndexEvent = (TapDeleteIndexEvent) tapEvent;
            tapDeleteIndexEvent.indexNames = new ArrayList<>(indexNames);
        }
    }
}
