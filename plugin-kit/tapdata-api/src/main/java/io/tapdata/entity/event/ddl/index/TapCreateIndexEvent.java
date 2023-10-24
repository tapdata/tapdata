package io.tapdata.entity.event.ddl.index;


import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.TapIndex;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class TapCreateIndexEvent extends TapIndexEvent {
    public static final int TYPE = 101;
    private List<TapIndex> indexList;

    public TapCreateIndexEvent() {
        super(TYPE);
    }

    public TapCreateIndexEvent indexList(List<TapIndex> indexList) {
        this.indexList = indexList;
        return this;
    }

    public List<TapIndex> getIndexList() {
        return indexList;
    }

    public void setIndexList(List<TapIndex> indexList) {
        this.indexList = indexList;
    }

    @Override
    public void clone(TapEvent tapEvent) {
        super.clone(tapEvent);
        if(tapEvent instanceof TapCreateIndexEvent) {
            TapCreateIndexEvent tapCreateIndexEvent = (TapCreateIndexEvent) tapEvent;
            tapCreateIndexEvent.indexList = new ArrayList<>(indexList);
        }
    }
}
