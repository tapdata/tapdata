package io.tapdata.entity.aspect;

import io.tapdata.entity.schema.TapTable;

public class SampleAspect extends Aspect {
    private TapTable table;
    public SampleAspect table(TapTable table) {
        this.table = table;
        return this;
    }

    public TapTable getTable() {
        return table;
    }

    public void setTable(TapTable table) {
        this.table = table;
    }
}
