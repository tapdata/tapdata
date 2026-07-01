package com.tapdata.entity;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/6/26 10:58 Create
 * @description
 */
public class TapdataBeginTableSnapshotEvent extends TapdataEvent {
    private static final long serialVersionUID = 265422482461825374L;

    private String sourceTableName;

    public TapdataBeginTableSnapshotEvent(){}

    public TapdataBeginTableSnapshotEvent(String sourceTableName) {
        this.sourceTableName = sourceTableName;
    }

    public String getSourceTableName() {
        return sourceTableName;
    }

    private void setSourceTableName(String sourceTableName) {
        this.sourceTableName = sourceTableName;
    }

    @Override
    protected void clone(TapdataEvent tapdataEvent) {
        super.clone(tapdataEvent);
        ((TapdataBeginTableSnapshotEvent) tapdataEvent).setSourceTableName(sourceTableName);
    }

    @Override
    public boolean isConcurrentWrite() {
        return false;
    }
}
