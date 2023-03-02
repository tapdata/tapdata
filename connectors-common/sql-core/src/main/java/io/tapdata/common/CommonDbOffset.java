package io.tapdata.common;

import io.tapdata.entity.utils.DataMap;

public class CommonDbOffset {

    private Long offsetSize;
    private DataMap columnValue;

    public CommonDbOffset(DataMap columnValue, Long offsetSize) {
        this.columnValue = columnValue;
        this.offsetSize = offsetSize;
    }

    public Long getOffsetSize() {
        return offsetSize;
    }

    public void setOffsetSize(Long offsetSize) {
        this.offsetSize = offsetSize;
    }

    public DataMap getColumnValue() {
        return columnValue;
    }

    public void setColumnValue(DataMap columnValue) {
        this.columnValue = columnValue;
    }
}
