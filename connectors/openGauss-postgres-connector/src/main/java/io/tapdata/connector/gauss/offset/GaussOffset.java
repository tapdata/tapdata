package io.tapdata.connector.gauss.offset;

import java.io.Serializable;

public class GaussOffset implements Serializable {

    private String sortString;
    private Long offsetValue;

    private String sourceOffset;

    public GaussOffset() {
    }

    public GaussOffset(String sortString, Long offsetValue) {
        this.sortString = sortString;
        this.offsetValue = offsetValue;
    }

    public String getSortString() {
        return sortString;
    }

    public void setSortString(String sortString) {
        this.sortString = sortString;
    }

    public Long getOffsetValue() {
        return offsetValue;
    }

    public void setOffsetValue(Long offsetValue) {
        this.offsetValue = offsetValue;
    }

    public String getSourceOffset() {
        return sourceOffset;
    }

    public void setSourceOffset(String sourceOffset) {
        this.sourceOffset = sourceOffset;
    }
}