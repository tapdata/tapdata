package io.tapdata.connector.dameng.cdc;

import java.io.Serializable;

/**
 * @author lemon
 */
public class DamengOffset implements Serializable {


    private String sortString;
    private Long offsetValue;
    private Long lastScn;
    private Long pendingScn;
    private Long timestamp;

    public DamengOffset() {

    }

    public DamengOffset(String sortString, Long offsetValue) {
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

    public Long getLastScn() {
        return lastScn;
    }

    public void setLastScn(Long lastScn) {
        this.lastScn = lastScn;
    }

    public Long getPendingScn() {
        return pendingScn;
    }

    public void setPendingScn(Long pendingScn) {
        this.pendingScn = pendingScn;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }
}
