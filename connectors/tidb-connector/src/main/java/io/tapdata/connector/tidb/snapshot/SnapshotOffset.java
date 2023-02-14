package io.tapdata.connector.tidb.snapshot;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * @author Dexter
 */
public class SnapshotOffset implements Serializable {

    private String sortString;
    private Long offsetValue;

    private Long timestamp;

    private String sourceOffset;

    private Map<String, Long> tableOffset = new HashMap<>();


    public Map<String, Long> tableOffset() {
        return this.tableOffset;
    }

    public Long timestamp(){
        return this.timestamp;
    }

    public SnapshotOffset timestamp(Long timestamp){
        this.timestamp = timestamp;
        return this;
    }

    public SnapshotOffset tableOffset(Map<String, Long> tableOffset) {
        this.tableOffset = tableOffset;
        return this;
    }
    public SnapshotOffset tableOffset(String tableId, Long time) {
        if (Objects.isNull(this.tableOffset)){
            this.tableOffset = new HashMap<>();
        }
        this.tableOffset.put(tableId,time);
        return this;
    }

    public SnapshotOffset() {
    }

    public SnapshotOffset(String sortString, Long offsetValue) {
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
