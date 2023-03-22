package io.tapdata.connector.doris.bean;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class DorisSnapshotOffset implements Serializable {
    private Map<String, Object> offset;

    public DorisSnapshotOffset() {
        offset = new HashMap<>();
    }

    public Map<String, Object> getOffset() {
        return offset;
    }

    public void setOffset(Map<String, Object> offset) {
        this.offset = offset;
    }
}
