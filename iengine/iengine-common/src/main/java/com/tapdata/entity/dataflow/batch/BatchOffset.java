package com.tapdata.entity.dataflow.batch;

import java.io.Serializable;

public class BatchOffset implements Serializable {
    private static final long serialVersionUID = 5599838762323297718L;

    public BatchOffset() {
    }

    public BatchOffset(Object offset, String status) {
        this.offset = offset;
        this.status = status;
    }

    /**
     * table offset
     * */
    Object offset;
    /**
     * table batch read status: over | running
     * */
    String status;

    public Object getOffset() {
        return offset;
    }

    public void setOffset(Object offset) {
        this.offset = offset;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

}
