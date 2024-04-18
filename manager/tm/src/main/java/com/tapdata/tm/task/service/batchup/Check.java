package com.tapdata.tm.task.service.batchup;

public enum Check {
    MORE(1),
    LESS(-1),
    EQUALS(0);
    int status;
    Check(int status) {
        this.status = status;
    }
    public boolean equalsValue(Check c) {
        return null != c && c.status == this.status;
    }
}