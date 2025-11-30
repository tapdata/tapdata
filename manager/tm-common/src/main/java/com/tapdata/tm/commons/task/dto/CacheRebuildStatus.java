package com.tapdata.tm.commons.task.dto;

public enum CacheRebuildStatus {
    PENDING,
    RUNNING,
    DONE,
    ERROR
    ;
    public static CacheRebuildStatus fromValue(String value) {
        for (CacheRebuildStatus status : CacheRebuildStatus.values()) {
            if (status.name().equalsIgnoreCase(value)) {
                return status;
            }
        }
        return null;
    }
}
