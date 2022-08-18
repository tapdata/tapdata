package com.tapdata.tm.autoinspect.entity;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.LinkedHashMap;
import java.util.Map;

@Setter
@Getter
public class CompareEvent {

    private @NonNull String tableName;
    private @NonNull LinkedHashMap<String, Object> originalKeymap;
    private @NonNull Map<String, Object> data;
    private Object offset;

    public CompareEvent(@NonNull String tableName, @NonNull LinkedHashMap<String, Object> originalKeymap, @NonNull Map<String, Object> data, Object offset) {
        this.tableName = tableName;
        this.originalKeymap = originalKeymap;
        this.data = data;
        this.offset = offset;
    }
}
