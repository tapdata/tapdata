package io.tapdata.flow.engine.V2.util;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public enum SyncTypeEnum {
    /**
     * 初始化全量采集
     */
    INITIAL_SYNC("initial_sync"),
    /**
     * 增量采集
     */
    CDC("cdc"),
    /**
     * 初始化全量 和 增量采集
     */
    INITIAL_SYNC_CDC("initial_sync+cdc");

    private static final Map<String, SyncTypeEnum> ENUM_MAP;

    private String syncType;

    SyncTypeEnum(String syncType) {
        this.syncType = syncType;
    }

    public String getSyncType() {
        return syncType;
    }

    static {
        Map<String, SyncTypeEnum> map = new ConcurrentHashMap<>();
        for (SyncTypeEnum instance : SyncTypeEnum.values()) {
            map.put(instance.syncType.toLowerCase(), instance);
        }
        ENUM_MAP = Collections.unmodifiableMap(map);
    }

    public static SyncTypeEnum get(String name) {
        return ENUM_MAP.get(name.toLowerCase());
    }
}
