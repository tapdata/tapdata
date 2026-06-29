package com.tapdata.tm.userLog.constant;

import com.tapdata.tm.commons.task.dto.TaskDto;

/**
 * 随着业务增加，可以新增新的枚举值
 * 这里需要和国际化的资源属性文件里的key对应，
 */
public enum Modular {
    SYNC("sync"),
    MIGRATION("migration"),
    LOG_COLLECTOR(TaskDto.SYNC_TYPE_LOG_COLLECTOR),
    CONN_HEARTBEAT(TaskDto.SYNC_TYPE_CONN_HEARTBEAT),
    MEM_CACHE(TaskDto.SYNC_TYPE_MEM_CACHE),
    CONNECTION("connection"),
    AGENT("agent"),
    INSPECT("inspect"),
    MESSAGE("message"),
    USER_NOTIFICATION("userNotification"),


    DATA_FLOW_INSIGHT("DataFlowInsight"),
//    DATA_FLOWS("dataflows"),
    JOBS("Jobs"),
    METADATA_INSTANCES("MetadataInstances"),
    WORKERS("Workers"),
    SYSTEM("system"),
    USER("user"),
    ROLE("role"),
    ACCESS_CODE("accessCode"),
    CUSTOMER("customer"),
    MCP("mcp");

    private final String value;

    // 构造方法
    private Modular(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static Modular of(String value) {
        Modular[] modules = Modular.values();
        for (int i = 0; i < modules.length; i++) {
            if(modules[i].value.equals(value)) {
                return modules[i];
            }
        }
        return SYSTEM;
    }

    public static Modular ofTaskSyncType(String taskSyncType) {
        if (null == taskSyncType) {
            return MIGRATION;
        }
        return switch (taskSyncType) {
            case TaskDto.SYNC_TYPE_SYNC -> SYNC;
            case TaskDto.SYNC_TYPE_MIGRATE -> MIGRATION;
            case TaskDto.SYNC_TYPE_LOG_COLLECTOR -> LOG_COLLECTOR;
            case TaskDto.SYNC_TYPE_CONN_HEARTBEAT -> CONN_HEARTBEAT;
            case TaskDto.SYNC_TYPE_MEM_CACHE -> MEM_CACHE;
            default -> MIGRATION;
        };
    }
}
