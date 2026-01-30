package com.tapdata.tm.group.dto;

import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.module.dto.ModulesDto;

import java.util.Map;

public enum ResourceType {
    CONNECTION,
    MIGRATE_TASK,
    SYNC_TASK,
    MODULE,
    SHARE_CACHE,
    INSPECT_TASK,
    METADATA_DEFINITION;

    public static String getResourceName(String resourceType) {
        return switch (resourceType) {
            case "MIGRATE_TASK" -> "MigrateTask.json";
            case "SYNC_TASK" -> "SyncTask.json";
            case "MODULE" -> "Module.json";
            case "CONNECTION" -> "Connection.json";
            case "SHARE_CACHE" -> "ShareCache.json";
            case "INSPECT_TASK" -> "InspectTask.json";
            case "METADATA_DEFINITION" -> "MetadataDefinition.json";
            default -> null;
        };
    }

    public static Boolean hasTags(ResourceType resourceType){
       if(resourceType.equals(ResourceType.SHARE_CACHE) || resourceType.equals(ResourceType.METADATA_DEFINITION)){
           return false;
       };
       return true;
    }



}
