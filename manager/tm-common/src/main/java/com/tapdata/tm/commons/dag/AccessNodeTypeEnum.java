package com.tapdata.tm.commons.dag;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum AccessNodeTypeEnum {
    AUTOMATIC_PLATFORM_ALLOCATION("平台自动分配"),
    MANUALLY_SPECIFIED_BY_THE_USER("用户手动指定"),
    MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP("用户手动指定-引擎组");

    private final String name;

    public static boolean isManually(String nodeType) {
        return isUserManually(nodeType) || isGroupManually(nodeType);
    }

    public static boolean isUserManually(String nodeType) {
        return MANUALLY_SPECIFIED_BY_THE_USER.name.equalsIgnoreCase(nodeType);
    }

    public static boolean isGroupManually(String nodeType) {
        return MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP.name.equalsIgnoreCase(nodeType);
    }

    public static boolean isAutomatic(String nodeType) {
        return AUTOMATIC_PLATFORM_ALLOCATION.name.equalsIgnoreCase(nodeType);
    }
}
