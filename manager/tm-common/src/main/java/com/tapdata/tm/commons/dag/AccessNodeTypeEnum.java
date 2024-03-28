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
}
