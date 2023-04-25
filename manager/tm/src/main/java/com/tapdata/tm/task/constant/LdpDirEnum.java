package com.tapdata.tm.task.constant;

import lombok.Getter;

public enum LdpDirEnum {
    LDP_DIR_SOURCE("SOURCE", "source"),
    LDP_DIR_FDM("FDM", "fdm"),
    LDP_DIR_MDM("MDM", "mdm"),
    LDP_DIR_TARGET("TARGET", "target"),
    ;

    @Getter
    private final String value;
    @Getter
    private final String itemType;

    LdpDirEnum(String value, String itemType) {
        this.value = value;
        this.itemType = itemType;
    }
}
