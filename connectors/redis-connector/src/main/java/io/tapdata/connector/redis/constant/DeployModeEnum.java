package io.tapdata.connector.redis.constant;


import org.apache.commons.lang3.StringUtils;

/**
 * @author lemon
 */

public enum DeployModeEnum {

    // redis three mode
    STANDALONE("standalone"),
    SENTINEL("sentinel"),
    MASTER_SLAVE("master-slave"),
    CLUSTER("cluster");

    private final String mode;

    DeployModeEnum(String mode) {
        this.mode = mode;
    }

    public String getMode() {
        return mode;
    }

    public static DeployModeEnum fromString(String mode) {
        if (StringUtils.isBlank(mode)) {
            return null;
        }
        for (DeployModeEnum value : values()) {
            if (mode.equals(value.getMode())) {
                return value;
            }
        }
        return null;
    }
}
