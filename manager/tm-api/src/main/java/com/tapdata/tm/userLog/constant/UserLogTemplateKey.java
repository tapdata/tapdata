package com.tapdata.tm.userLog.constant;

public final class UserLogTemplateKey {

    public static final String BUNDLE_NAME = "userLogsTemplate";
    public static final String PREFIX = "userLogs.";
    public static final String DEFAULT_PREFIX = PREFIX + "_default.";
    public static final String MODULE_PREFIX = PREFIX + "_module.";

    private UserLogTemplateKey() {
    }

    public static String specificOperation(String modular, String operation) {
        return PREFIX + modular + "." + operation;
    }

    public static String defaultOperation(String operation) {
        return DEFAULT_PREFIX + operation;
    }

    public static String moduleName(String modular) {
        return MODULE_PREFIX + modular;
    }
}
