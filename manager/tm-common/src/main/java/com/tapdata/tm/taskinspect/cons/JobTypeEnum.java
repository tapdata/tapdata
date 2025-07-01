package com.tapdata.tm.taskinspect.cons;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/3/24 18:54 Create
 */
public enum JobTypeEnum {

    UNKNOWN,
    FULL,
    CDC,
    RECOVER,
    CHECKER,
    MANUAL_CHECK,
    ;

    public static JobTypeEnum fromString(String type) {
        if (type != null) {
            for (JobTypeEnum t : values()) {
                if (t.name().equalsIgnoreCase(type)) {
                    return t;
                }
            }
        }
        return UNKNOWN;
    }
}
