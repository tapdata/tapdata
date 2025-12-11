package com.tapdata.tm.skiperrortable;

/**
 * 错误表跳过状态
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/12/3 17:12 Create
 */
public enum SkipErrorTableStatusEnum {
    SKIPPED,
    RECOVERING,
    ;

    public static SkipErrorTableStatusEnum parse(String status) {
        for (SkipErrorTableStatusEnum s : SkipErrorTableStatusEnum.values()) {
            if (s.name().equalsIgnoreCase(status)) {
                return s;
            }
        }
        return null;
    }
}
