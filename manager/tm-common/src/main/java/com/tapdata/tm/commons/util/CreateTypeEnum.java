package com.tapdata.tm.commons.util;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * 创建类型
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/7/26 15:10 Create
 */
@Getter
@AllArgsConstructor
public enum CreateTypeEnum {
    User("用户"), System("系统"),
    ;

    private final String cn;

    public static boolean isSystem(String str) {
        return System.name().equals(str);
    }

    public static boolean isUser(String str) {
        return !isSystem(str);
    }

    public static boolean isSystem(CreateTypeEnum val) {
        return System == val;
    }

    public static boolean isUser(CreateTypeEnum val) {
        return !isSystem(val);
    }
}
