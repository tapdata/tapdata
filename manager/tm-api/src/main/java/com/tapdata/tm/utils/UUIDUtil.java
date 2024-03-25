package com.tapdata.tm.utils;

import java.util.UUID;

public class UUIDUtil {

    /**
     * 默认返回32位id
     *
     * @return
     */
    public static String getUUID() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    public static String get64UUID() {
        // 生成64位唯一辨识的字符串
        return getUUID() + getUUID();
    }

}
