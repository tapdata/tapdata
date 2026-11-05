package com.tapdata.tm.utils;

import java.util.Optional;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/6/2 16:05 Create
 * @description
 */
public final class SettingValueUtil {

    private SettingValueUtil() {
        //do nothing
    }

    public static boolean getBoolean(Object value, Object defaultValue) {
        Object val = Optional.ofNullable(Optional.ofNullable(value).orElse(defaultValue)).orElse(false);
        if (val instanceof String iString) {
            try {
                return Boolean.parseBoolean(iString);
            } catch (Exception ignored) {
                return false;
            }
        } else if (val instanceof Boolean iBool) {
            return iBool;
        }
        return false;
    }
}
