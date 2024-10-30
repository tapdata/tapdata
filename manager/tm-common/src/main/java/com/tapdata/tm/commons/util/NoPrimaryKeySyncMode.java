package com.tapdata.tm.commons.util;

/**
 * 无主键表处理模式
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/10/25 17:03 Create
 */
public enum NoPrimaryKeySyncMode {

    ALL_COLUMNS,
    ADD_HASH,
    ;

    public static NoPrimaryKeySyncMode fromValue(String value) {
        // value 为空时代表旧任务配置，使用 ALL_COLUMNS 模式
        if (null == value || value.trim().isEmpty()) return ALL_COLUMNS;

        for (NoPrimaryKeySyncMode item : NoPrimaryKeySyncMode.values()) {
            if (item.name().equalsIgnoreCase(value)) {
                return item;
            }
        }
        return ADD_HASH;
    }
}
