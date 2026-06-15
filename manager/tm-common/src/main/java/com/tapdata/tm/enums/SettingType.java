package com.tapdata.tm.enums;

import lombok.Getter;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/6/2 11:47 Create
 * @description
 */
@Getter
public enum SettingType {
    SHARE_CDC_ENABLE("global_share_cdc_enable", "share_cdc"),
    ;

    final String key;
    final String category;

    SettingType(String key, String category) {
        this.key = key;
        this.category = category;
    }
}
