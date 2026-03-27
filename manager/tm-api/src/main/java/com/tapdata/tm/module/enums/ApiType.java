package com.tapdata.tm.module.enums;

import lombok.Getter;

@Getter
public enum ApiType {
    CUSTOMER_QUERY("customerQuery"),
    DEFAULT_API("defaultApi")
    ;
    final String type;

    ApiType(String type) {
        this.type = type;
    }
}
