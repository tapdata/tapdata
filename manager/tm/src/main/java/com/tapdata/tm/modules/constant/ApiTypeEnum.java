package com.tapdata.tm.modules.constant;

public enum ApiTypeEnum {

    DEFAULT_API("defaultApi"),
    CUSTOMER_QUERY("customerQuery");

    private String value;

    ApiTypeEnum(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
