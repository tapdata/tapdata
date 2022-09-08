package com.tapdata.tm.modules.constant;

public enum ModuleStatusEnum {
    ACTIVE("active","已发布"),
    PENDING("pending","待发布"),
    GENERATING("generating","待生成") ;

    private final String value;
    private final String describe;

    // 构造方法
    private ModuleStatusEnum(String value,String describe) {
        this.value = value;
        this.describe = describe;
    }

    public String getValue() {
        return value;
    }

}
