package com.tapdata.tm.metadatadefinition.constant;

public enum ItemTypeEnum {
    DATA_FLOW("dataflow"),
    TASK("task"),
    MONGO_VIEW("mongo_view"),

    TABLE("table"),
    VIEW("view"),
    COLLECTION("collection");

    private String value;

    // 构造方法
    private ItemTypeEnum(String value) {
        this.value = value;
    }



    public String getValue() {
        return value;
    }
}
