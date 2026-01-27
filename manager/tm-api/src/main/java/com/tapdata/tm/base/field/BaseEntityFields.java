package com.tapdata.tm.base.field;

public enum BaseEntityFields implements CollectionField {
    _ID("_id"),
    CREATE_TIME("createTime"),
    LAST_UPDATED("last_updated"),
    UPDATED_AT("updatedAt"),
    CUSTOM_ID("customId"),
    USER_ID("userId"),
    LAST_UPD_BY("lastUpdBy"),
    CREATE_USER("createUser"),
    PERMISSIONS("permissions");
    final String fieldName;

    BaseEntityFields(String name) {
        this.fieldName = name;
    }

    @Override
    public String field() {
        return fieldName;
    }
}
