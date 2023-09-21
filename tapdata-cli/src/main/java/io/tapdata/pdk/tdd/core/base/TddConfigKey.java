package io.tapdata.pdk.tdd.core.base;

public enum TddConfigKey {
    INCREMENTAL_DELAY_SEC("incrementalDelaySec", 3),
    DELETE_RECORD_AFTER_CREATE_TABLE("deleteRecordAfterCreateTable", true),

    ;

    String keyName;
    Object defaultValue;
    TddConfigKey(String keyName, Object defaultValue){
        this.keyName = keyName;
        this.defaultValue = defaultValue;
    }
    public String KeyName(){
        return this.keyName;
    }
    public Object defaultValue(){
        return this.defaultValue;
    }
}
