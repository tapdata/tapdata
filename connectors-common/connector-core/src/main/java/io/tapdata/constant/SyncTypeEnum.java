package io.tapdata.constant;

public enum SyncTypeEnum {

    INITIAL_SYNC("initial_sync"),
    CDC("cdc"),
    INITIAL_SYNC_CDC("initial_sync+cdc"),
    ;

    private final String type;

    SyncTypeEnum(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public static SyncTypeEnum fromValue(String value) {
        for (SyncTypeEnum syncType : SyncTypeEnum.values()) {
            if (syncType.getType().equals(value)) {
                return syncType;
            }
        }
        return INITIAL_SYNC_CDC;
    }
}
