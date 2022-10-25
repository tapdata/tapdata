package io.tapdata.constant;

public enum ConnectionTypeEnum {

    SOURCE("source"),
    TARGET("target"),
    SOURCE_AND_TARGET("source_and_target"),
    ;

    private final String type;

    ConnectionTypeEnum(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public static ConnectionTypeEnum fromValue(String value) {
        for (ConnectionTypeEnum connectionType : ConnectionTypeEnum.values()) {
            if (connectionType.getType().equals(value)) {
                return connectionType;
            }
        }
        return SOURCE_AND_TARGET;
    }
}
