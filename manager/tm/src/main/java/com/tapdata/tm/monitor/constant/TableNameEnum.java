package com.tapdata.tm.monitor.constant;

public enum TableNameEnum {
    AgentEnvironment("AgentEnvironment"),
    AgentMeasurement("AgentMeasurement"),

    AgentStatistics("AgentStatistics");
    private String tableName;

    TableNameEnum(String tableName) {
        this.tableName=tableName;
    }

    public String getValue() {
        return tableName;
    }

    public static String getTableName(String value) {
        TableNameEnum[] businessModeEnums = values();
        for (TableNameEnum tableNameEnum : businessModeEnums) {
            if (tableNameEnum.getValue().equals(value)) {
                return tableNameEnum.getValue();
            }
        }
        return null;
    }



}
