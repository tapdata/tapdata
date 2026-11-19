package com.tapdata.tm.proxyprocess.constant;

/** Supported AgentProcess types. */
public enum AgentProcessType {
    ORACLE_LOG_PARSER("OracleLogParser"),
    DB2_FOR_Z_CDC_READER("Db2 for z cdc reader");

    private final String value;

    AgentProcessType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
