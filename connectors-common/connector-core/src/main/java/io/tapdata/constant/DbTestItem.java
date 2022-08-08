package io.tapdata.constant;

public enum DbTestItem {

    HOST_PORT("Check host port is invalid"),
    CHECK_VERSION("Check supported database version"),
    CHECK_CDC_PRIVILEGES("Check replication privileges"),
    CHECK_TABLE_PRIVILEGE("Check all for table privilege"),
    CHECK_LOG_PLUGIN("Check log plugin for database"),
    ;

    private final String content;

    DbTestItem(String content) {
        this.content = content;
    }

    public String getContent() {
        return content;
    }

}
