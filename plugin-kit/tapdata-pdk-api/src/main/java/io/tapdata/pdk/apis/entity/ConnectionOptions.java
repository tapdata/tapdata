package io.tapdata.pdk.apis.entity;

import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.CopyOnWriteArrayList;

public class ConnectionOptions {
    public static final String CAPABILITY_MASTER_SLAVE_MERGE = "master-slave-merge";
    public static final String CAPABILITY_RESUME_STREAM_BY_TIMESTAMP = "resume-stream-by-timestamp";

    public static final String DDL_ALTER_FIELD_NAME_EVENT = "alter-field-name-event";
    public static final String DDL_ALTER_FIELD_DATATYPE_EVENT = "alter-field-datatype-event";
    public static final String DDL_ALTER_FIELD_CHECK_EVENT = "alter-field-check-event";
    public static final String DDL_CREATE_TABLE_EVENT = "create-table-event";
    public static final String DDL_DROP_TABLE_EVENT = "drop-table-event";
    public static final String DDL_ALTER_PRIMARY_KEY_EVENT = "alter-primary-key-event";
    public static final String DDL_ALTER_FIELD_CONSTRAINT_EVENT = "alter-field-constraint-event";
    public static final String DDL_DROP_FIELD_EVENT = "drop-field-event";
    public static final String DDL_ALTER_FIELD_NOT_NULL_EVENT = "alter-field-not-null-event";
    public static final String DDL_ALTER_FIELD_COMMENT_EVENT = "alter-field-comment-event";
    public static final String DDL_NEW_FIELD_EVENT = "new-field-event";
    public static final String DDL_ALTER_TABLE_CHARSET_EVENT = "alter-table-charset-event";
    public static final String DDL_ALTER_FIELD_DEFAULT_EVENT = "alter-field-default-event";
    public static final String DDL_ALTER_DATABASE_TIMEZONE_EVENT = "alter-database-timezone-event";


    /**
     * Database timezone
     */
    private TimeZone timeZone;
    public ConnectionOptions timeZone(TimeZone timeZone) {
        this.timeZone = timeZone;
        return this;
    }
    private List<String> capabilities;
    private List<String> supportedDDLEvents;
    public ConnectionOptions supportedDDLEvent(String ddlEvent) {
        if(supportedDDLEvents == null) {
            supportedDDLEvents = new CopyOnWriteArrayList<>();
        }
        supportedDDLEvents.add(ddlEvent);
        return this;
    }

    public ConnectionOptions capability(String capability) {
        if(capabilities == null)
            capabilities = new CopyOnWriteArrayList<>();
        capabilities.add(capability);
        return this;
    }

    public static ConnectionOptions create() {
        return new ConnectionOptions();
    }

    public List<String> getCapabilities() {
        return capabilities;
    }

    public void setCapabilities(List<String> capabilities) {
        this.capabilities = capabilities;
    }

    public TimeZone getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(TimeZone timeZone) {
        this.timeZone = timeZone;
    }

    public List<String> getSupportedDDLEvents() {
        return supportedDDLEvents;
    }

    public void setSupportedDDLEvents(List<String> supportedDDLEvents) {
        this.supportedDDLEvents = supportedDDLEvents;
    }
}
