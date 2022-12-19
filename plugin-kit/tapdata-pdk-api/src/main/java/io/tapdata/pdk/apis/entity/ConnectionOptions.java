package io.tapdata.pdk.apis.entity;

import io.tapdata.entity.event.ddl.table.*;
import io.tapdata.entity.utils.FormatUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.CopyOnWriteArrayList;

public class ConnectionOptions {
    //Connector support master slave merge feature.
    public static final String CAPABILITY_MASTER_SLAVE_MERGE = "master_slave_merge";
    //Connector support lookup stream offset by timestamp
    public static final String CAPABILITY_RESUME_STREAM_BY_TIMESTAMP = "resume_stream_by_timestamp";
    //Connector disable batch read offset or not, default is disable batch read offset
    public static final String CAPABILITY_DISABLE_BATCH_READ_OFFSET = "disable_batch_read_offset";
    //Connector support concurrent write records which means writeRecord function need to be thread safe
    public static final String CAPABILITY_CONCURRENT_WRITE = "disable_concurrent_write";

    //DDL events
    public static final String DDL_ALTER_FIELD_NAME_EVENT = FormatUtils.formatTapEvent(TapAlterFieldNameEvent.class);
    public static final String DDL_ALTER_FIELD_ATTRIBUTES_EVENT = FormatUtils.formatTapEvent(TapAlterFieldAttributesEvent.class);
    public static final String DDL_CREATE_TABLE_EVENT = FormatUtils.formatTapEvent(TapCreateTableEvent.class);
    public static final String DDL_DROP_TABLE_EVENT = FormatUtils.formatTapEvent(TapDropTableEvent.class);
    public static final String DDL_CLEAR_TABLE_EVENT = FormatUtils.formatTapEvent(TapClearTableEvent.class);
    public static final String DDL_DROP_FIELD_EVENT = FormatUtils.formatTapEvent(TapDropFieldEvent.class);
    public static final String DDL_NEW_FIELD_EVENT = FormatUtils.formatTapEvent(TapNewFieldEvent.class);
    public static final String DDL_ALTER_TABLE_CHARSET_EVENT = FormatUtils.formatTapEvent(TapAlterTableCharsetEvent.class);
    public static final String DDL_ALTER_DATABASE_TIMEZONE_EVENT = FormatUtils.formatTapEvent(TapAlterDatabaseTimezoneEvent.class);
    public static final String DDL_RENAME_TABLE_EVENT = FormatUtils.formatTapEvent(TapRenameTableEvent.class);

    public static final String DML_INSERT_POLICY = "dml_insert_policy";
    public static final String DML_INSERT_POLICY_UPDATE_ON_EXISTS = "update_on_exists";
    public static final String DML_INSERT_POLICY_IGNORE_ON_EXISTS = "ignore_on_exists";
    public static final String DML_UPDATE_POLICY = "dml_update_policy";
    public static final String DML_UPDATE_POLICY_IGNORE_ON_NON_EXISTS = "ignore_on_nonexists";
    public static final String DML_UPDATE_POLICY_INSERT_ON_NON_EXISTS = "insert_on_nonexists";


    /**
     * Connection string, need provide by PDK developer.
     */
    private String connectionString;
    public ConnectionOptions connectionString(String connectionString) {
        this.connectionString = connectionString;
        return this;
    }
    /**
     * Database timezone
     */
    private TimeZone timeZone;
    private String charset;
    public ConnectionOptions charset(String charset) {
        this.charset = charset;
        return this;
    }
    public ConnectionOptions timeZone(TimeZone timeZone) {
        this.timeZone = timeZone;
        return this;
    }
    private List<Capability> capabilities;
    public ConnectionOptions supportedDDLEvent(String ddlEvent) {
        if(capabilities == null) {
            capabilities = new CopyOnWriteArrayList<>();
        }
        capabilities.add(Capability.create(ddlEvent).type(Capability.TYPE_DDL));
        return this;
    }

    public ConnectionOptions capability(Capability capability) {
        if(capabilities == null)
            capabilities = new CopyOnWriteArrayList<>();
        if(capability.getType() == null)
            capability.type(Capability.TYPE_OTHER);
        capabilities.add(capability);
        return this;
    }

    public static ConnectionOptions create() {
        return new ConnectionOptions();
    }

    public List<Capability> getCapabilities() {
        return capabilities;
    }

    public void setCapabilities(List<Capability> capabilities) {
        this.capabilities = capabilities;
    }

    public TimeZone getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(TimeZone timeZone) {
        this.timeZone = timeZone;
    }

    public List<String> fetchSupportedDDLEvents() {
        if(capabilities != null) {
            List<String> ddlCapabilities = new ArrayList<>();
            for(Capability capability : capabilities) {
                if(capability.getType() == Capability.TYPE_DDL && capability.getId() != null) {
                    ddlCapabilities.add(capability.getId());
                }
            }
            return ddlCapabilities;
        }
        return null;
    }

    public void putSupportedDDLEvents(List<String> supportedDDLEvents) {
        if(supportedDDLEvents != null) {
            if(capabilities == null)
                capabilities = new CopyOnWriteArrayList<>();
            for(String ddlEvent : supportedDDLEvents) {
                capabilities.add(Capability.create(ddlEvent).type(Capability.TYPE_DDL));
            }
        }
    }

    public String getConnectionString() {
        return connectionString;
    }

    public void setConnectionString(String connectionString) {
        this.connectionString = connectionString;
    }

    public String getCharset() {
        return charset;
    }

    public void setCharset(String charset) {
        this.charset = charset;
    }
}
