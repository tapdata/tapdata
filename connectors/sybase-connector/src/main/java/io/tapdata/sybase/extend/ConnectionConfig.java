package io.tapdata.sybase.extend;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.sybase.cdc.dto.analyse.filter.ReadFilter;

import java.util.Map;
import java.util.Optional;

/**
 * @author GavinXiao
 * @description ConnectionConfig create by Gavin
 * @create 2023/7/14 15:10
 **/
public class ConnectionConfig {
    private String username;
    private String password;
    private String host;
    private Integer port;
    private String database;
    private String schema;
    private String addtionalString;
    private String timezone;
    private int logCdcQuery;

    public ConnectionConfig(TapConnectionContext context) {
        if (null == context || null == context.getConnectionConfig()) {
            throw new CoreException("TapConnectionContext not be empty or connection config not be empty");
        }
        load(context.getConnectionConfig());
    }

    public ConnectionConfig(DataMap config) {
        load(config);
    }

    private void load(DataMap config) {
        username = config.getString("username");
        password = config.getString("password");
        host = config.getString("host");
        port = config.getInteger("port");
        database = config.getString("database");
        schema = config.getString("schema");
        addtionalString = config.getString("addtionalString");
        timezone = config.getString("timezone");

        logCdcQuery = Optional.ofNullable(config.getInteger("logCdcQuery")).orElse(ReadFilter.LOG_CDC_QUERY_READ_LOG);
        if (logCdcQuery != ReadFilter.LOG_CDC_QUERY_READ_LOG && logCdcQuery != ReadFilter.LOG_CDC_QUERY_READ_SOURCE) {
            logCdcQuery = ReadFilter.LOG_CDC_QUERY_READ_LOG;
        }
    }


    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getAddtionalString() {
        return addtionalString;
    }

    public void setAddtionalString(String addtionalString) {
        this.addtionalString = addtionalString;
    }

    public String getTimezone() {
        return timezone;
    }

    public void setTimezone(String timezone) {
        this.timezone = timezone;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public int getLogCdcQuery() {
        return logCdcQuery;
    }

    public void setLogCdcQuery(int logCdcQuery) {
        this.logCdcQuery = logCdcQuery;
    }
}
