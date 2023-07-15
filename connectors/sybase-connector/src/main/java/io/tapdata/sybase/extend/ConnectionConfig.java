package io.tapdata.sybase.extend;

import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;

import java.util.Map;

/**
 * @author GavinXiao
 * @description ConnectionConfig create by Gavin
 * @create 2023/7/14 15:10
 **/
public class ConnectionConfig {
    String username;
    String password;
    String host;
    Integer port;
    String database;
    String schema;
    String addtionalString;
    String timezone;

    public ConnectionConfig(TapConnectionContext context) {
        load(context.getConnectionConfig());
    }

    public ConnectionConfig(DataMap config) {
        load(config);
    }

    private void load(DataMap config){
        username = config.getString("username");
        password = config.getString("password");
        host = config.getString("host");
        port = config.getInteger("port");
        database = config.getString("database");
        schema = config.getString("schema");
        addtionalString = config.getString("addtionalString");
        timezone = config.getString("timezone");
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
}
