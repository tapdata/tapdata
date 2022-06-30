package io.tapdata.common;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.BeanUtils;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.kit.EmptyKit;

import java.io.Serializable;
import java.util.Map;

/**
 * common relation database config
 *
 * @author Jarad
 * @date 2022/5/30
 */
public class CommonDbConfig implements Serializable {

    private static final String TAG = CommonDbConfig.class.getSimpleName();
    private static final JsonParser jsonParser = InstanceFactory.instance(JsonParser.class); //json util
    private static final BeanUtils beanUtils = InstanceFactory.instance(BeanUtils.class); //bean util

    private String dbType;
    private String host;
    private int port;
    private String database;
    private String schema;
    private String user;
    private String password;
    private String extParams;
    private String jdbcDriver;

    //pattern for jdbc-url
    public String getDatabaseUrlPattern() {
        // last %s reserved for extend params
        return "jdbc:" + dbType + "://%s:%d/%s%s";
    }

    //deal with extend params no matter there is ?
    public String getDatabaseUrl() {
        if (EmptyKit.isNull(this.getExtParams())) {
            this.setExtParams("");
        }
        if (EmptyKit.isNotEmpty(this.getExtParams()) && !this.getExtParams().startsWith("?")) {
            this.setExtParams("?" + this.getExtParams());
        }
        return String.format(this.getDatabaseUrlPattern(), this.getHost(), this.getPort(), this.getDatabase(), this.getExtParams());
    }

    public CommonDbConfig load(String json) {
        try {
            beanUtils.copyProperties(jsonParser.fromJson(json, this.getClass()), this);
            return this;
        } catch (Exception e) {
            TapLogger.error(TAG, "config json file is invalid!");
            e.printStackTrace();
        }
        return null;
    }

    /**
     * load config from map, then need to cast it into its own class
     *
     * @param map attributes for config
     * @return ? extends CommonDbConfig
     */
    public CommonDbConfig load(Map<String, Object> map) {
        return beanUtils.mapToBean(map, this);
    }

    public String getDbType() {
        return dbType;
    }

    public void setDbType(String dbType) {
        this.dbType = dbType;
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

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getExtParams() {
        return extParams;
    }

    public void setExtParams(String extParams) {
        this.extParams = extParams;
    }

    public String getJdbcDriver() {
        return jdbcDriver;
    }

    public void setJdbcDriver(String jdbcDriver) {
        this.jdbcDriver = jdbcDriver;
    }
}
