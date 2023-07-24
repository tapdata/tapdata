package io.tapdata.sybase.extend;

import io.tapdata.common.CommonDbConfig;
import io.tapdata.kit.EmptyKit;

import java.util.Map;
import java.util.Properties;

/**
 * @author GavinXiao
 * @description SybaseConfig create by Gavin
 * @create 2023/7/10 18:42
 **/
public class SybaseConfig extends CommonDbConfig {
    protected String username;

    public SybaseConfig() {
        setDbType("sybase");
        setEscapeChar(' ');
        setJdbcDriver("net.sourceforge.jtds.jdbc.Driver");
        setUsername(username);
    }

    public String getDatabaseUrlPattern() {
        // last %s reserved for extend params
        String url = super.getDatabaseUrlPattern();
        return url.replace("jdbc:sybase:", "jdbc:jtds:sybase:");
    }

    @Override
    public SybaseConfig load(Map<String, Object> map) {
        SybaseConfig config = (SybaseConfig) super.load(map);
        setUser(EmptyKit.isBlank(getUser()) ? (String) map.get("username") : getUser());
        setUsername(getUser());
        setExtParams(EmptyKit.isBlank(getExtParams()) ? (String) map.get("addtionalString") : getExtParams());
        setSchema(getSchema());
        return config;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    @Override
    public Properties getProperties() {
        Properties properties = super.getProperties();
        if (null == properties) properties = new Properties();
        properties.put("username", username);
        properties.put("user", username);
        properties.put("database", this.getDatabase());
        properties.put("dbType", "SYBASE_ASE");
        properties.put("host", this.getHost());
        properties.put("port", this.getPort());
        properties.put("schema", this.getSchema());
        properties.put("password", this.getPassword());
        properties.put("addtionalString", this.getExtParams());
        properties.put("jdbcDriver", this.getJdbcDriver());
        return properties;
    }
}
