package io.tapdata.connector.mysql.config;

import io.tapdata.common.CommonDbConfig;
import io.tapdata.kit.EmptyKit;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

public class MysqlConfig extends CommonDbConfig {

    public MysqlConfig() {
        setDbType("mysql");
        setEscapeChar('`');
        setJdbcDriver("com.mysql.cj.jdbc.Driver");
    }

    private static final Map<String, String> DEFAULT_PROPERTIES = new HashMap<String, String>() {{
        put("rewriteBatchedStatements", "true");
        put("useCursorFetch", "true");
        put("useSSL", "false");
        put("zeroDateTimeBehavior", "convertToNull");
        put("allowPublicKeyRetrieval", "true");
        put("useTimezone", "false");
        put("tinyInt1isBit", "false");
        put("autoReconnect", "true");
    }};

    @Override
    public MysqlConfig load(Map<String, Object> map) {
        MysqlConfig config = (MysqlConfig) super.load(map);
        setUser(EmptyKit.isBlank(getUser()) ? (String) map.get("username") : getUser());
        setExtParams(EmptyKit.isBlank(getExtParams()) ? (String) map.get("addtionalString") : getExtParams());
        setSchema(getDatabase());
        return config;
    }

    @Override
    public String getConnectionString() {
        return getHost() + ":" + getPort() + "/" + getDatabase();
    }

    @Override
    public String getDatabaseUrl() {
        String additionalString = getExtParams();
        additionalString = null == additionalString ? "" : additionalString.trim();
        if (additionalString.startsWith("?")) {
            additionalString = additionalString.substring(1);
        }

        Map<String, String> properties = new HashMap<>();
        StringBuilder sbURL = new StringBuilder("jdbc:").append(getDbType()).append("://").append(getHost()).append(":").append(getPort()).append("/").append(getDatabase());

        if (StringUtils.isNotBlank(additionalString)) {
            String[] additionalStringSplit = additionalString.split("&");
            for (String s : additionalStringSplit) {
                String[] split = s.split("=");
                if (split.length == 2) {
                    properties.put(split[0], split[1]);
                }
            }
        }
        for (String defaultKey : DEFAULT_PROPERTIES.keySet()) {
            if (properties.containsKey(defaultKey)) {
                continue;
            }
            properties.put(defaultKey, DEFAULT_PROPERTIES.get(defaultKey));
        }

        if (StringUtils.isNotBlank(timezone)) {
            try {
                timezone = "GMT" + timezone;
                String serverTimezone = timezone.replace("+", "%2B").replace(":00", "");
                properties.put("serverTimezone", serverTimezone);
            } catch (Exception ignored) {
            }
        }
        StringBuilder propertiesString = new StringBuilder();
        properties.forEach((k, v) -> propertiesString.append("&").append(k).append("=").append(v));

        if (propertiesString.length() > 0) {
            additionalString = StringUtils.removeStart(propertiesString.toString(), "&");
            sbURL.append("?").append(additionalString);
        }

        return sbURL.toString();
    }

    private String timezone;

    public String getTimezone() {
        return timezone;
    }

    public void setTimezone(String timezone) {
        this.timezone = timezone;
    }
}
