package com.tapdata.tm.modules.util;

import com.mongodb.ConnectionString;
import com.tapdata.manager.common.utils.StringUtils;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/4/2 10:21 Create
 * @description
 */
public final class MongoUriUtil {
    private static final String PASSWORD = "password";

    private MongoUriUtil() {
    }

    public static String uriByParam(Map<String, Object> config) {
        StringBuilder sb = new StringBuilder("mongodb://");
        if (config.get("user") != null) {
            String user = parse((String) config.get("user"), "user");
            String password = parse((String) config.get(PASSWORD), PASSWORD);
            sb.append(user).append(":")
                    .append(password).append("@");
        }
        sb.append(config.get("host"))
                .append("/").append(config.get("database"));
        if (config.get("additionalString") != null) {
            sb.append("?").append(config.get("additionalString"));
        }
        return sb.toString();
    }

    private static String parse(String str, String tag) {
        try {
            return URLEncoder.encode(str, StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format("[%s] in the connection information is invalid", tag));
        }
    }

    public static String uriByConnectionString(String uri) {
        ConnectionString connectionString;
        try {
            connectionString = new ConnectionString(uri);
        } catch (Exception e) {
            throw new IllegalArgumentException("API publishing cannot be performed using an invalid connection");
        }
        String username = connectionString.getUsername();
        char[] passwordChar = connectionString.getPassword();
        String password = null;
        if (passwordChar != null) {
            password = new String(passwordChar);
        }
        if (StringUtils.isNotBlank(username)) {
            String newUsername = parse(username, "username");
            uri = uri.replace(username, newUsername);
        }
        if (StringUtils.isNotBlank(password)) {
            String newPassword = parse(password, PASSWORD);
            uri = uri.replace(password, newPassword);
        }
        return uri;
    }
}
