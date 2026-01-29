package com.tapdata.tm.v2.api.monitor.utils;

import org.apache.commons.lang3.StringUtils;

import java.util.StringJoiner;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/1/12 22:55 Create
 * @description
 */
public class ApiPathUtil {
    static final String PATH_SPLIT = "/";

    private ApiPathUtil() {

    }

    public static String apiPath(String version, String basePath, String prefix) {
        StringJoiner path = new StringJoiner(PATH_SPLIT);
        path.add("");
        if (!StringUtils.isBlank(version)) {
            path.add(version);
        }
        if (StringUtils.isNotBlank(basePath)) {
            path.add(basePath);
        }
        if (StringUtils.isNotBlank(prefix)) {
            path.add(prefix);
        }
        return path.toString();
    }
}
