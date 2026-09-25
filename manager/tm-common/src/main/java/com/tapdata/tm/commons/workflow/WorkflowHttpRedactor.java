package com.tapdata.tm.commons.workflow;

import org.apache.commons.lang3.StringUtils;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Webhook / JS HTTP 输出脱敏：Authorization、token、password、secret 不得以明文进入运行记录。
 */
public final class WorkflowHttpRedactor {

    private WorkflowHttpRedactor() {
    }

    public static String redactHeaderValue(String name, String value) {
        if (isSensitive(name)) {
            return "***";
        }
        return value;
    }

    public static Map<String, String> redactHeaders(Map<String, String> headers) {
        Map<String, String> redacted = new LinkedHashMap<>();
        if (headers == null) {
            return redacted;
        }
        headers.forEach((k, v) -> redacted.put(k, redactHeaderValue(k, v)));
        return redacted;
    }

    public static String redactBody(String body, int maxChars) {
        if (body == null) {
            return null;
        }
        String masked = body.replaceAll(
                "(?i)\"(authorization|token|password|secret)\"\\s*:\\s*\"[^\"]*\"",
                "\"$1\":\"***\"");
        masked = masked.replaceAll("(?i)(authorization|token|password)\\s*[:=]\\s*[^,\\s\"]+", "$1=***");
        if (maxChars > 0 && masked.length() > maxChars) {
            return masked.substring(0, maxChars);
        }
        return masked;
    }

    public static boolean isSensitive(String name) {
        if (StringUtils.isBlank(name)) {
            return false;
        }
        String n = name.toLowerCase(Locale.ROOT);
        return n.contains("authorization") || n.contains("token") || n.contains("password") || n.contains("secret");
    }
}
