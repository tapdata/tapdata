package com.tapdata.tm.utils;

import com.tapdata.tm.userLog.constant.Modular;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** 共用审计脱敏规则，供写入、查询和历史修复使用。 */
public final class SensitiveDataMasker {
    private static final String KEYS = "(?:access[_-]?code|access[_-]?token|api[_-]?key|secret(?:[_-]?key)?"
            + "|password|passwd|pwd|token|credential(?:s)?|signature|private[_-]?key|session[_-]?id|ticket)";
    private static final Pattern QUOTED_VALUE = Pattern.compile(
            "(?i)([\"']?\\b" + KEYS + "\\b[\"']?\\s*[:=]\\s*)([\"'])((?:\\\\.|(?!\\2).)*?)\\2", Pattern.DOTALL);
    private static final Pattern PAIR = Pattern.compile(
            "(?i)(\\b" + KEYS + "\\b\\s*[:=]\\s*)([^&#\\s}\\]\"']+)");
    // URL 查询值统一隐藏，兼容编码后的参数名，避免仅依赖参数名黑名单。
    private static final Pattern QUERY_VALUE = Pattern.compile("([?&][^=&#\\s\"'<>]+(?:=|%3[dD]))([^&#\\s\"'<>]*)");
    private static final Pattern BEARER = Pattern.compile("(?i)(bearer\\s+)[A-Za-z0-9._~+/=-]+");
    private static final Pattern USER_INFO = Pattern.compile("(?i)(://[^/@\\s:]*:)[^/\\s]*(@)");

    private SensitiveDataMasker() {
    }

    public static boolean isSensitiveKey(String key) {
        return key != null && key.toLowerCase(Locale.ROOT).replace("_", "").replace("-", "")
                .matches("accesscode|accesstoken|apikey|secret|secretkey|password|passwd|pwd|token|credentials?|signature|privatekey|sessionid|ticket");
    }

    public static String maskAuditField(String modular, String field, String text) {
        if (Modular.ACCESS_CODE.getValue().equals(modular) && "parameter1".equals(field)
                && text != null && !text.isEmpty()) {
            return "***";
        }
        return mask(text);
    }

    public static String mask(String text) {
        if (text == null || text.isEmpty()) {
            return text;
        }
        Matcher quoted = QUOTED_VALUE.matcher(text);
        StringBuffer buffer = new StringBuffer();
        while (quoted.find()) {
            quoted.appendReplacement(buffer, Matcher.quoteReplacement(
                    quoted.group(1) + quoted.group(2) + "***" + quoted.group(2)));
        }
        quoted.appendTail(buffer);
        String result = replaceValue(PAIR, buffer.toString());
        result = replaceValue(QUERY_VALUE, result);
        result = replaceValue(BEARER, result);
        Matcher userInfo = USER_INFO.matcher(result);
        buffer = new StringBuffer();
        while (userInfo.find()) {
            userInfo.appendReplacement(buffer, Matcher.quoteReplacement(userInfo.group(1) + "***" + userInfo.group(2)));
        }
        userInfo.appendTail(buffer);
        return buffer.toString();
    }

    private static String replaceValue(Pattern pattern, String text) {
        Matcher matcher = pattern.matcher(text);
        StringBuffer buffer = new StringBuffer();
        while (matcher.find()) {
            matcher.appendReplacement(buffer, Matcher.quoteReplacement(matcher.group(1) + "***"));
        }
        matcher.appendTail(buffer);
        return buffer.toString();
    }
}
