package com.tapdata.tm.commons.alarm;

import java.util.regex.Pattern;

/**
 * 用户组 gid 前缀。现网 gid 是字母数字，仍做 quote，避免把前缀当成正则。
 */
public final class GidPrefix {
    private GidPrefix() {
    }

    public static String regex(String gid) {
        return "^" + Pattern.quote(gid == null ? "" : gid);
    }

    public static boolean matches(String candidate, String gid) {
        if (candidate == null || gid == null || gid.isEmpty()) {
            return false;
        }
        return Pattern.compile(regex(gid)).matcher(candidate).lookingAt();
    }
}
