package com.tapdata.tm.commons.alarm;

import java.util.regex.Pattern;

/**
 * 用户组 gid 前缀。现网 gid 是字母数字，仍做 quote，避免把前缀当成正则。
 */
public final class GidPrefix {
    /** Impossible pattern: never matches, aligned with matches() empty-gid semantics. */
    private static final String NEVER_MATCH = "a^";

    private GidPrefix() {
    }

    public static String regex(String gid) {
        if (gid == null || gid.isEmpty()) {
            return NEVER_MATCH;
        }
        return "^" + Pattern.quote(gid);
    }

    public static boolean matches(String candidate, String gid) {
        if (candidate == null || gid == null || gid.isEmpty()) {
            return false;
        }
        return candidate.startsWith(gid);
    }
}
