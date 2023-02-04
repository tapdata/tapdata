package io.tapdata.databend.util;

public class JdbcUtil {
    public static void closeQuietly(AutoCloseable c) {
        try {
            if (null != c) {
                c.close();
            }
        } catch (Throwable ignored) {
        }
    }
}