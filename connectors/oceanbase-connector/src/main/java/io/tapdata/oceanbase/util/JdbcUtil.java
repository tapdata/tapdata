package io.tapdata.oceanbase.util;

/**
 * @author dayun
 * @date 2022/6/24 16:47
 */
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
