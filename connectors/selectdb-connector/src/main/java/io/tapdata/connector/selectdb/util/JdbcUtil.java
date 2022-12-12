package io.tapdata.connector.selectdb.util;

/**
 * Author:Skeet
 * Date: 2022/12/9
 **/
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
