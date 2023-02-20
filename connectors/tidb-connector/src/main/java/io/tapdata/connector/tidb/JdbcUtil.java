package io.tapdata.connector.tidb;

import io.tapdata.entity.logger.TapLogger;

import java.sql.Connection;

/**
 * @author Dexter
 */
public class JdbcUtil {
    public static final String TAG = JdbcUtil.class.getSimpleName();
    public static void closeQuietly(AutoCloseable c) {
        try {
            if (null != c) {
                c.close();
            }
        } catch (Throwable ignored) {
        }
    }

    // 并发情况下不适合调用此方法， isValid 会被阻塞
    public static void tryCommit(Connection connection) {
        try {
            if (connection != null && connection.isValid(5) && !connection.getAutoCommit()) {
                connection.commit();
            }
        } catch (Throwable ignore) {
            TapLogger.warn(TAG, "Commit error: " + ignore.getMessage(), ignore);
        }
    }

    public static void tryRollBack(Connection connection) {
        try {
            if (connection != null && connection.isValid(5) && !connection.getAutoCommit()) {
                connection.rollback();
            }
        } catch (Throwable ignore) {
            TapLogger.warn(TAG, "RollBack error: " + ignore.getMessage(), ignore);
        }
    }
}
