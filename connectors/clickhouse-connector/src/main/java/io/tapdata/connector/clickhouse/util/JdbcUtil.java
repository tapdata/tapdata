package io.tapdata.connector.clickhouse.util;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author samuel
 * @Description
 * @create 2022-04-28 16:37
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

	public static void tryCommit(Connection connection) {
		try {
			if (connection != null && connection.isValid(5) && !connection.getAutoCommit()) {
				connection.commit();
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public static void tryRollBack(Connection connection) {
		try {
			if (connection != null && connection.isValid(5) && !connection.getAutoCommit()) {
				connection.rollback();
			}
		} catch (Throwable ignored) {
		}
	}
}
