package io.tapdata.connector.mysql.util;

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
}
