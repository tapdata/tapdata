package io.tapdata.connector.hive1.util;

import io.tapdata.connector.hive1.config.Hive1Config;
import io.tapdata.entity.logger.TapLogger;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * @author samuel
 * @Description
 * @create 2022-04-28 16:37
 **/
public class JdbcUtil {

	private static String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";

	private final static String DRIVER_PREFIX = "jdbc:hive2://";

	public static void closeQuietly(AutoCloseable c) {
		try {
			if (null != c) {
				c.close();
			}
		} catch (Throwable ignored) {
		}
	}

	public static Connection createConnection(Hive1Config hive1Config) throws SQLException {
		String username = hive1Config.getUser();
		String passwd = hive1Config.getPassword();
		String database = hive1Config.getDatabase();

		Properties prop = new Properties();
		if (StringUtils.isNotBlank(username)) {
			prop.setProperty("user", username);
		}
		if (StringUtils.isNotBlank(passwd)) {
			prop.setProperty("password", passwd);
		}
		if (StringUtils.isNotBlank(database)) {
			prop.setProperty("database", database);
		}
		prop.setProperty("hive.metastore.client.socket.timeout", "180");
		prop.setProperty("hive.server.read.socket.timeout", "180");
		prop.setProperty("hive.server.write.socket.timeout", "180");
		prop.setProperty("hive.server.thrift.socket.timeout", "180");
		prop.setProperty("hive.client.thrift.socket.timeout", "180");

		Connection conn = null;
		try {
			Class.forName(DRIVER_NAME);
			String jdbcUrl = getJdbcUrl(hive1Config);
			conn = DriverManager.getConnection(jdbcUrl, prop);
//			conn.setAutoCommit(false);
		} catch (ClassNotFoundException e) {
			TapLogger.error("Hive Driver class not found,", "driver name is:{}", DRIVER_NAME, e);
		}
		return conn;
	}

	public static String getJdbcUrl(Hive1Config hive1Config) {
		String host = hive1Config.getHost();
		Integer port = hive1Config.getPort();
		String database = hive1Config.getDatabase();

		String url = DRIVER_PREFIX + host;
		if (port != null && port > 0) {
			url += ":" + port;
		}

		if (StringUtils.isNotBlank(database)) {
			url += "/" + database;
		}
		return url;
	}
}
