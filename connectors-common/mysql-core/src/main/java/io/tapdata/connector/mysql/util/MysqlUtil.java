package io.tapdata.connector.mysql.util;

import org.apache.commons.lang3.StringUtils;

/**
 * @author samuel
 * @Description
 * @create 2022-05-06 20:31
 **/
public class MysqlUtil extends JdbcUtil {

	public static Integer getSubVersion(String version, int index) {
		if (StringUtils.isBlank(version)) {
			return null;
		}

		String[] split = version.split("\\.");
		if (split.length <= 1) {
			return null;
		}

		return Integer.valueOf(split[index - 1]);
	}
}
