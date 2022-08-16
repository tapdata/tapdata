package io.tapdata.connector.mariadb.util;

import org.apache.commons.lang3.StringUtils;

import java.util.Random;
import java.util.regex.Pattern;

/**
 * @author samuel
 * @Description
 * @create 2022-05-06 20:31
 **/
public class MariadbUtil extends JdbcUtil {

	public static Integer getSubVersion(String version, int index) {
		if (StringUtils.isBlank(version)) {
			return null;
		}

		String[] split = version.split("\\.");
		if (split.length <= 1) {
			return null;
		}

		String str = split[index - 1];
		try {
			return Integer.valueOf(str);
		} catch (NumberFormatException e) {
			throw new RuntimeException("Version string: " + str + ", is not a number");
		}
	}

	public static int randomServerId() {
		int lowestServerId = 5400;
		int highestServerId = Integer.MAX_VALUE;
		return lowestServerId + new Random().nextInt(highestServerId - lowestServerId);
	}

	public static String fixDataType(String dataType, String version) {
		if (StringUtils.isBlank(dataType)) {
			return dataType;
		}
		// Fix datetime/timestamp when version<5.6
		dataType = fixDatetime(dataType, version);
		return dataType;
	}

	public static String fixDatetime(String dataType, String version) {
		if (StringUtils.isBlank(version)) {
			return dataType;
		}
		Integer firstVersion = getFirstVersion(version);
		if (null == firstVersion) {
			return dataType;
		}
		Integer secondVersion = getSecondVersion(version);
		if (null == secondVersion) {
			return dataType;
		}
		if (firstVersion.compareTo(5) <= 0 && secondVersion.compareTo(6) <= 0) {
			Pattern pattern = Pattern.compile("(datetime|timestamp)\\(\\d+\\)", Pattern.CASE_INSENSITIVE);
			if (pattern.matcher(dataType).matches()) {
				dataType = dataType.replaceAll("\\(\\d+\\)", "");
			}
		}
		return dataType;
	}

	private static Integer getSecondVersion(String version) {
		Integer secondVersion;
		try {
			secondVersion = getSubVersion(version, 2);
		} catch (Exception e) {
			throw new RuntimeException("Get second version number failed, version string: " + version + ", error: " + e.getMessage(), e);
		}
		return secondVersion;
	}

	private static Integer getFirstVersion(String version) {
		Integer firstVersion;
		try {
			firstVersion = getSubVersion(version, 1);
		} catch (Exception e) {
			throw new RuntimeException("Get first version number failed, version string: " + version + ", error: " + e.getMessage(), e);
		}
		return firstVersion;
	}
}
