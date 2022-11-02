package io.tapdata.connector.mysql.util;

import org.apache.commons.lang3.StringUtils;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Random;
import java.util.TimeZone;
import java.util.regex.Pattern;

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
		// Fix json when version<5.7
		dataType = fixJson(dataType, version);
		return dataType;
	}

	private static String fixJson(String dataType, String version) {
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
		if (firstVersion.compareTo(5) <= 0 && secondVersion.compareTo(7) < 0) {
			if (StringUtils.equalsIgnoreCase(dataType, "json")) {
				dataType = "longtext";
			}
		}
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

	public static long convertTimestamp(long timestamp, TimeZone fromTimeZone, TimeZone toTimeZone) {
		LocalDateTime dt = LocalDateTime.now();
		ZonedDateTime fromZonedDateTime = dt.atZone(fromTimeZone.toZoneId());
		ZonedDateTime toZonedDateTime = dt.atZone(toTimeZone.toZoneId());
		long diff = Duration.between(toZonedDateTime, fromZonedDateTime).toMillis();
		return timestamp + diff;
	}

	public static String convertTime(Object time){
		String str[] =((String)time).split(":");
		String timeTemp;
		if(str.length==3){
			int hour = Math.abs(Integer.parseInt(str[0]))%24;
			timeTemp = (hour < 10 ? ("0" + hour) : hour) + ":" +str[1] + ":"+str[2];
			return timeTemp;
		}
		return null;
	}

	public static String toHHmmss(long time) {
		String timeTemp;
		int hours = (int) (time % (1000 * 60 * 60 * 24) / (1000 * 60 * 60));
		int minutes = (int) (time % (1000 * 60 * 60) / (1000 * 60));
		int seconds = (int) (time % (1000 * 60) / 1000);
		timeTemp = (hours < 10 ? ("0" + hours) : hours) + ":" + (minutes < 10 ? ("0" + minutes) : minutes) + ":" + (seconds < 10 ? ("0" + seconds) : seconds);
		return timeTemp;
	}
}
