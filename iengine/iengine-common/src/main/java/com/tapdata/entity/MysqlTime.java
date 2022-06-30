package com.tapdata.entity;

import java.math.BigDecimal;
import java.sql.Time;
import java.time.ZoneId;
import java.util.Objects;
import java.util.TimeZone;

/**
 * Mysql时间格式数据
 * <pre>
 * Author: <a href="mailto:linhs@thoughtup.cn">Harsen</a>
 * CreateTime: 2021/5/9 下午2:37
 * </pre>
 */
public class MysqlTime {

	public static MysqlTime ofMillisecond(long milliseconds, int scale) {
		if (milliseconds < 0) {
			return new MysqlTime(false, Math.abs(milliseconds) / 1000, (int) (milliseconds % 1000) * 1000000, scale);
		}
		return new MysqlTime(true, milliseconds / 1000, (int) (milliseconds % 1000) * 1000000, scale);
	}

	public static MysqlTime ofMicrosecond(long milliseconds, int scale) {
		if (milliseconds < 0) {
			return new MysqlTime(false, Math.abs(milliseconds) / 1000000, (int) (milliseconds % 1000000) * 1000, scale);
		}
		return new MysqlTime(true, milliseconds / 1000000, (int) (milliseconds % 1000000) * 1000, scale);
	}

	/**
	 * MysqlTime 类型转换
	 *
	 * @param data   时间数据（int, long, string(HH:mm:ss.S), time, mysqlTime）
	 * @param scale  精度
	 * @param fromId 源时区
	 * @return MysqlTime
	 */
	public static MysqlTime parse(Object data, int scale, ZoneId fromId) {
		MysqlTime ins;
		if (data instanceof Integer) {
			// debezium cdc integer type is millisecond
			long dataTimes = Long.parseLong(data.toString());
			ins = MysqlTime.ofMillisecond(dataTimes, scale);
		} else if (data instanceof Long) {
			// debezium cdc long type is microsecond
			long dataTimes = (Long) data;
			ins = MysqlTime.ofMicrosecond(dataTimes, scale);
		} else if (data instanceof String) {
			String[] split = ((String) data).trim().split(":");
			if (split.length != 3) {
				throw new RuntimeException("invalid time string format: " + data);
			}
			int nanoseconds = 0;
			boolean positive = split[0].startsWith("-");
			BigDecimal hour = new BigDecimal(split[0]);
			BigDecimal minute = new BigDecimal(split[1]);
			BigDecimal second;
			if (split[2].contains(".")) {
				String secondStr = split[2].substring(0, split[2].indexOf("."));
				String suffixStr = split[2].substring(split[2].indexOf(".") + 1);
				second = new BigDecimal(secondStr);
				nanoseconds = parseSuffix(suffixStr, 9);
			} else {
				second = new BigDecimal(split[2]);
			}

			BigDecimal bigDecimal = hour.abs().multiply(new BigDecimal("3600"));
			bigDecimal = bigDecimal.add(minute.multiply(new BigDecimal("60")));
			bigDecimal = bigDecimal.add(second);

			long seconds = new BigDecimal(bigDecimal.stripTrailingZeros().toPlainString()).longValue();
			ins = new MysqlTime(positive, seconds, nanoseconds, scale);
		} else if (data instanceof Time) {
			long milliseconds = ((Time) data).getTime();
			ins = MysqlTime.ofMillisecond(milliseconds, scale);
		} else if (data instanceof MysqlTime) {
			return (MysqlTime) data;
		} else {
			throw new RuntimeException("Unsupported time type, value: " + data + ", type: " + data.getClass().getName());
		}

		if (null == fromId) {
			return ins;
		}
		return ins.subtract(fromId);
	}

	/**
	 * 时间后缀转换
	 * <ul>
	 *   <li>scale(3): 100, 010, 001</li>
	 *   <li>scale(6): 100000, 010000, 001000, 000100, 000010, 000001</li>
	 *   <li>scale(9): 100000000, 010000000, 001000000, 000100000, 000010000, 000001000, 000000100, 000000010, 000000001</li>
	 * </ul>
	 *
	 * @param str   时间后缀数值
	 * @param scale 精度（0-9）
	 * @return 精度对应数据
	 */
	private static int parseSuffix(String str, int scale) {
		int data = Integer.parseInt(str), diffSize = scale - str.length();
		if (0 < diffSize) {
			data *= Math.pow(10, diffSize);
		} else if (0 > diffSize) {
			data = data / (int) Math.pow(10, Math.abs(diffSize));
		}
		return data;
	}

	private boolean positive;
	private long seconds;
	private int nanoseconds;
	private final int scale;

	public MysqlTime(boolean positive, long seconds, int nanoseconds, int scale) {
		this.positive = positive;
		this.seconds = seconds;
		this.scale = scale;
		this.nanoseconds = nanoseconds;
	}

	public boolean isPositive() {
		return positive;
	}

	public long getSeconds() {
		return seconds;
	}

	public int getScale() {
		return scale;
	}

	public int getNanoseconds() {
		return nanoseconds;
	}

	public int getScaleData() {
		return getScaleData(getScale());
	}

	public int getScaleData(int scale) {
		return scale > 0 ? (int) (nanoseconds % Math.pow(10, scale)) : 0;
	}

	private MysqlTime subtract(ZoneId zoneId) {
		TimeZone timeZone = TimeZone.getTimeZone(zoneId);
		int offsetTimes = timeZone.getRawOffset();
		return new MysqlTime(positive, getSeconds() - offsetTimes / 1000, nanoseconds - (offsetTimes % 1000) * 1000000, getScale());
	}

	private MysqlTime add(ZoneId zoneId) {
		TimeZone timeZone = TimeZone.getTimeZone(zoneId);
		int offsetTimes = timeZone.getRawOffset();
		return new MysqlTime(positive, getSeconds() + offsetTimes / 1000, nanoseconds + (offsetTimes % 1000) * 1000000, getScale());
	}

	public Time toTime() {
		return new Time(getScaleData(3));
	}

	public String toString(ZoneId toZoneId) {
		return toString(toZoneId, getScale());
	}

	public String toString(ZoneId toZoneId, int scale) {
		if (null == toZoneId) {
			return toString(scale);
		}
		return add(toZoneId).toString(scale);
	}

	public String toString(int scale) {
		long seconds = getSeconds();
		StringBuilder buf = new StringBuilder();
		seconds = Math.abs(seconds);
		buf.insert(0, String.format("%02d", seconds % 60)); // 秒
		buf.insert(0, String.format("%02d:", (seconds / 60) % 60)); // 分
		buf.insert(0, String.format("%02d:", (seconds / 3600))); // 时
		if (!positive) buf.insert(0, "-");
		if (getScale() > 0) {
			buf.append(String.format(".%0" + scale + "d", getScaleData()));
		}
		return buf.toString();
	}

	@Override
	public String toString() {
		return toString(getScale());
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		MysqlTime mysqlTime = (MysqlTime) o;
		return seconds == mysqlTime.seconds;
	}

	@Override
	public int hashCode() {
		return Objects.hash(seconds);
	}

	public static void main(String[] args) {
		// parseSuffix 逻辑测试
		int data;
		String msgFormat = "%4s >> %4s: %s\n", suffixStr;
		System.out.println("parseSuffix 逻辑测试：");
		System.out.printf(msgFormat, (suffixStr = "0"), data = parseSuffix(suffixStr, 3), 0 == data);
		System.out.printf(msgFormat, (suffixStr = "000"), data = parseSuffix(suffixStr, 3), 0 == data);
		System.out.printf(msgFormat, (suffixStr = "001"), data = parseSuffix(suffixStr, 3), 1 == data);
		System.out.printf(msgFormat, (suffixStr = "010"), data = parseSuffix(suffixStr, 3), 10 == data);
		System.out.printf(msgFormat, (suffixStr = "01"), data = parseSuffix(suffixStr, 3), 10 == data);
		System.out.printf(msgFormat, (suffixStr = "100"), data = parseSuffix(suffixStr, 3), 100 == data);
		System.out.printf(msgFormat, (suffixStr = "1"), data = parseSuffix(suffixStr, 3), 100 == data);
		System.out.printf(msgFormat, (suffixStr = "1000"), data = parseSuffix(suffixStr, 3), 100 == data);
		System.out.printf(msgFormat, (suffixStr = "0001"), data = parseSuffix(suffixStr, 3), 0 == data);

		System.out.println("\n时间格式测试：");
		System.out.println(MysqlTime.parse("00:00:01.001", 0, null));
		System.out.println(MysqlTime.parse("-00:00:01.001", 3, null));
		System.out.println(MysqlTime.parse("00:00:01.001", 6, null));
		System.out.println(new MysqlTime(true, 0, 1, 6));
		System.out.println(new MysqlTime(false, 0, 1000, 6));
		System.out.println(new Time(new MysqlTime(true, 0, 1000000, 6).getScaleData(6)).toLocalTime());
	}
}
