package com.tapdata.constant;

import java.math.BigDecimal;

/**
 * @author samuel
 * @Description
 * @create 2021-03-29 10:37
 **/
public class NumberUtil {

	private static BigDecimal shortMax = new BigDecimal(Short.MAX_VALUE);
	private static BigDecimal shortMin = new BigDecimal(Short.MIN_VALUE);
	private static BigDecimal integerMax = new BigDecimal(Integer.MAX_VALUE);
	private static BigDecimal integerMin = new BigDecimal(Integer.MIN_VALUE);
	private static BigDecimal longMax = new BigDecimal(Long.MAX_VALUE);
	private static BigDecimal longMin = new BigDecimal(Long.MIN_VALUE);
	private static BigDecimal floatMax = new BigDecimal(Float.MAX_VALUE);
	private static BigDecimal floatMin = new BigDecimal(Float.MIN_VALUE);
	private static BigDecimal doubleMax = new BigDecimal(Double.MAX_VALUE);
	private static BigDecimal doubleMin = new BigDecimal(Double.MIN_VALUE);

	public static boolean betweenShort(BigDecimal val) {
		return between(val, shortMin, shortMax);
	}

	public static boolean betweenInteger(BigDecimal val) {
		return between(val, integerMin, integerMax);
	}

	public static boolean betweenLong(BigDecimal val) {
		return between(val, longMin, longMax);
	}

	public static boolean betweenFloat(BigDecimal val) {
		return between(val, floatMin, floatMax);
	}

	public static boolean betweenDouble(BigDecimal val) {
		return between(val, doubleMin, doubleMax);
	}

	public static boolean between(BigDecimal val, BigDecimal min, BigDecimal max) {
		return greaterOrEqual(val, min) && lessOrEqual(val, max);
	}

	public static boolean less(BigDecimal a, BigDecimal b) {
		return a.compareTo(b) < 0;
	}

	public static boolean eq(BigDecimal a, BigDecimal b) {
		return a.compareTo(b) == 0;
	}

	public static boolean greater(BigDecimal a, BigDecimal b) {
		return a.compareTo(b) > 0;
	}

	public static boolean lessOrEqual(BigDecimal a, BigDecimal b) {
		return a.compareTo(b) <= 0;
	}

	public static boolean greaterOrEqual(BigDecimal a, BigDecimal b) {
		return a.compareTo(b) >= 0;
	}

	/**
	 * 判断是否整数
	 *
	 * @param bigDecimal
	 * @return
	 */
	public static boolean isInteger(BigDecimal bigDecimal) {
		if (bigDecimal == null) {
			throw new RuntimeException("Input param cannot be null");
		}

		if (new BigDecimal(bigDecimal.toBigInteger()).compareTo(bigDecimal) == 0) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * 根据data的值，获取最合适的类型的数值
	 *
	 * @param data
	 * @return
	 */
	public static Object getAppropriateNumber(Object data) {
		BigDecimal bigDecimal;
		if (!(data instanceof BigDecimal)) {
			try {
				bigDecimal = new BigDecimal(data.toString());
			} catch (Exception e) {
				return data;
			}
		} else {
			bigDecimal = (BigDecimal) data;
		}

		if (isInteger(bigDecimal)) {
			if (betweenShort(bigDecimal)) {
				return bigDecimal.shortValue();
			} else if (betweenInteger(bigDecimal)) {
				return bigDecimal.intValue();
			} else if (betweenLong(bigDecimal)) {
				return bigDecimal.longValue();
			} else {
				return bigDecimal.toBigInteger();
			}
		} else {
			if (betweenDouble(bigDecimal)) {
				return bigDecimal.doubleValue();
			} else {
				return bigDecimal;
			}
		}
	}
}
