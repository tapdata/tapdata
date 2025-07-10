package com.tapdata.constant;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.tapdata.entity.MysqlJson;
import com.tapdata.entity.values.BooleanNotExist;
import com.tapdata.exception.CompareException;
import io.tapdata.entity.schema.value.DateTime;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

public class CommonUtil {

	/**
	 * 比较版本号的大小,前者大则返回一个正数,后者大返回一个负数,相等则返回0
	 *
	 * @param version1
	 * @param version2
	 * @return
	 */
	public static int compareVersion(String version1, String version2) throws Exception {
		if (version1 == null || version2 == null) {
			throw new Exception("compareVersion error:illegal params.");
		}
		String[] versionArray1 = version1.split("\\.");//注意此处为正则匹配，不能用"."；
		String[] versionArray2 = version2.split("\\.");
		int idx = 0;
		int minLength = Math.min(versionArray1.length, versionArray2.length);//取最小长度值
		int diff = 0;
		while (idx < minLength
				&& (diff = versionArray1[idx].length() - versionArray2[idx].length()) == 0//先比较长度
				&& (diff = versionArray1[idx].compareTo(versionArray2[idx])) == 0) {//再比较字符
			++idx;
		}
		//如果已经分出大小，则直接返回，如果未分出大小，则再比较位数，有子版本的为大；
		diff = (diff != 0) ? diff : versionArray1.length - versionArray2.length;
		return diff;
	}

	public static int compareObjects(Object[] val1, Object[] val2,boolean ignoreTimePrecision,String roundingMode) {
		// First, check if both arrays are null
		if (val1 == null && val2 == null) {
			return 0; // Both are null, considered equal
		} else if (val1 == null) {
			return -1; // Only val1 is null, considered less than val2
		} else if (val2 == null) {
			return 1; // Only val2 is null, considered greater than val1
		}

		// Next, compare the lengths of the two arrays
		int len1 = val1.length;
		int len2 = val2.length;

		if (len1 < len2) {
			return -1; // Length of val1 is less than val2, considered less than val2
		} else if (len1 > len2) {
			return 1; // Length of val1 is greater than val2, considered greater than val2
		}

		// If lengths are equal, compare elements one by one
		for (int i = 0; i < len1; i++) {
			Object obj1 = val1[i];
			Object obj2 = val2[i];
			if (obj1 instanceof Boolean && !(obj2 instanceof Boolean)) {
				obj2 = toBoolean(obj2);
				if(obj2 instanceof BooleanNotExist){
					obj2 = null;
				}
			}
			if (obj2 instanceof Boolean && !(obj1 instanceof Boolean)) {
				obj1 = toBoolean(obj1);
				if(obj1 instanceof BooleanNotExist){
					obj1 = null;
				}
			}
			if(obj1 instanceof byte[]){
				obj1 = new String((byte[]) obj1, StandardCharsets.UTF_8);
			}
			if(obj2 instanceof byte[]){
				obj2 = new String((byte[]) obj2, StandardCharsets.UTF_8);
			}
			if (obj1 == null && obj2 == null) {
				continue; // Both are null, compare the next element
			} else if (obj1 == null) {
				return -1; // Only obj1 is null, considered less than obj2
			} else if (obj2 == null) {
				return 1; // Only obj2 is null, considered greater than obj1
			}

			// Compare non-null elements using the compareTo method, assuming they implement the Comparable interface
			if (obj1 instanceof Comparable<?> && obj2 instanceof Comparable<?>) {
				if (obj1 instanceof Number || obj2 instanceof Number) {
					obj1 = new BigDecimal(obj1.toString());
					obj2 = new BigDecimal(obj2.toString());
				}else if(obj1 instanceof String || obj2 instanceof String){
					obj1 = obj1.toString().trim();
					obj2 = obj2.toString().trim();
				}
				if (ignoreTimePrecision) {
					if (obj1 instanceof DateTime dateTime) {
						obj1 = dateTime.toInstant();
					}
					if (obj2 instanceof DateTime dateTime) {
						obj2 = dateTime.toInstant();
					}
					if (obj1 instanceof Instant && obj2 instanceof Instant) {
						if (!compareInstant((Instant) obj1, (Instant) obj2, ignoreTimePrecision,roundingMode)) {
							continue;
						}

					}
				}
				int result = ((Comparable<Object>) obj1).compareTo(obj2);
				if (result != 0) {
					return result; // If the comparison result is not 0, return the result
				}
			}else {
				// If elements are not comparable, you can consider other comparison strategies
				// Here, you can customize based on the specific scenario
				// For example, you can compare string representations using the toString method
				int result = obj1.toString().compareTo(obj2.toString());
				if (result != 0) {
					return result;
				}
			}
		}

		return 0; // Both arrays are equal
	}

	public static boolean compareInstant(Instant val1, Instant val2, boolean ignoreTimePrecision, String roundingMode) {
		Instant instant1 = val1;
		Instant instant2 = val2;

		if (ignoreTimePrecision) {
			int precision1 = CommonUtil.getValPrecision(instant1);
			int precision2 = CommonUtil.getValPrecision(instant2);
			if (precision1 != precision2) {
				int minPrecision = Math.min(precision1, precision2);
				Instant norm1 = CommonUtil.normalizePrecision(instant1, minPrecision, roundingMode);
				Instant norm2 = CommonUtil.normalizePrecision(instant2, minPrecision, roundingMode);
				if (norm1.equals(norm2)) {
					return false;
				}
			} else {
				long diffMillis = Math.abs(Duration.between(val1, val2).toMillis());
				return diffMillis > 3;
			}
		}
		return !instant1.equals(instant2);
	}

	public static int getValPrecision(Instant val1) {
		int valPrecision = 9;
		String nanosStr = String.format("%09d", val1.getNano());
		while (nanosStr.endsWith("0")) {
			nanosStr = nanosStr.substring(0, nanosStr.length() - 1);
			valPrecision--;
		}
		return valPrecision;
	}

	public static Instant normalizePrecision(Instant high, int targetPrecision,String roundingModeString) {
		long seconds = high.getEpochSecond();
		int nanos = high.getNano(); // 纳秒：0~999_999_999

		// 保留小数点后 targetPrecision 位纳秒（最多9位）
		BigDecimal nanoDecimal = BigDecimal.valueOf(nanos)
				.divide(BigDecimal.valueOf(1_000_000_000), 9, RoundingMode.HALF_UP);
		RoundingMode roundingMode = StringUtils.isBlank(roundingModeString) ? RoundingMode.HALF_UP : RoundingMode.valueOf(roundingModeString);
		BigDecimal roundedDecimal = nanoDecimal.setScale(targetPrecision, roundingMode);

		BigDecimal newNano = roundedDecimal.multiply(BigDecimal.valueOf(1_000_000_000));
		int newNanoInt = newNano.intValue();

		// 判断是否进位到下一秒
		if (newNanoInt >= 1_000_000_000) {
			return Instant.ofEpochSecond(seconds + 1, 0);
		} else {
			return Instant.ofEpochSecond(seconds, newNanoInt);
		}
	}

	/**
	 * @param val1
	 * @param val2
	 * @return 相等返回true，不相等返回false
	 */
	public static boolean compareBoolean(Boolean val1, Object val2) {
		if (null == val1 && null == val2){
			return true;
		}
		if (null == val1 || null == val2) {
			return false;
		}
		if (val2 instanceof Boolean) {
			return val1.equals(val2);
		}
		Object val2Boolean = CommonUtil.toBoolean(val2);
		if (val2Boolean instanceof BooleanNotExist) {
			return false;
		}
		return val1.equals(val2Boolean);
	}


	public static Object toBoolean(Object val) {
		if (val instanceof Number) {
			long intValue = ((Number) val).longValue();
			if (intValue == 0) {
				return false;
			} else if (intValue == 1) {
				return true;
			} else {
				return new BooleanNotExist();
			}
		}
		if (val instanceof BigDecimal) {
			BigDecimal bigDecimalValue = (BigDecimal) val;
			if (bigDecimalValue.compareTo(BigDecimal.ZERO) == 0) {
				return false;
			} else if (bigDecimalValue.compareTo(BigDecimal.ONE) == 0) {
				return true;
			} else {
				return new BooleanNotExist();
			}
		}
		if (val instanceof String) {
			String str = val.toString().toLowerCase().trim();
			if ("0".equals(str) || "false".equals(str)) {
				return false;
			} else if ("1".equals(str) || "true".equals(str)) {
				return true;
			} else {
				return new BooleanNotExist();
			}
		}
		return new BooleanNotExist();
	}

	public static boolean compare(Object val1, Object val2, boolean ignoreTimePrecision, String roundingMode) throws CompareException {
		try {
			if (val1 == null && val2 == null) return false;
			if (val1 == null || val2 == null) return true;

			if (val1 instanceof MysqlJson) return compare((MysqlJson) val1, val2, ignoreTimePrecision, roundingMode);
			if (val2 instanceof MysqlJson) return compare((MysqlJson) val2, val1, ignoreTimePrecision, roundingMode);
			if (val1 instanceof Map) return compare((Map) val1, val2, ignoreTimePrecision, roundingMode);
			if (val2 instanceof Map) return compare((Map) val2, val1, ignoreTimePrecision, roundingMode);
			if (val1 instanceof Collection) return compare((Collection) val1, val2, ignoreTimePrecision, roundingMode);
			if (val2 instanceof Collection) return compare((Collection) val2, val1, ignoreTimePrecision, roundingMode);
			if (val1 instanceof Boolean) return !compareBoolean((Boolean)val1,val2);
			if (val2 instanceof Boolean) return !compareBoolean((Boolean)val2,val1);
			if (ignoreTimePrecision) {
				if (val1 instanceof DateTime) {
					if (((DateTime) val1).isContainsIllegal()) {
						val1 = ((DateTime) val1).getIllegalDate();
					} else {
						val1 = ((DateTime) val1).toInstant();
					}
				}
				if (val2 instanceof DateTime) {
					if (((DateTime) val2).isContainsIllegal()) {
						val2 = ((DateTime) val2).getIllegalDate();
					} else {
						val2 = ((DateTime) val2).toInstant();
					}
				}
				if (val1 instanceof Instant && val2 instanceof Instant) {
					return CommonUtil.compareInstant((Instant) val1, (Instant) val2, ignoreTimePrecision, roundingMode);
				}
			}

			val1 = try2String(val1,ignoreTimePrecision);
			val2 = try2String(val2,ignoreTimePrecision);

			if (val1 instanceof String || val2 instanceof String) {
				val1 = val1.toString().trim();
				val2 = val2.toString().trim();
			} else if (val1 instanceof Byte || val2 instanceof Byte
					|| val1 instanceof Short || val2 instanceof Short
					|| val1 instanceof Integer || val2 instanceof Integer
					|| val1 instanceof Long || val2 instanceof Long) {
				val1 = new BigDecimal(val1.toString()).longValue();
				val2 = new BigDecimal(val2.toString()).longValue();
			} else if (val1 instanceof Float || val2 instanceof Float
					|| val1 instanceof Double || val2 instanceof Double
					|| val1 instanceof BigDecimal || val2 instanceof BigDecimal) {
				val1 = new BigDecimal(val1.toString()).doubleValue();
				val2 = new BigDecimal(val2.toString()).doubleValue();
			}

			return !val1.equals(val2);
		} catch (Throwable e) {
			throw new CompareException(e).sourceValue(val1).targetValue(val2);
		}
	}



	private static boolean compare(Map val, Object obj, boolean ignoreTimePrecision, String roundingMode) throws CompareException {
		if (obj instanceof Map) {
			if (val.size() != ((Map) obj).size()) return true;
			Map objVal = (Map) obj;
			Object v1, v2;
			for (Object k : val.keySet()) {
				v1 = val.get(k);
				v2 = objVal.get(k);
				if (compare(v1, v2, ignoreTimePrecision, roundingMode)) return true;
			}
			return false;
		} else if (obj instanceof String) {
			return obj2JsonCompare(val, obj, ignoreTimePrecision, roundingMode);
		}
		return true;
	}

	private static boolean compare(Collection val, Object obj, boolean ignoreTimePrecision, String roundingMode) throws CompareException {
		Object v1, v2;
		if (obj instanceof Collection) {
			if (val.size() != ((Collection) obj).size()) return true;
			Iterator i1 = val.iterator(), i2 = ((Collection) obj).iterator();
			for (int i = 0, len = val.size(); i < len; i++) {
				v1 = i1.next();
				v2 = i2.next();
				if (compare(v1, v2, ignoreTimePrecision, roundingMode)) return true;
			}
			return false;
		} else if (obj instanceof Array) {
			if (val.size() != Array.getLength(obj)) return true;
			Iterator i1 = val.iterator();
			for (int i = 0, len = val.size(); i < len; i++) {
				v1 = i1.next();
				v2 = Array.get(obj, i);
				if (compare(v1, v2, ignoreTimePrecision, roundingMode)) return true;
			}
			return false;
		} else if (obj instanceof String) {
			return obj2JsonCompare(val, obj, ignoreTimePrecision, roundingMode);
		}
		return true;
	}

	private static boolean compareArray(Object val, Object obj, boolean ignoreTimePrecision, String roundingMode) throws CompareException {
		int len = Array.getLength(val);
		Object v1, v2;
		if (obj instanceof Collection) {
			if (len != ((Collection) obj).size()) return true;
			Iterator i2 = ((Collection) obj).iterator();
			for (int i = 0; i < len; i++) {
				v1 = Array.get(val, i);
				v2 = i2.next();
				if (compare(v1, v2, ignoreTimePrecision, roundingMode)) return true;
			}
			return false;
		} else if (obj instanceof Array) {
			if (len != Array.getLength(obj)) return true;
			for (int i = 0; i < len; i++) {
				v1 = Array.get(val, i);
				v2 = Array.get(obj, i);
				if (compare(v1, v2, ignoreTimePrecision, roundingMode)) return true;
			}
			return false;
		} else if (obj instanceof String) {
			return obj2JsonCompare(val, obj, ignoreTimePrecision, roundingMode);
		}
		return true;
	}

	private static boolean compare(MysqlJson val, Object obj, boolean ignoreTimePrecision, String roundingMode) throws CompareException {
		try {
			if (obj instanceof Map) {
				return compare((Map) obj, val.toObject(), ignoreTimePrecision, roundingMode);
			} else if (obj instanceof Collection) {
				return compare((Collection) obj, val.toObject(), ignoreTimePrecision, roundingMode);
			} else if (obj instanceof Array) {
				return compareArray(obj, val.toObject(), ignoreTimePrecision, roundingMode);
			}
			return !val.getData().equals(obj.toString());
		} catch (Exception e) {
			throw new CompareException(e).sourceValue(val).targetValue(obj);
		}
	}

	private static boolean obj2JsonCompare(Object val, Object obj, boolean ignoreTimePrecision, String roundingMode) throws CompareException {
		try {
			JSONUtil.disableFeature(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
			return !JSONUtil.obj2Json(val).equals(obj.toString());
		} catch (JsonProcessingException e) {
			throw new CompareException(e).sourceValue(val).targetValue(obj);
		} finally {
			JSONUtil.enableFeature(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
		}
	}

	private static Object try2String(Object val, boolean ignoreTimePrecision) {
		if (val instanceof ObjectId) {
			return ((ObjectId) val).toHexString();
		} else if (val instanceof byte[]) {
			return new String((byte[]) val, StandardCharsets.UTF_8);
		} else if (val instanceof Date) {
			return ((Date) val).toInstant().toString();
		} else if (val instanceof Instant) {
			return try2IgnoreTimePrecision(((Instant) val).toString(),ignoreTimePrecision);
		} else if (val instanceof DateTime) {
			DateTime dateTime = (DateTime) val;
			if(dateTime.isContainsIllegal()){
				return try2IgnoreTimePrecision(dateTime.getIllegalDate(),ignoreTimePrecision);
			}else{
				return try2IgnoreTimePrecision(dateTime.toInstant().toString(),ignoreTimePrecision);
			}
		}
		return val;
	}

	private static Object try2IgnoreTimePrecision(String value, boolean ignoreTimePrecision){
		if(ignoreTimePrecision){
			return value.split("\\.")[0];
		}
		return value;
	}

	private static Object try2IgnoreTimePrecision(Object val){
		if(val instanceof DateTime dateTime){
            if(dateTime.isContainsIllegal()){
				return dateTime.getIllegalDate().split("\\.")[0];
			}else{
				return dateTime.toInstant().toString().split("\\.")[0];
			}
		}else if (val instanceof Instant dateTime){
			return dateTime.toString().split("\\.")[0];
		}
		return val;
	}
}
