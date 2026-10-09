package com.tapdata.tm.inspect.util;

import com.tapdata.tm.inspect.dto.InspectDetailsDto;
import org.bson.types.Decimal128;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 校验差异明细返回给前端时，JS Number(double) 无法精确表示的数值转为字符串，避免页面显示丢失精度
 * <p>
 * 引擎二次校验依赖明细中的原始类型(如数值主键)，需通过 /api/InspectDetails/raw 读取原始数据
 */
public class InspectDetailsNumberUtils {

	/**
	 * JS Number.MAX_SAFE_INTEGER
	 */
	public static final long MAX_SAFE_INTEGER = 9007199254740991L;
	/**
	 * double 可精确往返的十进制有效位数
	 */
	public static final int MAX_SAFE_PRECISION = 15;
	private static final BigInteger MAX_SAFE_BIG_INTEGER = BigInteger.valueOf(MAX_SAFE_INTEGER);

	private InspectDetailsNumberUtils() {
	}

	public static <T extends Collection<InspectDetailsDto>> T toJsSafe(T details) {
		if (null != details) {
			details.forEach(InspectDetailsNumberUtils::toJsSafe);
		}
		return details;
	}

	public static InspectDetailsDto toJsSafe(InspectDetailsDto detail) {
		if (null != detail) {
			detail.setSource(toJsSafeMap(detail.getSource()));
			detail.setTarget(toJsSafeMap(detail.getTarget()));
		}
		return detail;
	}

	protected static Map<String, Object> toJsSafeMap(Map<String, Object> record) {
		if (null == record) {
			return null;
		}
		Map<String, Object> result = new LinkedHashMap<>();
		for (Map.Entry<String, Object> entry : record.entrySet()) {
			result.put(entry.getKey(), toJsSafeValue(entry.getValue()));
		}
		return result;
	}

	@SuppressWarnings("unchecked")
	protected static Object toJsSafeValue(Object value) {
		if (value instanceof Map) {
			return toJsSafeMap((Map<String, Object>) value);
		} else if (value instanceof Collection) {
			List<Object> result = new ArrayList<>(((Collection<?>) value).size());
			for (Object item : (Collection<?>) value) {
				result.add(toJsSafeValue(item));
			}
			return result;
		} else if (value instanceof Long) {
			long l = (Long) value;
			return (l > MAX_SAFE_INTEGER || l < -MAX_SAFE_INTEGER) ? String.valueOf(l) : value;
		} else if (value instanceof BigInteger) {
			return ((BigInteger) value).abs().compareTo(MAX_SAFE_BIG_INTEGER) > 0 ? value.toString() : value;
		} else if (value instanceof BigDecimal) {
			return toJsSafeDecimal((BigDecimal) value);
		} else if (value instanceof Decimal128) {
			Decimal128 decimal128 = (Decimal128) value;
			if (decimal128.isNaN() || decimal128.isInfinite()) {
				return decimal128.toString();
			}
			try {
				return toJsSafeDecimal(decimal128.bigDecimalValue());
			} catch (ArithmeticException e) {
				// 负零无法转为 BigDecimal
				return decimal128.toString();
			}
		}
		return value;
	}

	protected static Object toJsSafeDecimal(BigDecimal value) {
		if (value.stripTrailingZeros().precision() > MAX_SAFE_PRECISION) {
			return value.toPlainString();
		}
		return value;
	}
}
