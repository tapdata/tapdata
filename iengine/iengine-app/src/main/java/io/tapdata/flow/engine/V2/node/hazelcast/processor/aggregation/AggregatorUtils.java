package io.tapdata.flow.engine.V2.node.hazelcast.processor.aggregation;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * @author alexouyang
 * @Date 2022/4/29
 */
public class AggregatorUtils {

	public static BigDecimal getBigDecimal(Object value) {
		BigDecimal ret = null;
		if (value != null) {
			if (value instanceof BigDecimal) {
				ret = (BigDecimal) value;
			} else if (value instanceof String) {
				ret = new BigDecimal((String) value);
			} else if (value instanceof BigInteger) {
				ret = new BigDecimal((BigInteger) value);
			} else if (value instanceof Number) {
				ret = new BigDecimal(value.toString());
			} else {
				throw new ClassCastException("Not possible to coerce [" + value + "] from class " + value.getClass() + " into a BigDecimal.");
			}
		}
		return ret;
	}
}
