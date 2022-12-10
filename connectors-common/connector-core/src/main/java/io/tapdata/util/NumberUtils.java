package io.tapdata.util;

import io.tapdata.entity.error.CoreException;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * @author aplomb
 */
public class NumberUtils {
	public static Number add(Object a, Object b) {
		if(a instanceof Double && b instanceof Double) {
			return (Double) a + (Double) b;
		} else if(a instanceof Float && b instanceof Float) {
			return (Float) a + (Float) b;
		} else if(a instanceof Long && b instanceof Long) {
			return (Long) a + (Long) b;
		} else if(a instanceof Integer && b instanceof Integer){
			return (Integer) a + (Integer) b;
		} else if(a instanceof BigDecimal && b instanceof BigDecimal){
			return ((BigDecimal) a).add((BigDecimal) b);
		} else if(a instanceof BigInteger && b instanceof BigInteger){
			return ((BigInteger) a).add((BigInteger) b);
		}
		return null;
	}

	public static Number subtract(Object a, Object b) {
		if(a instanceof Double && b instanceof Double) {
			return (Double) a - (Double) b;
		} else if(a instanceof Float && b instanceof Float) {
			return (Float) a - (Float) b;
		} else if(a instanceof Long && b instanceof Long) {
			return (Long) a - (Long) b;
		} else if(a instanceof Integer && b instanceof Integer){
			return (Integer) a - (Integer) b;
		} else if(a instanceof BigDecimal && b instanceof BigDecimal){
			return ((BigDecimal) a).subtract((BigDecimal) b);
		} else if(a instanceof BigInteger && b instanceof BigInteger){
			return ((BigInteger) a).subtract((BigInteger) b);
		}
		return null;
	}

	public static Number divide(Object a, Object b) {
		if(a instanceof Double && b instanceof Number) {
			return (Double) a / ((Number) b).doubleValue();
		} else if(a instanceof Float && b instanceof Number) {
			return (Float) a / ((Number) b).floatValue();
		} else if(a instanceof Long && b instanceof Number) {
			return (Long) a / ((Number) b).longValue();
		} else if(a instanceof Integer && b instanceof Number){
			return (Integer) a / ((Number) b).intValue();
		} else if(a instanceof BigDecimal && b instanceof Number){
			return ((BigDecimal) a).divide(BigDecimal.valueOf(((Number) b).doubleValue()));
		} else if(a instanceof BigInteger && b instanceof Number){
			return ((BigInteger) a).divide(BigInteger.valueOf(((Number) b).longValue()));
		}
		return null;
	}

	public static Number multiply(Object a, Object b) {

		if(a instanceof Double && b instanceof Number) {
			return (Double) a * ((Number) b).doubleValue();
		} else if(a instanceof Float && b instanceof Number) {
			return (Float) a * ((Number) b).floatValue();
		} else if(a instanceof Long && b instanceof Number) {
			return (Long) a * ((Number) b).longValue();
		} else if(a instanceof Integer && b instanceof Number){
			return (Integer) a * ((Number) b).intValue();
		} else if(a instanceof BigDecimal && b instanceof Number){
			return ((BigDecimal) a).multiply(BigDecimal.valueOf(((Number) b).doubleValue()));
		} else if(a instanceof BigInteger && b instanceof Number){
			return ((BigInteger) a).multiply(BigInteger.valueOf(((Number) b).longValue()));
		}
		return null;
	}

}
