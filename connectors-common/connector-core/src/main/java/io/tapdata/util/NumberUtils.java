package io.tapdata.util;

import io.tapdata.entity.error.CoreException;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * @author aplomb
 */
public class NumberUtils {
	public static Object add(Object a, Object b) {
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

	public static Object subtract(Object a, Object b) {
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
		if(a instanceof Double && b instanceof Double) {
			return (Double) a / (Double) b;
		} else if(a instanceof Float && b instanceof Float) {
			return (Float) a / (Float) b;
		} else if(a instanceof Long && b instanceof Long) {
			return (Long) a / (Long) b;
		} else if(a instanceof Integer && b instanceof Integer){
			return (Integer) a / (Integer) b;
		} else if(a instanceof BigDecimal && b instanceof BigDecimal){
			return ((BigDecimal) a).divide((BigDecimal) b);
		} else if(a instanceof BigInteger && b instanceof BigInteger){
			return ((BigInteger) a).divide((BigInteger) b);
		}
		return null;
	}

	public static Number multiply(Object a, Object b) {
		if(a instanceof Double && b instanceof Double) {
			return (Double) a * (Double) b;
		} else if(a instanceof Float && b instanceof Float) {
			return (Float) a * (Float) b;
		} else if(a instanceof Long && b instanceof Long) {
			return (Long) a * (Long) b;
		} else if(a instanceof Integer && b instanceof Integer){
			return (Integer) a * (Integer) b;
		} else if(a instanceof BigDecimal && b instanceof BigDecimal){
			return ((BigDecimal) a).multiply((BigDecimal) b);
		} else if(a instanceof BigInteger && b instanceof BigInteger){
			return ((BigInteger) a).multiply((BigInteger) b);
		}
		return null;
	}

}
