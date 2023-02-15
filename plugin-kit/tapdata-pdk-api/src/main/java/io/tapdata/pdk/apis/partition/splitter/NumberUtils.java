package io.tapdata.pdk.apis.partition.splitter;

import io.tapdata.entity.error.CoreException;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;

/**
 * @author aplomb
 */
public class NumberUtils {
	public static Number add(Object a, Object b) {
		if(a instanceof Double && b instanceof Number) {
			return (Double) a + ((Number) b).doubleValue();
		} else if(a instanceof Float && b instanceof Number) {
			return (Float) a + ((Number) b).floatValue();
		} else if(a instanceof Long && b instanceof Number) {
			return (Long) a + ((Number) b).longValue();
		} else if(a instanceof Integer && b instanceof Number){
			return (Integer) a + ((Number) b).intValue();
		} else if(a instanceof BigDecimal && b instanceof Number){
			return ((BigDecimal) a).add(new BigDecimal(b.toString()));
		} else if(a instanceof BigInteger && b instanceof Number){
			return ((BigInteger) a).add(new BigInteger(b.toString()));
		} else if(a instanceof Byte && b instanceof Number) {
			return (Byte)a + ((Number) b).byteValue();
		} else if(a instanceof Short && b instanceof Number) {
			return (Short)a + ((Number) b).shortValue();
		}
		return null;
	}

	public static Number subtract(Object a, Object b) {
		if(a instanceof Double && b instanceof Number) {
			return (Double) a - ((Number) b).doubleValue();
		} else if(a instanceof Float && b instanceof Number) {
			return (Float) a - ((Number) b).floatValue();
		} else if(a instanceof Long && b instanceof Number) {
			return (Long) a - ((Number) b).longValue();
		} else if(a instanceof Integer && b instanceof Number){
			return (Integer) a - ((Number) b).intValue();
		} else if(a instanceof BigDecimal && b instanceof Number){
			return ((BigDecimal) a).subtract(new BigDecimal((b.toString())));
		} else if(a instanceof BigInteger && b instanceof Number){
			return ((BigInteger) a).subtract(new BigInteger(b.toString()));
		} else if(a instanceof Byte && b instanceof Number) {
			return ((Byte) a + ((Number) b).byteValue());
		} else if(a instanceof Short && b instanceof Number) {
			return ((Short) a + ((Number) b).shortValue());
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
			return ((BigDecimal) a).divide(new BigDecimal(b.toString()), RoundingMode.HALF_UP);
		} else if(a instanceof BigInteger && b instanceof Number){
			return ((BigInteger) a).divide(new BigInteger(b.toString()));
		} else if(a instanceof Byte && b instanceof Number) {
			return (Byte) a / ((Number) b).byteValue();
		} else if(a instanceof Short && b instanceof Number) {
			return (Short) a / ((Number) b).shortValue();
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
			return ((BigDecimal) a).multiply(new BigDecimal(b.toString()));
		} else if(a instanceof BigInteger && b instanceof Number){
			return ((BigInteger) a).multiply(new BigInteger(b.toString()));
		} else if(a instanceof Byte && b instanceof Number) {
			return (Byte) a * ((Number) b).byteValue();
		} else if(a instanceof Short && b instanceof Number) {
			return (Short) a * ((Number) b).shortValue();
		}
		return null;
	}
	public static int compareTo(Object a, Object b) {
		if(a instanceof Double && b instanceof Number) {
			return ((Double) a).compareTo(((Number) b).doubleValue());
		} else if(a instanceof Float && b instanceof Number) {
			return ((Float) a).compareTo(((Number) b).floatValue());
		} else if(a instanceof Long && b instanceof Number) {
			return ((Long) a).compareTo(((Number) b).longValue());
		} else if(a instanceof Integer && b instanceof Number){
			return ((Integer) a).compareTo(((Number) b).intValue());
		} else if(a instanceof BigDecimal && b instanceof Number){
			return ((BigDecimal) a).compareTo(new BigDecimal(b.toString()));
		} else if(a instanceof BigInteger && b instanceof Number){
			return ((BigInteger) a).compareTo(new BigInteger(b.toString()));
		} else if(a instanceof Byte && b instanceof Number) {
			return ((Byte) a).compareTo(((Number) b).byteValue());
		} else if(a instanceof Short && b instanceof Number) {
			return ((Short)a).compareTo(((Number) b).shortValue());
		}
		return 0;
	}
}
