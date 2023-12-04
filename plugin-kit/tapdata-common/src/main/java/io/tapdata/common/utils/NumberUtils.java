package io.tapdata.common.utils;

import java.math.BigInteger;

public class NumberUtils {
    public static Number addNumbers(Number a, Number b) {
        if(a instanceof Double || b instanceof Double) {
            return a.doubleValue() + b.doubleValue();
        } else if(a instanceof Float || b instanceof Float) {
            return a.floatValue() + b.floatValue();
        } else if(a instanceof Long || b instanceof Long) {
            return a.longValue() + b.longValue();
        } else if (a instanceof BigInteger || b instanceof BigInteger){
            return new BigInteger(a.toString()).add(new BigInteger(b.toString()));
        } else {
            return a.longValue() + b.longValue();
        }
    }

    public static Number divideNumbers(Number a, Number b) {
        if(a instanceof Double || b instanceof Double) {
            return a.doubleValue() / b.doubleValue();
        } else if(a instanceof Float || b instanceof Float) {
            return a.floatValue() / b.floatValue();
        } else if(a instanceof Long || b instanceof Long) {
            return a.longValue() / b.longValue();
        } else if (a instanceof BigInteger || b instanceof BigInteger) {
            return new BigInteger(a.toString()).divide(new BigInteger(b.toString()));
        } else {
            return a.intValue() / b.intValue();
        }
    }
}
