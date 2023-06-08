package com.tapdata.tm.utils;

import java.math.BigInteger;

public class NumberUtil {
    public static BigInteger parseDataTotal(Object param) {
        if (param == null) {
            return BigInteger.ZERO;
        }
        if ("null".equals(param)) {
            return BigInteger.ZERO;
        }

        if (param instanceof Long) {
            return BigInteger.valueOf(Long.parseLong(String.valueOf(param)));
        } else if (param instanceof BigInteger) {
            return new BigInteger(String.valueOf(param));
        }

        return new BigInteger(String.valueOf(param));
    }
}
