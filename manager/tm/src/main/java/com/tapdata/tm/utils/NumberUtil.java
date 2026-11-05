package com.tapdata.tm.utils;

import java.math.BigInteger;
import java.security.SecureRandom;

public class NumberUtil {
    private static final SecureRandom RANDOM = new SecureRandom();

    public static int getRandomNumber(int max) {
        return RANDOM.nextInt(max);
    }

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
