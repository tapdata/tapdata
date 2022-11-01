package io.tapdata.connector.csv.util;

import io.tapdata.util.DateUtil;

import java.math.BigDecimal;

public class MatchUtil {

    public static boolean matchBoolean(String value) {
        return "true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value);
    }

    public static boolean matchNumber(String value) {
        String reg = "(^[+-]?\\d+)|(^[+-]?\\d+\\.\\d+)";
        return value.matches(reg);
    }

    public static boolean matchInteger(String value) {
        String reg = "^[+-]?\\d+";
        if (value.matches(reg)) {
            BigDecimal bigDecimal = new BigDecimal(value);
            return bigDecimal.compareTo(BigDecimal.valueOf(Integer.MAX_VALUE)) < 0 &&
                    bigDecimal.compareTo(BigDecimal.valueOf(Integer.MIN_VALUE)) > 0;
        }
        return false;
    }

    public static boolean matchDateTime(String value) {
        Object res = DateUtil.parse(value);
        return !(res instanceof String) || !value.equals(res);
    }
}
