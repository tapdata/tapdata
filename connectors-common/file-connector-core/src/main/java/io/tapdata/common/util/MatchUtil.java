package io.tapdata.common.util;

import io.tapdata.kit.EmptyKit;
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
        Object res = DateUtil.parseInstant(value);
        return !(res instanceof String) || !value.equals(res);
    }

    public static Object parse(String str, String dataType) {
        if (EmptyKit.isEmpty(str)) {
            return null;
        }
        switch (dataType) {
            case "BOOLEAN":
                return "true".equalsIgnoreCase(str);
            case "INTEGER":
                return Integer.parseInt(str);
            case "NUMBER":
                return new BigDecimal(str);
            case "DATETIME":
                return DateUtil.parseInstant(str);
            default:
                return str;
        }
    }
}
