package io.tapdata.construct.utils;

import io.tapdata.entity.schema.value.DateTime;
import org.bson.types.Decimal128;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class DataUtil {

    private DataUtil() {
    }

    public static Object replaceDataValue(Object data) {
        if (data instanceof Map) {
            Map<String, Object> dataMap = ((Map<String, Object>) data);
            dataMap.forEach((key, value) -> {
                dataMap.replace(key, replaceDataValue(value));
            });
        } else if (data instanceof List) {
            List<Object> dataList = (List<Object>) data;
            for (int i = 0; i < dataList.size(); i++) {
                dataList.set(i, replaceDataValue(dataList.get(i)));
            }
        }
        return convertDataType(data);
    }

    public static Object convertDataType(Object data) {
        if (data instanceof Instant) {
            Instant instant = (Instant) data;
            return Date.from(instant);
        } else if (data instanceof BigDecimal) {
            BigDecimal bigDecimal = (BigDecimal) data;
            if (bigDecimal.precision() > 34) {
                Decimal128 decimal128 = new Decimal128(bigDecimal.setScale(bigDecimal.scale() + 34 - bigDecimal.precision(), RoundingMode.HALF_UP));
                return decimal128;
            } else {
                return new Decimal128(bigDecimal);
            }
        } else if (data instanceof BigInteger) {
            BigInteger bigInteger = (BigInteger) data;
            return bigInteger.toString();
        } else if (data instanceof DateTime) {
            DateTime dateTime = (DateTime) data;
            return dateTime.toDate();
        } else {
            return data;
        }
    }
}
