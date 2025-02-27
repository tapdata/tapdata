package io.tapdata.huawei.drs.kafka.serialization.oracle.types;

import io.tapdata.entity.schema.type.TapDateTime;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.huawei.drs.kafka.types.BasicType;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/23 16:45 Create
 */
public class TimestampWithoutTimeZoneMysqlType extends BasicType {
    public TimestampWithoutTimeZoneMysqlType() {
        super("timestamp without time zone");
    }

    @Override
    public Object decode(Object value) {
        TapDateTimeValue result = new TapDateTimeValue();
        result.originValue(value).tapType(new TapDateTime().fraction(6));

        if (null == value) {
            return result;
        } else if (value instanceof String) {
            String str = (String) value;
            LocalDateTime localDateTime = LocalDateTime.parse(str, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS"));
            DateTime dateTime = new DateTime(localDateTime.toInstant(ZoneOffset.UTC));
            result.value(dateTime);
        } else {
            return value;
        }
        return result;
    }
}
