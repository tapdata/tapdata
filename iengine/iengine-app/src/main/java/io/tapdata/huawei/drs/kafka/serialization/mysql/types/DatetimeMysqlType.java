package io.tapdata.huawei.drs.kafka.serialization.mysql.types;

import io.tapdata.entity.schema.type.TapDateTime;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.huawei.drs.kafka.types.BasicType;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/21 18:03 Create
 */
public class DatetimeMysqlType extends BasicType {
    public DatetimeMysqlType() {
        super("datetime");
    }

    @Override
    public Object decode(Object value) {
        TapDateTimeValue result = new TapDateTimeValue();
        result.originValue(value).tapType(new TapDateTime());

        if (null == value) {
            return result;
        } else if (value instanceof String) {
            LocalDateTime localDateTime = LocalDateTime.parse((String) value, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            Instant instant = localDateTime.toInstant(ZoneOffset.UTC);
            DateTime dateTime = new DateTime(instant);
            result.value(dateTime);
        } else {
            return value;
        }
        return result;
    }
}
