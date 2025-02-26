package io.tapdata.huawei.drs.kafka.serialization.mysql.types;

import io.tapdata.entity.schema.type.TapDateTime;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.huawei.drs.kafka.types.BasicType;

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
        if (value instanceof String) {
            String str = (String) value;
            LocalDateTime localDateTime = LocalDateTime.parse(str, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            DateTime dateTime = new DateTime(localDateTime.toInstant(ZoneOffset.UTC));
            value = new TapDateTimeValue(dateTime).tapType(new TapDateTime());
        }
        return value;
    }
}
