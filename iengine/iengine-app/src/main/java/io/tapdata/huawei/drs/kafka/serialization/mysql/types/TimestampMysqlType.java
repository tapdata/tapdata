package io.tapdata.huawei.drs.kafka.serialization.mysql.types;

import io.tapdata.entity.schema.type.TapDateTime;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.huawei.drs.kafka.types.BasicType;

import java.time.Instant;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/21 18:03 Create
 */
public class TimestampMysqlType extends BasicType {
    public TimestampMysqlType() {
        super("timestamp");
    }

    @Override
    public Object decode(Object value) {
        if (value instanceof String) {
            String valStr = (String) value;
            double valDouble = Double.parseDouble(valStr);
            Instant instant = Instant.ofEpochMilli((long) valDouble * 1000L);
            DateTime dateTime = new DateTime(instant);
            value = new TapDateTimeValue(dateTime).tapType(new TapDateTime());
        }
        return value;
    }
}
