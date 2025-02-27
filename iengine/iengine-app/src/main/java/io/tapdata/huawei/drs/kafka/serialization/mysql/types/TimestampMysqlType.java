package io.tapdata.huawei.drs.kafka.serialization.mysql.types;

import io.tapdata.entity.schema.type.TapDateTime;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.huawei.drs.kafka.types.BasicType;

import java.math.BigDecimal;

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
        TapDateTimeValue result = new TapDateTimeValue();
        result.originValue(value).tapType(new TapDateTime().fraction(3));

        if (null == value) {
            return result;
        } else if (value instanceof String) {
            result.value(double2DateTime(new BigDecimal((String) value)));
        } else if (value instanceof Number) {
            result.value(double2DateTime(new BigDecimal(value.toString())));
        } else {
            return value;
        }
        return result;
    }

    protected DateTime double2DateTime(BigDecimal decimal) {
        decimal = decimal.multiply(new BigDecimal(1000));
        return new DateTime(decimal.longValue(), 3);
    }
}
