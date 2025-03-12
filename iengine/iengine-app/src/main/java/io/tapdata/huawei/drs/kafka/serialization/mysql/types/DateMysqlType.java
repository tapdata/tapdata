package io.tapdata.huawei.drs.kafka.serialization.mysql.types;

import io.tapdata.entity.schema.type.TapDate;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.schema.value.TapDateValue;
import io.tapdata.huawei.drs.kafka.types.BasicType;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/22 17:12 Create
 */
public class DateMysqlType extends BasicType {
    public DateMysqlType() {
        super("date");
    }

    @Override
    public Object decode(Object value) {
        TapDateValue result = new TapDateValue();
        result.originValue(value).tapType(new TapDate());

        if (null == value) {
            return result;
        } else if (value instanceof String) {
            DateTime dateTime = DateTime.withDateStr((String) value);
            result.value(dateTime);
        } else {
            return value;
        }
        return result;
    }
}
