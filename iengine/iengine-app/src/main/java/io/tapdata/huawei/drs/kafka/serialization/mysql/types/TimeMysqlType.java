package io.tapdata.huawei.drs.kafka.serialization.mysql.types;

import io.tapdata.entity.schema.type.TapDateTime;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.huawei.drs.kafka.serialization.IType;

/**
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/22 17:18 Create
 */
public class TimeMysqlType implements IType {
    @Override
    public String type() {
        return "time";
    }

    @Override
    public Object decode(Object value) {
        if (value instanceof String) {
            String str = (String) value;
            DateTime dateTime = new DateTime(str, "time");
            value = new TapDateTimeValue(dateTime).tapType(new TapDateTime());
        }
        return value;
    }

    @Override
    public Object encode(Object value) {
        return null;
    }
}
