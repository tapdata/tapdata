package io.tapdata.huawei.drs.kafka.serialization.mysql.types;

import io.tapdata.entity.schema.type.TapDateTime;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.huawei.drs.kafka.serialization.IType;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/22 17:12 Create
 */
public class DateMysqlType implements IType {
    @Override
    public String type() {
        return "date";
    }

    @Override
    public Object decode(Object value) {
        if (value instanceof String) {
            String str = (String) value;
            DateTime dateTime = new DateTime(str, "date");
            value = new TapDateTimeValue(dateTime).tapType(new TapDateTime());
        }
        return value;
    }

    @Override
    public Object encode(Object value) {
        return null;
    }
}
