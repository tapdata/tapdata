package io.tapdata.huawei.drs.kafka.serialization.mysql.types;

import io.tapdata.entity.schema.type.TapTime;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.schema.value.TapTimeValue;
import io.tapdata.huawei.drs.kafka.types.BasicType;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/22 17:18 Create
 */
public class TimeMysqlType extends BasicType {
    public TimeMysqlType() {
        super("time");
    }

    @Override
    public Object decode(Object value) {
        if (value instanceof String) {
            String str = (String) value;
            DateTime dateTime = DateTime.withTimeStr(str);
            value = new TapTimeValue(dateTime).tapType(new TapTime());
        }
        return value;
    }
}
