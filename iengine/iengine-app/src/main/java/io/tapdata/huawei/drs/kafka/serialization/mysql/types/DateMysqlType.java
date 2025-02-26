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
        if (value instanceof String) {
            String str = (String) value;
            DateTime dateTime = DateTime.withDateStr(str);
            value = new TapDateValue(dateTime).tapType(new TapDate());
        }
        return value;
    }
}
