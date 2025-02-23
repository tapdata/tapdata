package io.tapdata.huawei.drs.kafka.types;

import io.tapdata.entity.schema.type.TapString;
import io.tapdata.entity.schema.value.TapStringValue;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/23 14:19 Create
 */
public class StringType extends BasicType {
    public StringType(String type) {
        super(type);
    }

    @Override
    public Object decode(Object value) {
        String str;
        if (null == value) {
            return null;
        } else if (value instanceof String) {
            str = (String) value;
        } else if (value instanceof byte[]) {
            str = new String((byte[]) value);
        } else {
            str = value.toString();
        }
        return new TapStringValue(str).tapType(new TapString());
    }
}
