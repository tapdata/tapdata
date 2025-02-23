package io.tapdata.huawei.drs.kafka.types;

import io.tapdata.entity.schema.type.TapString;
import io.tapdata.entity.schema.value.TapStringValue;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/22 21:42 Create
 */
public class JsonType extends BasicType {
    public JsonType() {
        super("json");
    }

    public JsonType(String type) {
        super(type);
    }

    @Override
    public Object decode(Object value) {
        String strValue;
        if (value instanceof String) {
            strValue = (String) value;
        } else if (value instanceof byte[]) {
            strValue = new String((byte[]) value);
        } else {
            return value;
        }
        return new TapStringValue(strValue).tapType(new TapString());
    }
}
