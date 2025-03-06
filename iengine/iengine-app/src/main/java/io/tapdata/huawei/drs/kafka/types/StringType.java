package io.tapdata.huawei.drs.kafka.types;

import io.tapdata.entity.schema.type.TapString;
import io.tapdata.entity.schema.value.TapStringValue;
import io.tapdata.entity.schema.value.TapValue;

import java.util.function.Function;

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
        return decodeString(value, Object::toString);
    }

    protected Object decodeString(Object value, Function<Object, String> otherFn) {
        TapStringValue result = new TapStringValue();
        result.originValue(value).tapType(new TapString());

        if (null == value) {
            return result;
        } else if (value instanceof String) {
            result.value((String) value);
        } else if (value instanceof byte[]) {
            result.value(new String((byte[]) value));
        } else if (value instanceof TapValue) {
            return value;
        } else {
            result.value(otherFn.apply(value));
        }
        return result;

    }
}
