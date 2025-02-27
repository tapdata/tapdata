package io.tapdata.huawei.drs.kafka.serialization.oracle.types;

import io.tapdata.entity.schema.type.TapBoolean;
import io.tapdata.entity.schema.value.TapBooleanValue;
import io.tapdata.entity.schema.value.TapValue;
import io.tapdata.huawei.drs.kafka.types.BytesType;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/23 14:55 Create
 */
public class BooleanOracleType extends BytesType {
    public BooleanOracleType() {
        super("boolean");
    }

    public BooleanOracleType(String type) {
        super(type);
    }

    @Override
    public Object decode(Object value) {
        TapBooleanValue result = new TapBooleanValue();
        result.originValue(value).tapType(new TapBoolean());

        if (null == value) {
            return result;
        } else if (value instanceof Boolean) {
            result.value((Boolean) value);
        } else if (value instanceof String) {
            result.value(Boolean.parseBoolean((String) value));
        } else if (value instanceof Number) {
            Number numValue = (Number) value;
            result.value(numValue.doubleValue() > 0);
        } else if (value instanceof TapValue) {
            return value;
        } else {
            result.value(Boolean.parseBoolean(value.toString()));
        }
        return result;
    }
}
