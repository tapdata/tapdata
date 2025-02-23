package io.tapdata.huawei.drs.kafka.serialization.oracle.types;

import io.tapdata.entity.schema.type.TapBoolean;
import io.tapdata.entity.schema.value.TapBooleanValue;
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
        boolean boolValue;
        if (value instanceof Boolean) {
            boolValue = (Boolean) value;
        } else if (value instanceof String) {
            boolValue = Boolean.parseBoolean((String) value);
        } else if (value instanceof Number) {
            Number numValue = (Number) value;
            boolValue = numValue.doubleValue() > 0;
        } else {
            return value;
        }
        return new TapBooleanValue(boolValue).tapType(new TapBoolean());
    }
}
