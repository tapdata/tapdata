package io.tapdata.huawei.drs.kafka.types;

import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.entity.schema.value.TapNumberValue;
import io.tapdata.entity.schema.value.TapValue;

import java.math.BigDecimal;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/27 11:16 Create
 */
public class NumberType extends BasicType {
    protected final BigDecimal min;
    protected final BigDecimal max;

    public NumberType(String type, BigDecimal min, BigDecimal max) {
        super(type);
        this.min = min;
        this.max = max;
    }

    @Override
    public Object decode(Object value) {
        TapNumberValue result = new TapNumberValue();
        result.originValue(value).tapType(new TapNumber().minValue(min).maxValue(max));

        if (null == value) {
            return result;
        } else if (value instanceof String) {
            result.value(Double.parseDouble((String) value));
        } else if (value instanceof byte[]) {
            result.value(Double.parseDouble(new String((byte[]) value)));
        } else if (value instanceof Number) {
            result.value(((Number) value).doubleValue());
        } else if (value instanceof TapValue) {
            return value;
        } else {
            result.value(Double.parseDouble(value.toString()));
        }
        return result;
    }
}
