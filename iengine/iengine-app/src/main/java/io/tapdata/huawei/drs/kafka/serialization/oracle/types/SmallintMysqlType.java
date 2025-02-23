package io.tapdata.huawei.drs.kafka.serialization.oracle.types;

import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.entity.schema.value.TapNumberValue;
import io.tapdata.huawei.drs.kafka.types.BasicType;

import java.math.BigDecimal;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/23 15:02 Create
 */
public class SmallintMysqlType extends BasicType {
    public SmallintMysqlType() {
        super("smallint");
    }

    @Override
    public Object decode(Object value) {
        Double doubleValue;
        if (null == value) {
            doubleValue = null;
        } else if (value instanceof String) {
            doubleValue = Double.parseDouble((String) value);
        } else if (value instanceof Number) {
            doubleValue = ((Number) value).doubleValue();
        } else {
            return value;
        }
        return new TapNumberValue(doubleValue).tapType(new TapNumber()
            .minValue(BigDecimal.valueOf(-32768))
            .maxValue(BigDecimal.valueOf(32767))
        );
    }
}
