package io.tapdata.huawei.drs.kafka.serialization.mysql.types;

import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.entity.schema.value.TapNumberValue;
import io.tapdata.huawei.drs.kafka.types.BasicType;

import java.math.BigDecimal;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/21 18:03 Create
 */
public class IntMysqlType extends BasicType {
    public IntMysqlType() {
        super("int");
    }

    @Override
    public Object decode(Object value) {
        if (value instanceof String) {
            value = new TapNumberValue(Double.parseDouble((String) value)).tapType(new TapNumber()
                .minValue(BigDecimal.valueOf(Integer.MIN_VALUE))
                .maxValue(BigDecimal.valueOf(Integer.MAX_VALUE))
            );
        }
        return value;
    }
}
