package io.tapdata.huawei.drs.kafka.serialization.mysql.types;

import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.entity.schema.value.TapNumberValue;
import io.tapdata.huawei.drs.kafka.types.BasicType;

import java.math.BigDecimal;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/21 18:03 Create
 */
public class DecimalMysqlType extends BasicType {
    public DecimalMysqlType() {
        super("decimal");
    }

    public DecimalMysqlType(String type) {
        super(type);
    }

    @Override
    public Object decode(Object value) {
        if (value instanceof String) {
            String valStr = (String) value;
            BigDecimal decimal = new BigDecimal(valStr);
            value = new TapNumberValue(decimal.doubleValue()).tapType(new TapNumber()
                .minValue(BigDecimal.valueOf(Double.MIN_VALUE))
                .maxValue(BigDecimal.valueOf(Double.MAX_VALUE))
            );
        }
        return value;
    }
}
