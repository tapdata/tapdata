package io.tapdata.huawei.drs.kafka.serialization.mysql.types;

import io.tapdata.huawei.drs.kafka.types.NumberType;

import java.math.BigDecimal;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/21 18:03 Create
 */
public class DecimalMysqlType extends NumberType {
    public DecimalMysqlType() {
        super("decimal", BigDecimal.valueOf(Double.MIN_VALUE), BigDecimal.valueOf(Double.MAX_VALUE));
    }

    public DecimalMysqlType(String type) {
        super(type, BigDecimal.valueOf(Double.MIN_VALUE), BigDecimal.valueOf(Double.MAX_VALUE));
    }
}
