package io.tapdata.huawei.drs.kafka.serialization.mysql.types;

import io.tapdata.huawei.drs.kafka.types.NumberType;

import java.math.BigDecimal;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/21 18:03 Create
 */
public class FloatMysqlType extends NumberType {
    public FloatMysqlType() {
        super("float", BigDecimal.valueOf(Float.MIN_VALUE), BigDecimal.valueOf(Float.MAX_VALUE));
    }
}
