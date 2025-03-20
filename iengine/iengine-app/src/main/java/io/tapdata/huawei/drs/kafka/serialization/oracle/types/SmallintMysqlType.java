package io.tapdata.huawei.drs.kafka.serialization.oracle.types;

import io.tapdata.huawei.drs.kafka.types.NumberType;

import java.math.BigDecimal;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/23 15:02 Create
 */
public class SmallintMysqlType extends NumberType {
    public SmallintMysqlType() {
        super("smallint", BigDecimal.valueOf(-32768), BigDecimal.valueOf(32767));
    }

}
