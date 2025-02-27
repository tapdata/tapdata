package io.tapdata.huawei.drs.kafka.serialization.mysql.types;

import io.tapdata.huawei.drs.kafka.types.NumberType;

import java.math.BigDecimal;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/22 17:10 Create
 */
public class BigintMysqlType extends NumberType {
    public BigintMysqlType() {
        super("bigint", BigDecimal.valueOf(Long.MIN_VALUE), BigDecimal.valueOf(Long.MAX_VALUE));
    }
}
