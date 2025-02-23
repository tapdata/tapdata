package io.tapdata.huawei.drs.kafka.serialization.oracle.types;

import io.tapdata.huawei.drs.kafka.types.StringType;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/21 18:03 Create
 */
public class VarcharOracleType extends StringType {
    public VarcharOracleType() {
        super("varchar");
    }
}
