package io.tapdata.huawei.drs.kafka.serialization.mysql.types;

import io.tapdata.huawei.drs.kafka.types.BytesType;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/21 18:03 Create
 */
public class VarbinaryMysqlType extends BytesType {
    public VarbinaryMysqlType() {
        super("varbinary");
    }
}
