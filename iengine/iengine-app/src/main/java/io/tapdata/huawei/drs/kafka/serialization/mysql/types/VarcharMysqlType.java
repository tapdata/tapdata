package io.tapdata.huawei.drs.kafka.serialization.mysql.types;

import io.tapdata.entity.schema.type.TapString;
import io.tapdata.entity.schema.value.TapStringValue;
import io.tapdata.huawei.drs.kafka.serialization.IType;

/**
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/21 18:03 Create
 */
public class VarcharMysqlType implements IType {
    @Override
    public String type() {
        return "varchar";
    }

    @Override
    public Object decode(Object value) {
        if (value instanceof String) {
            value = new TapStringValue((String) value).tapType(new TapString());
        }
        return value;
    }

    @Override
    public Object encode(Object value) {
        return null;
    }
}
