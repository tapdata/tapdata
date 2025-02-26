package io.tapdata.huawei.drs.kafka.types;

import io.tapdata.entity.schema.type.TapBinary;
import io.tapdata.entity.schema.value.TapBinaryValue;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/23 14:23 Create
 */
public class BytesType extends BasicType {
    public BytesType(String type) {
        super(type);
    }

    @Override
    public Object decode(Object value) {
        byte[] bytes;
        if (null == value) {
            return null;
        } else if (value instanceof String) {
            bytes = bytesStr2Bytes((String) value);
        } else if (value instanceof byte[]) {
            bytes = (byte[]) value;
        } else {
            bytes = object2Bytes(value);
        }
        return new TapBinaryValue(bytes).tapType(new TapBinary());
    }

}
