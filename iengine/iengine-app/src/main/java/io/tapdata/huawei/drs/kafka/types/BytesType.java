package io.tapdata.huawei.drs.kafka.types;

import io.tapdata.entity.schema.type.TapBinary;
import io.tapdata.entity.schema.value.TapBinaryValue;
import io.tapdata.entity.schema.value.TapValue;

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
        TapBinaryValue result = new TapBinaryValue();
        result.originValue(value).tapType(new TapBinary());

        if (null == value) {
            return result;
        } else if (value instanceof String) {
            result.value(bytesStringDecode((String) value));
        } else if (value instanceof byte[]) {
            result.value((byte[]) value);
        } else if (value instanceof TapValue) {
            return value;
        } else {
            result.value(object2Bytes(value));
        }
        return result;
    }

}
