package io.tapdata.huawei.drs.kafka.serialization.mysql.types;

import io.tapdata.entity.schema.type.TapBinary;
import io.tapdata.entity.schema.value.TapBinaryValue;
import io.tapdata.huawei.drs.kafka.serialization.IType;

import java.nio.ByteBuffer;

/**
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/21 18:03 Create
 */
public class BinaryMysqlType implements IType {
    @Override
    public String type() {
        return "binary";
    }

    @Override
    public Object decode(Object value) {
        if (value instanceof String) {
            return new TapBinaryValue(toByteArray((String) value)).tapType(new TapBinary());
        }
        return value;
    }

    @Override
    public Object encode(Object value) {
        return value;
    }

    public static byte[] toByteArray(String value) {
        if (value.startsWith("[") && value.endsWith("]")) {
            value = value.substring(1, value.length() - 1);
            if (!value.trim().isEmpty()) {
                String[] arr = value.split(",");
                ByteBuffer buf = ByteBuffer.allocate(arr.length);
                for (String s : arr) {
                    buf.put(Byte.parseByte(s.trim()));
                }
                return buf.array();
            }
        }
        return new byte[0];
    }
}
