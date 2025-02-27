package io.tapdata.huawei.drs.kafka.types;

import io.tapdata.huawei.drs.kafka.IType;

import java.nio.ByteBuffer;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/23 14:33 Create
 */
public abstract class BasicType implements IType {
    private final String type;

    public BasicType(String type) {
        this.type = type;
    }

    @Override
    public String type() {
        return type;
    }

    public static byte[] object2Bytes(Object o) {
        return o.toString().getBytes();
    }

    public static byte[] bytesStringDecode(String bytesString) {
        if (bytesString.startsWith("[") && bytesString.endsWith("]")) {
            bytesString = bytesString.substring(1, bytesString.length() - 1);
            if (!bytesString.trim().isEmpty()) {
                String[] arr = bytesString.split(",");
                ByteBuffer buf = ByteBuffer.allocate(arr.length);
                for (String s : arr) {
                    buf.put(Byte.parseByte(s.trim()));
                }
                return buf.array();
            }
        }
        return new byte[0];
    }

    public static String bytesStringEncode(byte[] bytes) {
        StringBuilder builder = new StringBuilder(bytes.length * 2 + 1);
        builder.append("[");
        for (byte b : bytes) {
            builder.append(b);
            builder.append(",");
        }
        builder.setCharAt(builder.length() - 1, ']');
        return builder.toString();
    }
}
