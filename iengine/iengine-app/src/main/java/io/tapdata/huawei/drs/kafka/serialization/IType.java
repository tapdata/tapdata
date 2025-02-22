package io.tapdata.huawei.drs.kafka.serialization;

import java.util.Map;

/**
 * 字段类型序列化
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/21 17:22 Create
 */
public interface IType {
    String type();

    Object decode(Object value);

    Object encode(Object value);

    default void append2(Map<String, IType> map) {
        String type = type();
        if (map.containsKey(type))
            throw new RuntimeException("duplicate type '" + type + "'");
        map.put(type, this);
    }
}
