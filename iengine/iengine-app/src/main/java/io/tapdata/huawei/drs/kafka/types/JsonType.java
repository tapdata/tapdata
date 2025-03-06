package io.tapdata.huawei.drs.kafka.types;

import com.alibaba.fastjson.JSON;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/22 21:42 Create
 */
public class JsonType extends StringType {
    public JsonType() {
        super("json");
    }

    public JsonType(String type) {
        super(type);
    }

    @Override
    public Object decode(Object value) {
        return decodeString(value, JSON::toJSONString);
    }
}
