package io.tapdata.common.constant;

import java.util.Collection;
import java.util.Map;

public enum JsonType {
    STRING,
    NUMBER,
    BOOLEAN,
    NULL,
    OBJECT,
    ARRAY,
    ;

    public static JsonType of(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof Map) {
            return JsonType.OBJECT;
        }
        if ((obj instanceof Collection) || obj.getClass().isArray()) {
            return JsonType.ARRAY;
        }
        if (obj instanceof Number) {
            return JsonType.NUMBER;
        }
        if (obj instanceof Boolean) {
            return JsonType.BOOLEAN;
        }
        if (obj instanceof String) {
            return JsonType.STRING;
        }
        return null;
    }

}
