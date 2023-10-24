package io.tapdata.entity.mapping.type;

import java.util.Map;

/**
 *  "varbinary($byte)": {"byte": 255, "fixed": false, "to": "typeBinary"}
 */
public abstract class TapSizeBase extends TapMapping {
    public static final String KEY_SIZE = "size";

    private Integer size;

    @Override
    public void from(Map<String, Object> info) {
        Object sizeObj = info.get(KEY_SIZE);
        if(sizeObj instanceof Number) {
            size = ((Number) sizeObj).intValue();
        }
    }
}
