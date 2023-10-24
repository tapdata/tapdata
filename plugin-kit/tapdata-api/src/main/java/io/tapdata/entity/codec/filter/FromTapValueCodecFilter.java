package io.tapdata.entity.codec.filter;

import io.tapdata.entity.schema.value.TapValue;

import java.util.Map;

public interface FromTapValueCodecFilter {
    Map<String, Object> filter(Map<String, TapValue<?, ?>> tapValueMap);
}
