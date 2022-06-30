package io.tapdata.entity.conversion;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.type.TapType;

public interface UnsupportedTypeFallbackHandler {
    void handle(TapCodecsRegistry codecsRegistry, TapField unsupportedField, String dataType, TapType toTapType);
}
