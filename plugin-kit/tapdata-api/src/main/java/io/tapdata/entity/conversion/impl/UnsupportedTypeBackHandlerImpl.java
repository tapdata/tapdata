package io.tapdata.entity.conversion.impl;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.conversion.UnsupportedTypeFallbackHandler;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.type.TapType;

@Implementation(value = UnsupportedTypeFallbackHandler.class, buildNumber = 0)
public class UnsupportedTypeBackHandlerImpl implements UnsupportedTypeFallbackHandler {
    @Override
    public void handle(TapCodecsRegistry codecsRegistry, TapField unsupportedField, String dataType, TapType toTapType) {
        if(codecsRegistry != null) {
            TapType unsupportedTapType = unsupportedField.getTapType();
            if(unsupportedTapType != null && !codecsRegistry.isRegisteredFromTapValue(unsupportedTapType.tapValueClass())) {
                codecsRegistry.registerFromTapValue(unsupportedTapType.tapValueClass(), dataType, tapValue -> {
                    Object value = tapValue.getValue();
                    return value.toString();
                });
            }

        }
    }
}
