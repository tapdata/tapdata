package io.tapdata.entity.codec.impl;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.codec.TapDefaultCodecs;
import io.tapdata.entity.codec.ToTapValueCodec;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.schema.value.TapBooleanValue;

@Implementation(value = ToTapValueCodec.class, type = TapDefaultCodecs.TAP_BOOLEAN_VALUE, buildNumber = 0)
public class ToTapBooleanCodec implements ToTapValueCodec<TapBooleanValue> {
    @Override
    public TapBooleanValue toTapValue(Object value, TapType typeFromSchema) {
        TapBooleanValue booleanValue = null;
        if(value instanceof Number) {
            booleanValue = new TapBooleanValue(((Number) value).intValue() != 0);
        } else if(value instanceof Boolean) {
            booleanValue = new TapBooleanValue((Boolean) value);
        } else if(value instanceof String) {
            String theStr = (String) value;
            if(theStr.equalsIgnoreCase("false") || theStr.equalsIgnoreCase("n") || theStr.equalsIgnoreCase("no") || theStr.equalsIgnoreCase("0")) {
                booleanValue = new TapBooleanValue(false);
            } else {
                booleanValue = new TapBooleanValue(true);
            }
        }

        return booleanValue;
    }
}
