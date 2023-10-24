package io.tapdata.entity.codec.impl;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.codec.FromTapValueCodec;
import io.tapdata.entity.codec.TapDefaultCodecs;
import io.tapdata.entity.schema.value.TapStringValue;

@Implementation(value = FromTapValueCodec.class, type = TapDefaultCodecs.TAP_STRING_VALUE, buildNumber = 0)
public class FromTapStringCodec implements FromTapValueCodec<TapStringValue> {
    @Override
    public Object fromTapValue(TapStringValue tapValue) {
        if(tapValue == null)
            return null;
        return tapValue.getValue();
    }
}
