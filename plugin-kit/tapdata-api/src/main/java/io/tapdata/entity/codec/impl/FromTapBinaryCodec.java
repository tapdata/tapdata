package io.tapdata.entity.codec.impl;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.codec.FromTapValueCodec;
import io.tapdata.entity.codec.TapDefaultCodecs;
import io.tapdata.entity.schema.value.TapBinaryValue;

@Implementation(value = FromTapValueCodec.class, type = TapDefaultCodecs.TAP_BINARY_VALUE, buildNumber = 0)
public class FromTapBinaryCodec implements FromTapValueCodec<TapBinaryValue> {
    @Override
    public Object fromTapValue(TapBinaryValue tapValue) {
        if(tapValue == null)
            return null;
        //TODO need more code
        return tapValue.getValue();
    }
}
