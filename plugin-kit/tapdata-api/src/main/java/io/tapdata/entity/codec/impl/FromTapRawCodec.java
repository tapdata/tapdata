package io.tapdata.entity.codec.impl;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.codec.FromTapValueCodec;
import io.tapdata.entity.codec.TapDefaultCodecs;
import io.tapdata.entity.schema.value.TapRawValue;

@Implementation(value = FromTapValueCodec.class, type = TapDefaultCodecs.TAP_RAW_VALUE, buildNumber = 0)
public class FromTapRawCodec implements FromTapValueCodec<TapRawValue> {
    @Override
    public Object fromTapValue(TapRawValue tapValue) {
        if(tapValue == null)
            return null;
        //TODO need more code
        return tapValue.getValue();
    }
}
