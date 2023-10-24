package io.tapdata.entity.codec.impl;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.codec.FromTapValueCodec;
import io.tapdata.entity.codec.TapDefaultCodecs;
import io.tapdata.entity.schema.value.TapArrayValue;

@Implementation(value = FromTapValueCodec.class, type = TapDefaultCodecs.TAP_ARRAY_VALUE, buildNumber = 0)
public class FromTapArrayCodec implements FromTapValueCodec<TapArrayValue> {
    @Override
    public Object fromTapValue(TapArrayValue tapValue) {
        if(tapValue == null)
            return null;
        //TODO need more code
        return tapValue.getValue();
    }
}
