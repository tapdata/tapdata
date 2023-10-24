package io.tapdata.entity.codec.impl;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.codec.FromTapValueCodec;
import io.tapdata.entity.codec.TapDefaultCodecs;
import io.tapdata.entity.schema.value.TapMapValue;

@Implementation(value = FromTapValueCodec.class, type = TapDefaultCodecs.TAP_MAP_VALUE, buildNumber = 0)
public class FromTapMapCodec implements FromTapValueCodec<TapMapValue> {
    @Override
    public Object fromTapValue(TapMapValue tapValue) {
        if(tapValue == null)
            return null;
        //TODO need more code
        return tapValue.getValue();
    }
}
