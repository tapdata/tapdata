package io.tapdata.entity.codec.impl;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.codec.FromTapValueCodec;
import io.tapdata.entity.codec.TapDefaultCodecs;
import io.tapdata.entity.schema.value.TapTimeValue;

@Implementation(value = FromTapValueCodec.class, type = TapDefaultCodecs.TAP_TIME_VALUE, buildNumber = 0)
public class FromTapTimeCodec implements FromTapValueCodec<TapTimeValue> {
    @Override
    public Object fromTapValue(TapTimeValue tapValue) {
        if (tapValue == null)
            return null;
        return tapValue.getValue();
    }
}
