package io.tapdata.entity.codec.impl;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.codec.FromTapValueCodec;
import io.tapdata.entity.codec.TapDefaultCodecs;
import io.tapdata.entity.schema.value.TapBinaryValue;
import io.tapdata.entity.schema.value.TapYearValue;

@Implementation(value = FromTapValueCodec.class, type = TapDefaultCodecs.TAP_YEAR_VALUE, buildNumber = 0)
public class FromTapYearCodec implements FromTapValueCodec<TapYearValue> {
    @Override
    public Object fromTapValue(TapYearValue tapValue) {
        if(tapValue == null)
            return null;
        //TODO need more code
        return tapValue.getValue();
    }
}
