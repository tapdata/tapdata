package io.tapdata.entity.codec.impl;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.codec.TapDefaultCodecs;
import io.tapdata.entity.codec.ToTapValueCodec;
import io.tapdata.entity.codec.impl.utils.AnyTimeToDateTime;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.schema.value.TapBinaryValue;
import io.tapdata.entity.schema.value.TapTimeValue;
import io.tapdata.entity.schema.value.TapYearValue;

@Implementation(value = ToTapValueCodec.class, type = TapDefaultCodecs.TAP_YEAR_VALUE, buildNumber = 0)
public class ToTapYearCodec implements ToTapValueCodec<TapYearValue> {
    @Override
    public TapYearValue toTapValue(Object value, TapType typeFromSchema) {
        DateTime dateTime = AnyTimeToDateTime.toDateTime(value);
        if(dateTime != null) {
            return new TapYearValue(dateTime);
        }
        return null;
    }
}
