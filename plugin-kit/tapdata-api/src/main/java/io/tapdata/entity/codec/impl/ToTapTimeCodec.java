package io.tapdata.entity.codec.impl;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.codec.TapDefaultCodecs;
import io.tapdata.entity.codec.ToTapValueCodec;
import io.tapdata.entity.codec.impl.utils.AnyTimeToDateTime;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.entity.schema.value.TapTimeValue;

import java.util.Date;

@Implementation(value = ToTapValueCodec.class, type = TapDefaultCodecs.TAP_TIME_VALUE, buildNumber = 0)
public class ToTapTimeCodec implements ToTapValueCodec<TapTimeValue> {
    @Override
    public TapTimeValue toTapValue(Object value, TapType typeFromSchema) {
        if(value instanceof DateTime) {
            return new TapTimeValue((DateTime) value);
        }
        DateTime dateTime = AnyTimeToDateTime.toDateTime(value);
        if(dateTime != null) {
            TapTimeValue dateTimeValue = new TapTimeValue(dateTime);
            return dateTimeValue;
        }
        return null;
    }
}
