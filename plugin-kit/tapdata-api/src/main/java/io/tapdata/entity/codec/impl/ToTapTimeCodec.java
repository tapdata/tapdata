package io.tapdata.entity.codec.impl;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.codec.TapDefaultCodecs;
import io.tapdata.entity.codec.ToTapValueCodec;
import io.tapdata.entity.codec.impl.utils.AnyTimeToDateTime;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.schema.value.TapTimeValue;

import java.util.Date;

@Implementation(value = ToTapValueCodec.class, type = TapDefaultCodecs.TAP_TIME_VALUE, buildNumber = 0)
public class ToTapTimeCodec implements ToTapValueCodec<TapTimeValue> {
    @Override
    public TapTimeValue toTapValue(Object value, TapType typeFromSchema) {
        if (value instanceof Date) {
            value = ((Date) value).getTime() * 1000;
        }

        if (value instanceof DateTime) {
            return new TapTimeValue((DateTime) value);
        } else if (value instanceof Long) {
            long val = (Long) value;
            DateTime dateTime = new DateTime();
            dateTime.setNano((int) ((val % 1000000) * 1000));
            dateTime.setSeconds(val / 1000000);
            return new TapTimeValue(dateTime);
        } else if (value instanceof String) {
            DateTime dateTime = AnyTimeToDateTime.withTimeStr((String) value);
            return new TapTimeValue(dateTime);
        } else {
            DateTime dateTime = AnyTimeToDateTime.toDateTime(value);
            if (dateTime != null)
                return new TapTimeValue(dateTime);
        }
        return null;
    }
}
