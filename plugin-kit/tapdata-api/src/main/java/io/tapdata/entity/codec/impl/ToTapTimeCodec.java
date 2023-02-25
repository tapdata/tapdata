package io.tapdata.entity.codec.impl;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.codec.TapDefaultCodecs;
import io.tapdata.entity.codec.ToTapValueCodec;
import io.tapdata.entity.codec.impl.utils.AnyTimeToDateTime;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.schema.value.TapTimeValue;

import java.sql.Time;
import java.util.Date;

@Implementation(value = ToTapValueCodec.class, type = TapDefaultCodecs.TAP_TIME_VALUE, buildNumber = 0)
public class ToTapTimeCodec implements ToTapValueCodec<TapTimeValue> {
    @Override
    public TapTimeValue toTapValue(Object value, TapType typeFromSchema) {
        if(value instanceof Date) {
            value = ((Date) value).getTime();
        }

        if(value instanceof DateTime) {
            return new TapTimeValue((DateTime) value);
        } else if (value instanceof Long) {
            long val = (Long) value;
            DateTime dateTime = new DateTime();
            dateTime.setNano((int) ((val % 1000000) * 1000));
            dateTime.setSeconds(val = val/1000000);
            TapTimeValue dateTimeValue = new TapTimeValue(dateTime);

            boolean negative = val < 0;
            if (negative) val = -val;
            String timeStr = String.format("%02d", val % 60);
            timeStr = String.format("%02d", (val / 60) % 60) + ":" + timeStr;
            timeStr = (negative ? "-" : "") + String.format("%02d", val / 60) + ":" + timeStr;
            //TODO Don't understand this, Long should be the originValue, is it necessary to use time string?
            dateTimeValue.setOriginValue(timeStr);
            return dateTimeValue;
        } else if (value instanceof String) {
            DateTime dateTime = AnyTimeToDateTime.withTimeStr((String)value);
            TapTimeValue dateTimeValue = new TapTimeValue(dateTime);
            dateTimeValue.setOriginValue((String)value);
            return dateTimeValue;
        } else {
            DateTime dateTime = AnyTimeToDateTime.toDateTime(value);
            if (dateTime != null)
                return new TapTimeValue(dateTime);
        } /*else {
            throw new IllegalArgumentException("DateTime constructor time not support with " + value.getClass().getName());
        }*/
        return null;
    }
}
