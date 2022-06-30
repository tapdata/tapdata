package io.tapdata.entity.codec.impl;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.codec.TapDefaultCodecs;
import io.tapdata.entity.codec.ToTapValueCodec;
import io.tapdata.entity.codec.impl.utils.AnyTimeToDateTime;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.entity.schema.value.TapDateValue;

import java.util.Date;

@Implementation(value = ToTapValueCodec.class, type = TapDefaultCodecs.TAP_DATE_VALUE, buildNumber = 0)
public class ToTapDateCodec implements ToTapValueCodec<TapDateValue> {
    @Override
    public TapDateValue toTapValue(Object value, TapType typeFromSchema) {
        if(value instanceof DateTime) {
            return new TapDateValue((DateTime) value);
        }
        DateTime dateTime = AnyTimeToDateTime.toDateTime(value);
        TapDateValue tapDateValue = null;
        if(dateTime != null) {
            tapDateValue = new TapDateValue(dateTime);
        }

        return tapDateValue;
    }
}
