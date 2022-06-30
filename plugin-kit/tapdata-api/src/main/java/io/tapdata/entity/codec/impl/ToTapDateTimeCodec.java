package io.tapdata.entity.codec.impl;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.codec.TapDefaultCodecs;
import io.tapdata.entity.codec.ToTapValueCodec;
import io.tapdata.entity.codec.impl.utils.AnyTimeToDateTime;
import io.tapdata.entity.schema.type.TapDateTime;
import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.schema.value.TapDateTimeValue;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Date;

@Implementation(value = ToTapValueCodec.class, type = TapDefaultCodecs.TAP_DATE_TIME_VALUE, buildNumber = 0)
public class ToTapDateTimeCodec implements ToTapValueCodec<TapDateTimeValue> {
	@Override
	public TapDateTimeValue toTapValue(Object value, TapType typeFromSchema) {
		if(value instanceof DateTime) {
			return new TapDateTimeValue((DateTime) value);
		}
		if (typeFromSchema instanceof TapDateTime) {
			TapDateTime tapDateTime = (TapDateTime) typeFromSchema;
			DateTime dateTime = AnyTimeToDateTime.toDateTime(value, tapDateTime.getFraction());
			if (dateTime != null)
				return new TapDateTimeValue(dateTime);
		} else {
			DateTime dateTime = AnyTimeToDateTime.toDateTime(value);
			if (dateTime != null)
				return new TapDateTimeValue(dateTime);
		}
		return null;
	}
}
