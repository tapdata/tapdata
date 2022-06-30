package com.tapdata.tm.commons.base.convert;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;
import java.util.Date;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/8/15 2:03 下午
 * @description
 */
public class DatetimeSerialize extends JsonSerializer<Date> {
	@Override
	public void serialize(Date value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
		if (value != null)
			gen.writeNumber(value.getTime());
	}
}
