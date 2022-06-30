package com.tapdata.tm.commons.base.convert;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;
import java.util.Date;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/8/15 2:06 下午
 * @description
 */
public class DatetimeDeserialize extends JsonDeserializer<Date> {
	@Override
	public Date deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
		Long value = p.getValueAsLong();
		return new Date(value);
	}
}
