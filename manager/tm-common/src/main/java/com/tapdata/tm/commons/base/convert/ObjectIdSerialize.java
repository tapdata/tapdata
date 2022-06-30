package com.tapdata.tm.commons.base.convert;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.bson.types.ObjectId;

import java.io.IOException;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/8/15 2:03 下午
 * @description
 */
public class ObjectIdSerialize extends JsonSerializer<ObjectId> {
	@Override
	public void serialize(ObjectId value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
		if (value != null)
			gen.writeString(value.toHexString());
	}
}
