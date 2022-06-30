package com.tapdata.tm.commons.base.convert;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import org.bson.types.ObjectId;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/8/15 2:06 下午
 * @description
 */
public class ObjectIdDeserialize extends JsonDeserializer<ObjectId> {
	private static final Pattern pattern = Pattern.compile("^[0-9a-fA-F]{24}$");
	public static ObjectId toObjectId(String id) {
		if (id == null) {
			return null;
		}
		Matcher matcher = pattern.matcher(id);
		if(matcher.matches()) {
			return new ObjectId(id);
		} else
			return null;
	}
	@Override
	public ObjectId deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
		String value = p.getValueAsString();
		if (value != null)
			return toObjectId(value);
		return null;
	}
}
