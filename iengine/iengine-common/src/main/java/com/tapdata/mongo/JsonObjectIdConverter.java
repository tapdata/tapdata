package com.tapdata.mongo;

import org.bson.json.Converter;
import org.bson.json.StrictJsonWriter;
import org.bson.types.ObjectId;

import static java.lang.String.format;

/**
 * @author samuel
 * @Description
 * @create 2020-11-04 18:05
 **/
public class JsonObjectIdConverter implements Converter<ObjectId> {
	@Override
	public void convert(final ObjectId value, StrictJsonWriter writer) {
		writer.writeRaw(format("ObjectId(\"%s\")", value.toHexString()));
	}
}
