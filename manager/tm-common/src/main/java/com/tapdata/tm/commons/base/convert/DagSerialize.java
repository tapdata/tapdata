package com.tapdata.tm.commons.base.convert;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.tapdata.tm.commons.dag.DAG;

import java.io.IOException;

/**
 * @Author: Zed
 * @Date: 2021/11/6
 * @Description:
 */
public class DagSerialize extends JsonSerializer<DAG> {
	@Override
	public void serialize(DAG value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
		if (value != null) {
			gen.writeObject(value.toDag());
		}
	}
}
