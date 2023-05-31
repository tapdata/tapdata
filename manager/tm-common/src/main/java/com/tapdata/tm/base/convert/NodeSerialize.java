package com.tapdata.tm.base.convert;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.commons.dag.Node;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @Author: Zed
 * @Date: 2021/11/6
 * @Description:
 */
public class NodeSerialize extends JsonSerializer<List<Node>> {
	@Override
	public void serialize(List<Node> value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
		if (value != null) {
			String json = JsonUtil.toJsonUseJackson(value);
			List<Map<String, Object>> objectMap = JsonUtil.parseJsonUseJackson(json, new TypeReference<List<Map<String, Object>>>() {
			});
			gen.writeObject(objectMap);
		}
	}
}
