package com.tapdata.tm.base.convert;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.UnknownNode;

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
			String json = JsonUtil.toJson(value);
			List<Map<String, Object>> objectMap = JsonUtil.parseJsonUseJackson(json, new TypeReference<List<Map<String, Object>>>() {
			});
			flattenUnknownNodes(value, objectMap);
			gen.writeObject(objectMap);
		}
	}

	/**
	 * Gson writes the properties kept by {@link UnknownNode} as one nested object; put them back at the
	 * node level so the persisted node keeps its original shape.
	 */
	private static void flattenUnknownNodes(List<Node> nodes, List<Map<String, Object>> objectMap) {
		if (objectMap == null) {
			return;
		}
		for (int i = 0; i < nodes.size() && i < objectMap.size(); i++) {
			Map<String, Object> nodeMap = objectMap.get(i);
			if (nodes.get(i) instanceof UnknownNode && nodeMap != null) {
				nodeMap.remove(UnknownNode.PRESERVED_PROPERTIES_FIELD);
				((UnknownNode) nodes.get(i)).getUnknownProperties().forEach(nodeMap::putIfAbsent);
			}
		}
	}
}
