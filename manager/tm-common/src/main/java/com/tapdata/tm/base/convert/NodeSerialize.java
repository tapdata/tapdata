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
	 * node level so every original property is persisted with its original value. Kept values override what
	 * Gson wrote for the same key from a field (e.g. {@code isTransformed}), because they are the original data
	 * of the node. Fields of {@link com.tapdata.tm.commons.dag.Node} that the original node did not have are still
	 * written with their defaults (e.g. {@code elementType}, {@code disabled}), so the output is not byte-identical.
	 */
	private static void flattenUnknownNodes(List<Node> nodes, List<Map<String, Object>> objectMap) {
		if (objectMap == null) {
			return;
		}
		for (int i = 0; i < nodes.size() && i < objectMap.size(); i++) {
			Map<String, Object> nodeMap = objectMap.get(i);
			if (nodes.get(i) instanceof UnknownNode && nodeMap != null) {
				nodeMap.remove(UnknownNode.GSON_PRESERVED_PROPERTIES_KEY);
				nodeMap.putAll(((UnknownNode) nodes.get(i)).getUnknownProperties());
			}
		}
	}
}
