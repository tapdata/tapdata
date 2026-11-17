package com.tapdata.tm.base.convert;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.UnknownNode;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * @Author: Zed
 * @Date: 2021/11/6
 * @Description:
 */
public class NodeDeserialize extends JsonDeserializer<List<Node>> {
	private static final Logger logger = LoggerFactory.getLogger(NodeDeserialize.class);

	@Override
	public List<Node> deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
		List<Map<String, Object>> lists = p.getCodec().readValue(p, new TypeReference<List<Map<String, Object>>>() {
		});
		List<Node> nodes = new ArrayList<>();
		for (Map<String, Object> map : lists) {
			if (map != null && StringUtils.isNotBlank((CharSequence) map.get("type"))) {
				String type = (String) map.get("type");
				Class<? extends Node> clazz = DAG.getClassByType(type);
				if (clazz == null) {
					// Unregistered types (e.g. left by a development build) keep their data instead of failing the whole DAG.
					logger.warn("Unknown DAG node type '{}' (node id: {}), keep its original data as UnknownNode", type, map.get("id"));
					clazz = UnknownNode.class;
				}
				String json = JsonUtil.toJson(map);
				nodes.add(JsonUtil.parseJsonUseJackson(json, clazz));
			}
		}
		return nodes;
	}
}