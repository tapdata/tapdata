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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


/**
 * @Author: Zed
 * @Date: 2021/11/6
 * @Description:
 */
public class NodeDeserialize extends JsonDeserializer<List<Node>> {
	private static final Logger logger = LoggerFactory.getLogger(NodeDeserialize.class);
	/** Unknown types already reported at WARN; task lists are read repeatedly, so each type is reported once. */
	private static final Set<String> WARNED_UNKNOWN_TYPES = ConcurrentHashMap.newKeySet();
	private static final int MAX_WARNED_UNKNOWN_TYPES = 1000;

	@Override
	public List<Node> deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
		List<Map<String, Object>> lists = p.getCodec().readValue(p, new TypeReference<List<Map<String, Object>>>() {
		});
		List<Node> nodes = new ArrayList<>();
		for (Map<String, Object> map : lists) {
			if (map != null && StringUtils.isNotBlank((CharSequence) map.get("type"))) {
				String type = (String) map.get("type");
				Class<? extends Node> clazz = DAG.getClassByType(type);
				if (clazz != null) {
					nodes.add(JsonUtil.parseJsonUseJackson(JsonUtil.toJson(map), clazz));
				} else if (DAG.isNodeTypeRegistrationFailed(type)) {
					// An environment problem, not data from another version: do not hide it behind UnknownNode.
					throw new IllegalStateException(String.format("DAG node type '%s' (node id: %s) is not available because DAG node "
							+ "type registration failed, see the TM startup log for 'Unable to load class' or 'DAG node type scan failed'",
							type, map.get("id")));
				} else {
					nodes.add(toUnknownNode(map, type));
				}
			}
		}
		return nodes;
	}

	/**
	 * Unregistered types (e.g. left by a development build) keep their data instead of failing the whole DAG.
	 * Never returns null: if some property cannot be bound (e.g. an enum constant added by a newer build), the
	 * raw properties are kept without binding, so the node is not dropped when the task is written back.
	 */
	private static UnknownNode toUnknownNode(Map<String, Object> map, String type) {
		Object nodeId = map.get("id");
		if (WARNED_UNKNOWN_TYPES.size() < MAX_WARNED_UNKNOWN_TYPES && WARNED_UNKNOWN_TYPES.add(type)) {
			logger.warn("Unknown DAG node type '{}' (node id: {}), keep its original data as UnknownNode; tasks containing it "
					+ "cannot be started or scheduled. Further nodes of this type are logged at debug level", type, nodeId);
		} else {
			logger.debug("Unknown DAG node type '{}' (node id: {}), keep its original data as UnknownNode", type, nodeId);
		}
		try {
			UnknownNode node = JsonUtil.map2PojoUseJackson(map, UnknownNode.class);
			if (node != null) {
				return node;
			}
		} catch (IllegalArgumentException e) {
			logger.debug("Node {} of unknown type '{}' cannot be bound, keep its raw properties: {}", nodeId, type, e.getMessage());
		}
		return UnknownNode.fromRawProperties(map);
	}
}