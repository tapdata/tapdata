package io.tapdata.flow.engine.V2.node.hazelcast.processor.join;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public enum JoinType {
	LEFT("left"),
	RIGHT("right"),
	INNER("inner"),
	FULL("full"),
	;

	private String type;

	JoinType(String type) {
		this.type = type;
	}

	public String getType() {
		return type;
	}

	private static final Map<String, JoinType> ENUM_MAP;

	static {
		Map<String, JoinType> map = new ConcurrentHashMap<>();
		for (JoinType instance : JoinType.values()) {
			map.put(instance.type.toLowerCase(), instance);
		}
		ENUM_MAP = Collections.unmodifiableMap(map);
	}

	public static JoinType get(String name) {
		return ENUM_MAP.get(name.toLowerCase());
	}
}
