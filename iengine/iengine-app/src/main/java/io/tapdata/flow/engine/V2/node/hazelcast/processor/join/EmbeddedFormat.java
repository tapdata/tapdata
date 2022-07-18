package io.tapdata.flow.engine.V2.node.hazelcast.processor.join;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public enum EmbeddedFormat {
	OBJECT("object"),
	ARRAY("array"),
	;

	private String format;

	EmbeddedFormat(String format) {
		this.format = format;
	}

	private static final Map<String, EmbeddedFormat> ENUM_MAP;

	static {
		Map<String, EmbeddedFormat> map = new ConcurrentHashMap<>();
		for (EmbeddedFormat instance : EmbeddedFormat.values()) {
			map.put(instance.format.toLowerCase(), instance);
		}
		ENUM_MAP = Collections.unmodifiableMap(map);
	}

	public static EmbeddedFormat get(String name) {
		return ENUM_MAP.get(name.toLowerCase());
	}
}
