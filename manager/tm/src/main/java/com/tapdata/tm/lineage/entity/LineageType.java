package com.tapdata.tm.lineage.entity;

import java.util.HashMap;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2023-05-19 12:12
 **/
public enum LineageType {
	UPSTREAM("upstream"),
	DOWNSTREAM("downstream"),
	ALL_STREAM("allstream"),
	;
	private final String type;

	LineageType(String type) {
		this.type = type;
	}

	public String getType() {
		return type;
	}

	private static final Map<String, LineageType> TYPE_MAP = new HashMap<>();
	static {
		for (LineageType lineageType : LineageType.values()) {
			TYPE_MAP.put(lineageType.getType(), lineageType);
		}
	}

	public static LineageType fromType(String type) {
		return TYPE_MAP.get(type);
	}
}
