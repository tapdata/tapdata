package com.tapdata.entity;

import java.util.HashMap;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2021-07-13 18:20
 **/
public enum FlowEngineVersion {
	V1("Data_Flow_Engine_V1", ""),
	V2_JET("Jet_Flow_Engine_V2", ""),
	;

	private String version;
	private String describe;

	FlowEngineVersion(String version, String describe) {
		this.version = version;
		this.describe = describe;
	}

	public String getVersion() {
		return version;
	}

	public String getDescribe() {
		return describe;
	}

	private static Map<String, FlowEngineVersion> map = new HashMap<>();

	static {
		for (FlowEngineVersion value : FlowEngineVersion.values()) {
			map.put(value.getVersion(), value);
		}
	}

	public static FlowEngineVersion fromVersion(String version) {
		return map.get(version);
	}

	@Override
	public String toString() {
		return "FlowEngineVersion{" +
				"version='" + version + '\'' +
				", describe='" + describe + '\'' +
				'}';
	}
}
