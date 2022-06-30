package com.tapdata.entity;

import java.util.HashMap;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2020-09-03 16:39
 **/
public enum SyncStageEnum {
	SNAPSHOT("snapshot"),
	CDC("cdc"),
	UNKNOWN("unknown"),
	;

	private static Map<String, SyncStageEnum> map;

	static {
		map = new HashMap<>();
		for (SyncStageEnum value : SyncStageEnum.values()) {
			map.put(value.getSyncStage(), value);
		}
	}

	public static SyncStageEnum fromSyncStage(String syncStage) {
		return map.get(syncStage);
	}

	private String syncStage;

	SyncStageEnum(String syncStage) {
		this.syncStage = syncStage;
	}

	public String getSyncStage() {
		return syncStage;
	}
}
