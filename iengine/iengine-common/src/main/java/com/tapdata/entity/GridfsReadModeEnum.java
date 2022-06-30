package com.tapdata.entity;

import java.util.HashMap;
import java.util.Map;

/**
 * Gridfs read mode enum
 *
 * @author samuel
 */
public enum GridfsReadModeEnum {
	DATA("data"),
	BINARY("binary"),
	;

	private String mode;
	private final static Map<String, GridfsReadModeEnum> modeMap = new HashMap<>();

	static {
		for (GridfsReadModeEnum modeEnum : GridfsReadModeEnum.values()) {
			modeMap.put(modeEnum.getMode(), modeEnum);
		}
	}

	public static GridfsReadModeEnum fromMode(String mode) {
		return modeMap.get(mode);
	}

	GridfsReadModeEnum(String mode) {
		this.mode = mode;
	}

	public String getMode() {
		return mode;
	}
}
