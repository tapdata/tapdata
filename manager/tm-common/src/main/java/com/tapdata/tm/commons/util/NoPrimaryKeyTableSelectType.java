package com.tapdata.tm.commons.util;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/6/15 17:15 Create
 */
public enum NoPrimaryKeyTableSelectType {
	HasKeys, // 有主键
	NoKeys, // 无主键
	All, // 全部
	;

	public boolean equals(String value) {
		return this == parse(value);
	}

	public static NoPrimaryKeyTableSelectType parse(String value) {
		if (HasKeys.name().equals(value)) {
			return HasKeys;
		} else if (NoKeys.name().equals(value)) {
			return NoKeys;
		}
		return All;
	}
}
