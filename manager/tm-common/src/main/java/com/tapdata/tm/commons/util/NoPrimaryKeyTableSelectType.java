package com.tapdata.tm.commons.util;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/6/15 17:15 Create
 */
public enum NoPrimaryKeyTableSelectType {
	HasKeys, // 有主键
	NoKeys, // 无主键无索引
	All, // 全部
	OnlyPrimaryKey, // 仅包含主键
	OnlyUniqueIndex, // 仅包含唯一索引
	View,//logic view or materialized View
	;

	public boolean equals(String value) {
		return this == parse(value);
	}

	public static NoPrimaryKeyTableSelectType parse(String value) {
		if (null == value) {
			return All;
		}
		value = value.trim();
		for (NoPrimaryKeyTableSelectType t : values()) {
			if (t.name().equals(value)) {
				return t;
			}
		}
		return All;
	}
}
