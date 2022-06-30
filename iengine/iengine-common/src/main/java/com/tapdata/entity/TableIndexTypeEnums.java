package com.tapdata.entity;

/**
 * 索引类型
 * <pre>
 * Author: <a href="mailto:linhs@thoughtup.cn">Harsen</a>
 * CreateTime: 2021/4/1 下午5:15
 * </pre>
 */
public enum TableIndexTypeEnums {
	BTREE, TEXT, HASH, RTREE, OTHER,
	;

	public static TableIndexTypeEnums parse(String val) {
		if (null != val) {
			val = val.toUpperCase();
			switch (val) {
				case "BTREE":
					return BTREE;
				case "TEXT":
					return TEXT;
				case "HASH":
					return HASH;
				case "RTREE":
					return RTREE;
				case "OTHER":
					return OTHER;
				default:
					break;
			}
		}
		return null;
	}
}
