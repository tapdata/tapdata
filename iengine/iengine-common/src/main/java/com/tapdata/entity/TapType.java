package com.tapdata.entity;

import java.util.ArrayList;
import java.util.List;

/**
 * @author samuel
 * @Description
 * @create 2021-08-04 11:25
 **/
public enum TapType {
	String,
	Number,
	Bytes,
	Boolean,
	Date,
	Datetime,
	Datetime_with_timezone,
	Time,
	Time_with_timezone,
	Array,
	Map,
	Null,
	Unsupported,
	;

	private static List<String> names = new ArrayList<>();

	static {
		for (TapType value : TapType.values()) {
			names.add(value.name());
		}
	}

	public static List<java.lang.String> getNames() {
		return names;
	}
}
