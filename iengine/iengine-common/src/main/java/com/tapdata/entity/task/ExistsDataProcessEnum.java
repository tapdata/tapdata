package com.tapdata.entity.task;

import java.util.HashMap;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2022-05-17 17:19
 **/
public enum ExistsDataProcessEnum {
	KEEP_DATE("keepData"),
	REMOVE_DATE("removeData"),
	DROP_TABLE("dropTable"),
	;
	private String option;

	ExistsDataProcessEnum(String option) {
		this.option = option;
	}

	public String getOption() {
		return option;
	}

	private static Map<String, ExistsDataProcessEnum> optionMap;

	static {
		optionMap = new HashMap<>();
		for (ExistsDataProcessEnum value : ExistsDataProcessEnum.values()) {
			optionMap.put(value.getOption(), value);
		}
	}

	public static ExistsDataProcessEnum fromOption(String option) {
		return optionMap.get(option);
	}
}
