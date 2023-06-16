package com.tapdata.entity.dataflow;

import cn.hutool.core.util.StrUtil;
import com.tapdata.constant.MapUtil;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2021-10-12 17:49
 **/
public enum Capitalized {
	UPPER("toUpperCase"), // 转大写
	LOWER("toLowerCase"), // 转小写
	SNAKE("toSnakeCase"), // 转蛇形
	CAMEL("toCamelCase"), // 转驼峰
	NONE(""), // 不做转换
	NOOPERATION("noOperation"),
	;

	private String value;

	Capitalized(String value) {
		this.value = value;
	}

	public String getValue() {
		return value;
	}

	private static Map<String, Capitalized> valueMap;

	static {
		valueMap = new HashMap<>();
		for (Capitalized capitalized : Capitalized.values()) {
			valueMap.put(capitalized.getValue(), capitalized);
		}
	}

	public static Capitalized fromValue(String value) {
		return valueMap.get(value);
	}

	public static String convert(String objectName, String capitalized) {
		if (StringUtils.isBlank(objectName) || capitalized == null) {
			return objectName;
		}
		Capitalized fromValue;
		try {
			fromValue = Capitalized.fromValue(capitalized);
		} catch (Exception e) {
			return objectName;
		}
		if (fromValue == null) {
			throw new IllegalArgumentException("capitalized is invalid: " + capitalized);
		}
		switch (Capitalized.fromValue(capitalized)) {
			case UPPER:
				return objectName.toUpperCase();
			case LOWER:
				return objectName.toLowerCase();
			case SNAKE:
				return StrUtil.toUnderlineCase(objectName);
			case CAMEL:
				return StrUtil.toCamelCase(objectName);
			default:
				return objectName;
		}
	}

	public static Map<String, Object> convert(Map<String, Object> map, String capitalized) {
		if (MapUtils.isEmpty(map) || capitalized == null) {
			return map;
		}
		Capitalized fromValue;
		try {
			fromValue = Capitalized.fromValue(capitalized);
		} catch (Exception e) {
			return map;
		}
		if (fromValue == null) {
			throw new IllegalArgumentException("capitalized is invalid: " + capitalized);
		}
		switch (Capitalized.fromValue(capitalized)) {
			case UPPER:
				return MapUtil.keyToUpperCase(map);
			case LOWER:
				return MapUtil.keyToLowerCase(map);
			default:
				return map;
		}
	}
}
