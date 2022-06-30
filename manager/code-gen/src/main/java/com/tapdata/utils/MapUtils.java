package com.tapdata.utils;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/10/12 3:40 下午
 * @description
 */
public class MapUtils {

	private static ObjectMapper objectMapper = new ObjectMapper();

	public static void main(String[] args) throws IOException {
		HashMap<String, Object> objectObjectHashMap = new HashMap<>();
		String jsonschema = "";
		HashMap hashMap = objectMapper.readValue(jsonschema, HashMap.class);

	}

	public static String getAsString(Map<String, Object> data, String fieldName) {
		if (data != null && data.containsKey(fieldName)) {
			Object value = data.get(fieldName);
			if (value != null) {
				return (String) value;
			}
		}
		return null;
	}

	public static List getAsList(Map<String, Object> data, String fieldName) {
		if (data != null && data.containsKey(fieldName)) {
			Object value = data.get(fieldName);
			if (value != null) {
				return (List) value;
			}
		}
		return null;
	}

	public static Map getAsMap(Map<String, Object> data, String fieldName) {
		if (data != null && data.containsKey(fieldName)) {
			Object value = data.get(fieldName);
			if (value != null) {
				return (Map) value;
			}
		}
		return null;
	}

	public static Long getAsLong(Map<String, Object> data, String fieldName) {
		if (data != null && data.containsKey(fieldName)) {
			Object value = data.get(fieldName);
			if (value instanceof Integer) {
				return Long.valueOf((Integer) value);
			} else if (value instanceof Double) {
				return ((Double) value).longValue();
			} else if (value instanceof Long) {
				return (Long) value;
			}
		}
		return null;
	}

	public static Boolean getAsBoolean(Map<String, Object> data, String fieldName) {
		if (data != null && data.containsKey(fieldName)) {
			Object value = data.get(fieldName);
			if (value != null) {
				return (Boolean) value;
			}
		}
		return null;
	}

	public static Object getValueByPatchPath(Object map, String key) {
		Object value = map;
		String[] split = key.split("/");
		for (String keyStr : split) {
			if (!hasLength(keyStr)){
				continue;
			}
			if (value instanceof Map){
				value = ((Map)value).get(keyStr);
			}else {
				return null;
			}
		}
		return value;

	}

	private static boolean hasLength(String str) {
		return str != null && !str.isEmpty();
	}

}
