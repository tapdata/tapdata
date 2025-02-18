package io.tapdata.constant;

import java.lang.reflect.Field;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2025-02-07 15:01
 **/
public class AgentRuntime {
	public static String version;
	public static String codeVersion;
	public static ZoneId jvmZoneId;
	public static ZoneId osZoneId;

	public static Map<String, String> toMap() {
		Map<String, String> map = new HashMap<>();
		Field[] declaredFields = AgentRuntime.class.getDeclaredFields();
		for (Field declaredField : declaredFields) {
			declaredField.setAccessible(true);
			String fieldName = declaredField.getName();
			try {
				Object value = declaredField.get(AgentRuntime.class);
				if (value instanceof String) {
					map.put(fieldName, (String) value);
				} else {
					map.put(fieldName, value.toString());
				}
			} catch (IllegalAccessException e) {
				map.put(fieldName, e.getMessage());
			}
		}
		return map;
	}
}
