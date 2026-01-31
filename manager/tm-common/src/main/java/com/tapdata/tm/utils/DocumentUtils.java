package com.tapdata.tm.utils;

import org.bson.Document;

/**
 * @author samuel
 * @Description
 * @create 2024-08-29 12:31
 **/
public class DocumentUtils {

	private DocumentUtils() {
	}

	public static Long getLong(Document document, String key) {
		if (null == document || null == key) {
			return 0L;
		}
		Object value = document.get(key);
		if (null == value) {
			return 0L;
		}
		if (value instanceof Long) {
			return (Long) value;
		} else if (value instanceof Integer) {
			return Long.parseLong(value.toString());
		}  else if (value instanceof Double iDouble) {
			return iDouble.longValue();
		} else {
			return 0L;
		}
	}
}
