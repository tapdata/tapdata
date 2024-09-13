package com.tapdata.tm.utils;

import org.springframework.data.mongodb.core.mapping.Document;

/**
 * @author samuel
 * @Description
 * @create 2024-08-29 12:34
 **/
public class EntityUtils {
	private EntityUtils() {
	}

	public static String documentAnnotationValue(Class<?> clazz) {
		if (null == clazz) {
			throw new IllegalArgumentException("Input class is null");
		}
		Document annotation = clazz.getAnnotation(Document.class);
		if (null == annotation) {
			throw new IllegalArgumentException("Class %s not have a org.springframework.data.mongodb.core.mapping.Document annotation");
		}
		return annotation.value();
	}
}
