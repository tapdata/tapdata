package com.tapdata.tm.sdk.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * @author samuel
 * @Description
 * @create 2022-10-19 14:37
 **/
public class JacksonUtil {
	private static ObjectMapper objectMapper = new ObjectMapper();

	static {
		objectMapper.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);
		objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	}

	public static <E> E convertValue(Object fromValue, TypeReference<E> typeReference) {
		return objectMapper.convertValue(fromValue, typeReference);
	}

	public static String toJson(Object value) throws JsonProcessingException {
		return objectMapper.writeValueAsString(value);
	}

	public static <E> E fromJson(String json, TypeReference<E> typeReference) throws IOException {
		return objectMapper.readValue(json, typeReference);
	}
}
