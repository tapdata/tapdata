package com.tapdata.manager.common.utils;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.tapdata.entity.schema.type.TapType;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Type;

public class JsonUtil {
	private static Gson gson = null;
	private static boolean _pretty = false;
	private static ObjectMapper objectMapper;

	public JsonUtil() {
	}

	private static Gson buildGson() {
		if (gson == null) {
			GsonBuilder builder = new GsonBuilder();
			if (_pretty) {
				builder.setPrettyPrinting();
			}

			gson = builder.create();
		}

		return gson;
	}

	private static ObjectMapper buildObjectMapper() {
		if (objectMapper == null) {
			objectMapper = new ObjectMapper();
			objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
			objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
			if (_pretty) {
				objectMapper.setDefaultPrettyPrinter(new DefaultPrettyPrinter());
			}
			SimpleModule simpleModule = new SimpleModule();
			simpleModule.addDeserializer(TapType.class, new TapTypeDeserializer());
			objectMapper.registerModule(simpleModule);
			objectMapper.registerModule(new JavaTimeModule());
		}



		return objectMapper;
	}

	public static void setPretty(boolean pretty) {
		_pretty = pretty;
		buildGson();
	}

	public static <T> T parseJson(String json, Type type) {
		return json != null ? buildGson().fromJson(json, type) : null;
	}

	public static <T> T parseJson(String json, Class<T> clazz) {
		return json == null ? null : buildGson().fromJson(json, clazz);
	}

	public static String toJson(Object data) {
		return buildGson().toJson(data);
	}

	public static <T> T loadJsonFromClasspath(String resource, Type type) throws IOException {
		InputStream in = Utils.getSystemResourceAsStream(resource);
		return in == null ? null : buildGson().fromJson(new InputStreamReader(in), type);
	}

	public static <T> T parseJsonUseJackson(String json, TypeReference<T> typeReference) {
		if (json == null) {
			return null;
		} else {
			ObjectMapper objectMapper = buildObjectMapper();

			try {
				return objectMapper.readValue(json, typeReference);
			} catch (JsonProcessingException var4) {
				var4.printStackTrace();
				return null;
			}
		}
	}

	public static <T> T parseJsonUseJackson(String json, Class<T> clazz) {
		if (json == null) {
			return null;
		} else {
			ObjectMapper objectMapper = buildObjectMapper();

			try {
				return objectMapper.readValue(json, clazz);
			} catch (JsonProcessingException var4) {
				var4.printStackTrace();
				return null;
			}
		}
	}

	public static String toJsonUseJackson(Object object) {
		if (object == null) {
			return "";
		} else {
			ObjectMapper objectMapper = buildObjectMapper();

			try {
				return objectMapper.writeValueAsString(object);
			} catch (JsonProcessingException var3) {
				var3.printStackTrace();
				return null;
			}
		}
	}

	static class TapTypeDeserializer extends JsonDeserializer<TapType> {
		@Override
		public TapType deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
			ObjectCodec codec = p.getCodec();
			TreeNode treeNode = codec.readTree(p);
			TreeNode type = treeNode.get("type");
			int typeInt = (int) ((IntNode) type).numberValue();
			Class<? extends TapType> tapTypeClass = TapType.getTapTypeClass((byte) typeInt);
			if (null != tapTypeClass) {
				return codec.treeToValue(treeNode, tapTypeClass);
			} else {
				throw new RuntimeException("Unsupported tap type: " + typeInt);
			}
		}
	}
}
