package com.tapdata.tm.commons.util;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.DefaultJSONParser;
import com.alibaba.fastjson.parser.ParserConfig;
import com.alibaba.fastjson.parser.deserializer.ObjectDeserializer;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.tapdata.manager.common.utils.Utils;
import io.tapdata.entity.schema.partition.type.TapPartitionType;
import io.tapdata.entity.schema.type.TapType;
import lombok.extern.slf4j.Slf4j;
import ognl.Ognl;
import ognl.OgnlContext;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.Objects;

@Slf4j
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
			objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
			if (_pretty) {
				objectMapper.setDefaultPrettyPrinter(new DefaultPrettyPrinter());
			}
			SimpleModule simpleModule = new SimpleModule();
			simpleModule.addDeserializer(TapType.class, new TapTypeDeserializer());
			simpleModule.addDeserializer(TapPartitionType.class, new TapPartitionTypeDeserializer());
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

	public static <T> T map2PojoUseJackson(Map<String, Object> map, Class<T> className) {
		if (null == map || null == className) return null;
		ObjectMapper objectMapper = buildObjectMapper();
		return objectMapper.convertValue(map, className);
	}

	public static <T> T map2PojoUseJackson(Map<String, Object> map, TypeReference<T> typeReference) {
		if (null == map || null == typeReference) return null;
		ObjectMapper objectMapper = buildObjectMapper();
		return objectMapper.convertValue(map, typeReference);
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

	public static class TapPartitionTypeDeserializer extends JsonDeserializer<TapPartitionType> {
		@Override
		public TapPartitionType deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
			ObjectCodec codec = p.getCodec();
			TreeNode treeNode = codec.readTree(p);
			if (null == treeNode) return null;
			TextNode partitionTypeNode = (TextNode) treeNode.get(TapPartitionType.KEY_NAME);
			if (null == partitionTypeNode) return null;
			String type = partitionTypeNode.textValue();
			Class<? extends TapPartitionType> tapTypeClass = TapPartitionType.getTapPartitionTypeClass(type);
			if (Objects.isNull(tapTypeClass)) {
				throw new IllegalArgumentException("Unsupported tap type: " + type);
			} else {
				return codec.treeToValue(treeNode, tapTypeClass);
			}
		}
	}

	public static <T> T getValue(Map map, String path, Class<T> clazz) throws Exception {
		OgnlContext ctx = new OgnlContext();
		ctx.setRoot(map);
		return (T) Ognl.getValue(path, ctx, ctx.getRoot());
	}

	static {
		ParserConfig.getGlobalInstance().putDeserializer(TapPartitionType.class, new ObjectDeserializer() {
			@Override
			public TapPartitionType deserialze(DefaultJSONParser parser, Type type, Object fieldName) {
				JSONObject jsonObject = parser.parseObject(JSONObject.class);
				if (null == jsonObject) return null;
				String typeStr = jsonObject.getString(TapPartitionType.KEY_NAME);
				if (null == typeStr) return null;
				return jsonObject.toJavaObject(TapPartitionType.getTapPartitionTypeClass(typeStr));
			}

			@Override
			public int getFastMatchToken() {
				return 0;
			}
		});
	}
}
