package com.tapdata.constant;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.entity.schema.type.TapType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by tapdata on 12/12/2017.
 */
public class JSONUtil {

	private static Logger logger = LogManager.getLogger(JSONUtil.class);

	public static ObjectMapper mapper;

	static {
		mapper = new ObjectMapper();
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
		SimpleModule simpleModule = new SimpleModule();
		simpleModule.addDeserializer(TapType.class, new TapTypeDeserializer());
		mapper.registerModule(simpleModule);
		mapper.registerModule(new JavaTimeModule());
	}

	/**
	 * Note that after using this function,
	 * should call {@link JSONUtil#enableFeature(com.fasterxml.jackson.databind.SerializationFeature)} to restore the feature
	 *
	 * @param serializationFeature
	 */
	public static void disableFeature(SerializationFeature serializationFeature) {
		mapper.disable(serializationFeature);
	}

	public static void enableFeature(SerializationFeature serializationFeature) {
		mapper.enable(serializationFeature);
	}

	public static List<Map> json2List(String json) throws IOException {
		return json2List(json, Map.class);
	}

	public static <T> List<T> json2List(String json, Class<T> classz) throws IOException {
		List<T> list;
		try {
			TypeFactory typeFactory = mapper.getTypeFactory();
			list = mapper.readValue(json, typeFactory.constructCollectionType(List.class, classz));
//      list = InstanceFactory.instance(JsonParser.class).fromJson(json, new TypeHolder<List<T>>() {
//        },
//        TapConstants.abstractClassDetectors);
		} catch (Throwable e) {
			throw new IOException("parse json to " + classz.getName() + " list failed\n" + json, e);
		}
		return list;
	}

	public static String obj2Json(Object object) throws JsonProcessingException {
		String json;
		try {
			json = mapper.writeValueAsString(object);
		} catch (JsonProcessingException e) {
			logger.error("convert object to json failed.", e);
			logger.info(object);
			throw e;
		}
		return json;
	}

	public static String obj2JsonPretty(Object object) throws JsonProcessingException {
		String json;
		try {
			json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(object);
		} catch (JsonProcessingException e) {
			logger.error("convert object to json format fail", e);
			logger.info(object);
			throw e;
		}
		return json;
	}

	public static Map<String, Object> json2Map(String json) throws IOException {
		Map<String, Object> map;
		try {
			map = mapper.readValue(json, new TypeReference<HashMap<String, Object>>() {
			});
//      map = InstanceFactory.instance(JsonParser.class).fromJson(json, new TypeHolder<HashMap<String, Object>>() {
//        },
//        TapConstants.abstractClassDetectors);
		} catch (Throwable e) {
			throw new IOException("parse json to map failed\n" + json, e);
		}

		return map;
	}

	public static <T> T json2POJO(String json, Class<T> className) throws IOException {
		T pojo;
		try {
			pojo = mapper.readValue(json, className);
//      pojo = InstanceFactory.instance(JsonParser.class).fromJson(json, className,
//        TapConstants.abstractClassDetectors);
		} catch (Throwable e) {
			throw new IOException("parse json to " + className.getName() + " failed\n" + json, e);
		}

		return pojo;
	}

	public static <T> T json2POJO(String json, TypeReference<T> typeReference) throws IOException {
		T pojo;
		try {
			pojo = mapper.readValue(json, typeReference);
//      pojo = InstanceFactory.instance(JsonParser.class).fromJson(json, typeReference.getType(),
//        TapConstants.abstractClassDetectors);
		} catch (Throwable e) {
			throw new IOException("parse json to " + typeReference.getType().getTypeName() + " failed\n" + json, e);
		}

		return pojo;
	}

	public static <T> T map2POJO(Map map, Class<T> className) {

		return mapper.convertValue(map, className);
	}

	public static <T> T map2POJO(Map map, TypeReference<T> typeReference) {
		return mapper.convertValue(map, typeReference);
	}

	public static String map2Json(Map map) throws JsonProcessingException {
		return mapper.writeValueAsString(map);
	}

	public static String map2JsonPretty(Map map) throws JsonProcessingException {
		return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(map);
	}

	static class TapTypeDeserializer extends JsonDeserializer<TapType> {
		@Override
		public TapType deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
			ObjectCodec codec = p.getCodec();
			TreeNode treeNode = codec.readTree(p);
			if (!(treeNode instanceof ObjectNode)) {
				throw new RuntimeException("Deserialize TapType failed, expected ObjectNode, actual: " + treeNode.getClass().getSimpleName() + ", tree node: " + treeNode);
			}
			TreeNode type = treeNode.get("type");
			if (null == type) {
				throw new RuntimeException("Deserialize TapType failed, type number not exists: " + treeNode);
			}
			int typeInt;
			try {
				typeInt = (int) ((IntNode) type).numberValue();
			} catch (Throwable e) {
				throw new RuntimeException("Deserialize TapType failed, expected type node is a IntNode, actual: " + type.getClass().getSimpleName() + ", type node: " + type);
			}
			Class<? extends TapType> tapTypeClass = TapType.getTapTypeClass((byte) typeInt);
			if (null != tapTypeClass) {
				if (tapTypeClass.getName().equals(TapNumber.class.getName())) {
					TreeNode minValue = treeNode.get("minValue");
					if (minValue instanceof TextNode && "-Infinity".equals(((TextNode) minValue).asText())) {
						((ObjectNode) treeNode).put("minValue", new BigDecimal("-1E+6145"));
					}
					TreeNode maxValue = treeNode.get("maxValue");
					if (maxValue instanceof TextNode && "Infinity".equals(((TextNode) maxValue).asText())) {
						((ObjectNode) treeNode).put("maxValue", new BigDecimal("1E+6145"));
					}
				}
				return codec.treeToValue(treeNode, tapTypeClass);
			} else {
				throw new RuntimeException("Deserialize TapType failed, cannot find a TapType class by type number: " + typeInt + ", tree node: " + treeNode);
			}
		}
	}
}
