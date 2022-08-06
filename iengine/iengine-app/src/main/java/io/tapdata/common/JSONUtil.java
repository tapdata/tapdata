package io.tapdata.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.type.TypeFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by tapdata on 12/12/2017.
 */
public class JSONUtil {

	private static Logger logger = LogManager.getLogger(JSONUtil.class);

	protected static ObjectMapper mapper;

	static {
		mapper = new ObjectMapper();
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
	}

	public static <T> List<T> json2List(String json, Class<T> classz) throws IOException {
		List<T> list = null;
		try {
			TypeFactory typeFactory = mapper.getTypeFactory();
			list = mapper.readValue(json, typeFactory.constructCollectionType(List.class, classz));
		} catch (IOException e) {
			logger.warn("convert json to classz list fail", e);
			throw e;
		}
		return list;
	}

	public static String obj2Json(Object object) throws JsonProcessingException {
		String json = null;
		try {
			json = mapper.writeValueAsString(object);
		} catch (JsonProcessingException e) {
			logger.warn("convert object to json fail", e);
			throw e;
		}
		return json;
	}

	public static Map<String, Object> json2Map(String json) throws IOException {
		Map<String, Object> map = null;
		try {
			map = mapper.readValue(json, new TypeReference<HashMap<String, Object>>() {
			});
		} catch (IOException e) {
			logger.warn("convert object to map fail", e.getMessage());
			throw e;
		}

		return map;
	}
}
