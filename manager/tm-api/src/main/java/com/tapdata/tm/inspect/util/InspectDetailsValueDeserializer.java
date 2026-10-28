package com.tapdata.tm.inspect.util;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 校验差异明细 source/target 反序列化
 * <p>
 * 默认小数会解析为 Double，超出 double 精度的小数(如 DECIMAL(38,18))写入明细时丢失精度，页面上两侧显示相同的值。
 * 小数先按 BigDecimal 解析，转为 Double 无损时仍用 Double(与原行为一致)，有损时保留 BigDecimal
 */
public class InspectDetailsValueDeserializer extends JsonDeserializer<Map<String, Object>> {

	@Override
	public Map<String, Object> deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
		Object value = readValue(p);
		if (value instanceof Map) {
			return (Map<String, Object>) value;
		}
		return (Map<String, Object>) ctxt.handleUnexpectedToken(Map.class, p);
	}

	protected Object readValue(JsonParser p) throws IOException {
		JsonToken token = p.currentToken();
		if (null == token) return null;
		switch (token) {
			case START_OBJECT:
				Map<String, Object> map = new LinkedHashMap<>();
				while (p.nextToken() == JsonToken.FIELD_NAME) {
					String name = p.currentName();
					p.nextToken();
					map.put(name, readValue(p));
				}
				return map;
			case START_ARRAY:
				List<Object> list = new ArrayList<>();
				while (p.nextToken() != JsonToken.END_ARRAY) {
					list.add(readValue(p));
				}
				return list;
			case VALUE_STRING:
				return p.getText();
			case VALUE_NUMBER_INT:
				return p.getNumberValue();
			case VALUE_NUMBER_FLOAT:
				return toDoubleIfLossless(p.getDecimalValue());
			case VALUE_TRUE:
				return Boolean.TRUE;
			case VALUE_FALSE:
				return Boolean.FALSE;
			case VALUE_EMBEDDED_OBJECT:
				return p.getEmbeddedObject();
			case VALUE_NULL:
			default:
				return null;
		}
	}

	protected static Object toDoubleIfLossless(BigDecimal value) {
		double d = value.doubleValue();
		if (!Double.isInfinite(d) && new BigDecimal(Double.toString(d)).compareTo(value) == 0) {
			return d;
		}
		return value;
	}
}
