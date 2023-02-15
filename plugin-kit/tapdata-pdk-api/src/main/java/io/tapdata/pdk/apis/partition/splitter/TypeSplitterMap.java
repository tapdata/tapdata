package io.tapdata.pdk.apis.partition.splitter;

import io.tapdata.entity.codec.impl.utils.AnyTimeToDateTime;
import io.tapdata.entity.schema.value.DateTime;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author aplomb
 */
public class TypeSplitterMap {
	public static final String TYPE_NUMBER = "number"; // long, double, int, short, float, etc.
	public static final String TYPE_STRING = "string"; // String
	public static final String TYPE_BOOLEAN = "bool"; // Boolean
	public static final String TYPE_DATE = "date"; // Date
	private final Map<String, TypeSplitter<?>> typeSplitterMap = new ConcurrentHashMap<>();
	public TypeSplitterMap() {
		typeSplitterMap.put(TYPE_BOOLEAN, BooleanSplitter.INSTANCE);
		typeSplitterMap.put(TYPE_DATE, DateTimeSplitter.INSTANCE);
		typeSplitterMap.put(TYPE_NUMBER, NumberSplitter.INSTANCE);
		typeSplitterMap.put(TYPE_STRING, StringSplitter.INSTANCE);
	}
	public static String detectType(Object value) {
		if(value == null)
			return null;
		String type = null;
		if(value instanceof Number) {
			type = TYPE_NUMBER;
		} else if(value instanceof String) {
			type = TYPE_STRING;
		} else if(value instanceof Boolean) {
			type = TYPE_BOOLEAN;
		} else {
			if(value instanceof Date ||
					value instanceof LocalDateTime ||
					value instanceof Instant ||
					value instanceof ZonedDateTime ||
					value instanceof DateTime
			) {
				type = TYPE_DATE;
			} else {
				type = value.getClass().getName();
			}
		}
		return type;
	}

	public TypeSplitterMap registerCustomSplitter(Class<?> clazz, TypeSplitter<?> typeSplitter) {
		typeSplitterMap.put(clazz.getName(), typeSplitter);
		return this;
	}

	public TypeSplitterMap registerSplitter(String type, TypeSplitter<?> typeSplitter) {
		typeSplitterMap.put(type, typeSplitter);
		return this;
	}

	public TypeSplitter<?> get(String type) {
		return typeSplitterMap.get(type);
	}
}
