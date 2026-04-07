package io.tapdata.flow.engine.util;

import cn.hutool.json.JSONUtil;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.type.TapDateTime;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.observable.logging.ObsLogger;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.*;

public class TestRunInputEventConvertUtil {
	private TestRunInputEventConvertUtil() {
	}

	public static List<Map<String, Object>> parseTestRunInputEventJson(String inputEventJson,ObsLogger obsLogger) {
		String trimmedJson = StringUtils.trim(inputEventJson);
		try {
			return parseEventDataList(trimmedJson);
		} catch (IOException firstException) {
			obsLogger.error("mock json error",firstException);
			throw new RuntimeException(firstException);
		}
	}

	private static List<Map<String, Object>> parseEventDataList(String inputEventJson) throws IOException {
		if (inputEventJson.startsWith("[")) {
			return castToEventDataList(JSONUtil.toList(inputEventJson,Map.class));
		}
		return new ArrayList<>(castToEventDataList(Collections.singletonList(JSONUtil.toBean(inputEventJson,Map.class))));
	}

	static Map<String, Object> convertRecordDataBySchema(Map<String, Object> recordData, LinkedHashMap<String, TapField> tapFieldMap) {
		if (recordData == null) {
			return null;
		}
		return convertMapValues(recordData, tapFieldMap, null);
	}

	private static Map<String, Object> convertMapValues(Map<String, Object> source, LinkedHashMap<String, TapField> tapFieldMap, String parentPath) {
		Map<String, Object> converted = new LinkedHashMap<>();
		for (Map.Entry<String, Object> entry : source.entrySet()) {
			String fieldPath = appendPath(parentPath, entry.getKey());
			converted.put(entry.getKey(), convertValueBySchema(entry.getValue(), tapFieldMap, fieldPath));
		}
		return converted;
	}

	private static Object convertValueBySchema(Object value, LinkedHashMap<String, TapField> tapFieldMap, String fieldPath) {
		Object convertedValue = convertCurrentValue(value, tapFieldMap == null ? null : tapFieldMap.get(fieldPath));
		if (convertedValue instanceof Map) {
			return convertMapValues(castToMap(convertedValue), tapFieldMap, fieldPath);
		}
		if (convertedValue instanceof Collection) {
			return convertCollectionValues((Collection<?>) convertedValue, tapFieldMap, fieldPath);
		}
		return convertedValue;
	}

	private static List<Object> convertCollectionValues(Collection<?> values, LinkedHashMap<String, TapField> tapFieldMap, String fieldPath) {
		List<Object> convertedValues = new ArrayList<>(values.size());
		for (Object item : values) {
			if (item instanceof Map) {
				convertedValues.add(convertMapValues(castToMap(item), tapFieldMap, fieldPath));
			} else if (item instanceof Collection) {
				convertedValues.add(convertCollectionValues((Collection<?>) item, tapFieldMap, fieldPath));
			} else {
				convertedValues.add(item);
			}
		}
		return convertedValues;
	}

	private static Object convertCurrentValue(Object value, TapField tapField) {
		if (value == null || tapField == null || tapField.getTapType() == null) {
			return value;
		}
		try {
			TapType tapType = tapField.getTapType();
			switch (tapType.getType()) {
				case TapType.TYPE_STRING:
					return String.valueOf(value);
				case TapType.TYPE_NUMBER:
					return convertNumberValue(value);
				case TapType.TYPE_BOOLEAN:
					return convertBooleanValue(value);
				case TapType.TYPE_DATE:
					return convertDateValue(value);
				case TapType.TYPE_DATETIME:
					return convertDateTimeValue(value, (TapDateTime) tapType);
				case TapType.TYPE_TIME:
					return convertTimeValue(value);
				case TapType.TYPE_YEAR:
					return convertYearValue(value);
				case TapType.TYPE_ARRAY:
					return convertArrayValue(value);
				case TapType.TYPE_MAP:
					return convertMapValue(value);
				default:
					return value;
			}
		} catch (Exception e) {
			throw new RuntimeException("Type conversion failed",e);
		}
	}

	private static Object convertNumberValue(Object value) {
		if (value instanceof Number) {
			return value;
		}
		if (value instanceof Boolean) {
			return Boolean.TRUE.equals(value) ? 1 : 0;
		}
		if (value instanceof String) {
			String text = StringUtils.trim((String) value);
			if (StringUtils.isBlank(text)) {
				return value;
			}
			BigDecimal decimal = new BigDecimal(text);
			return text.contains(".") || text.contains("e") || text.contains("E") ? decimal.doubleValue() : decimal.longValueExact();
		}
		return value;
	}

	private static Object convertBooleanValue(Object value) {
		if (value instanceof Boolean) {
			return value;
		}
		if (value instanceof Number) {
			return ((Number) value).intValue() != 0;
		}
		if (value instanceof String) {
			String text = StringUtils.trim((String) value);
			if (StringUtils.equalsAnyIgnoreCase(text, "true", "t", "y", "yes", "1")) {
				return true;
			}
			if (StringUtils.equalsAnyIgnoreCase(text, "false", "f", "n", "no", "0")) {
				return false;
			}
		}
		return value;
	}

	private static final String[] DATE_PATTERNS = {
			"yyyy-MM-dd",
			"yyyy/MM/dd",
			"yyyyMMdd",
			"dd-MM-yyyy",
			"MM/dd/yyyy",
			"yyyy.MM.dd",
	};

	private static Object convertDateValue(Object value) {
		if (value instanceof String) {
			String text = StringUtils.trim((String) value);
			if (StringUtils.isBlank(text)) {
				return value;
			}
			for (String pattern : DATE_PATTERNS) {
				try {
					SimpleDateFormat sdf = new SimpleDateFormat(pattern);
					sdf.setLenient(false);
					return new DateTime(sdf.parse(text));
				} catch (ParseException ignored) {
				}
			}
			throw new IllegalArgumentException("Illegal date text: " + text);
		}
		return value;
	}

	private static Object convertDateTimeValue(Object value, TapDateTime tapDateTime) {
		if (value instanceof String) {
			String text = StringUtils.trim((String) value);
			if (StringUtils.isBlank(text)) {
				return value;
			}
			return parseDateTime(text, tapDateTime);
		}
		return value;
	}

	private static Object convertTimeValue(Object value) {
		if (value instanceof String) {
			return DateTime.withTimeStr(StringUtils.trim((String) value));
		}
		return value;
	}

	private static Object convertYearValue(Object value) {
		if (value instanceof Integer) {
			return value;
		}
		if (value instanceof Number) {
			return ((Number) value).intValue();
		}
		if (value instanceof String) {
			String text = StringUtils.trim((String) value);
			if (StringUtils.isBlank(text)) {
				return value;
			}
			return Integer.parseInt(text);
		}
		return value;
	}

	private static Object convertArrayValue(Object value){
		if (value instanceof Collection) {
			return value;
		}
		if (value instanceof String) {
			return JSONUtil.toList(StringUtils.trim((String) value), Map.class);
		}
		return value;
	}

	private static Object convertMapValue(Object value){
		if (value instanceof Map) {
			return value;
		}
		if (value instanceof String) {
			return JSONUtil.toBean(StringUtils.trim((String) value),Map.class);
		}
		return value;
	}

	private static final DateTimeFormatter[] DATETIME_FORMATTERS = buildDateTimeFormatters();

	private static DateTimeFormatter[] buildDateTimeFormatters() {
		String[] patterns = {
				"yyyy-MM-dd HH:mm:ss",
				"yyyy/MM/dd HH:mm:ss",
				"yyyy.MM.dd HH:mm:ss",
				"yyyyMMddHHmmss",
				"dd-MM-yyyy HH:mm:ss",
				"MM/dd/yyyy HH:mm:ss",
		};
		List<DateTimeFormatter> formatters = new ArrayList<>();
		for (String pattern : patterns) {
			formatters.add(new DateTimeFormatterBuilder()
					.appendPattern(pattern)
					.optionalStart()
					.appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
					.optionalEnd()
					.toFormatter());
		}
		return formatters.toArray(new DateTimeFormatter[0]);
	}

	private static DateTime parseDateTime(String text, TapDateTime tapDateTime) {
		try {
			return new DateTime(Instant.parse(text));
		} catch (Exception ignored) {
		}
		try {
			return new DateTime(ZonedDateTime.parse(text));
		} catch (Exception ignored) {
		}
		try {
			return new DateTime(OffsetDateTime.parse(text).toInstant());
		} catch (Exception ignored) {
		}
		try {
			return new DateTime(LocalDateTime.parse(text));
		} catch (Exception ignored) {
		}
		for (DateTimeFormatter formatter : DATETIME_FORMATTERS) {
			try {
				return new DateTime(LocalDateTime.parse(text, formatter));
			} catch (Exception ignored) {
			}
		}
		try {
			Integer fraction = tapDateTime == null ? null : tapDateTime.getFraction();
			return new DateTime(Long.parseLong(text), fraction == null ? 3 : fraction);
		} catch (Exception ignored) {
		}
		throw new IllegalArgumentException("Illegal datetime text: " + text);
	}


	@SuppressWarnings("unchecked")
	private static Map<String, Object> castToMap(Object value) {
		return (Map<String, Object>) value;
	}

	private static String appendPath(String parentPath, String fieldName) {
		return StringUtils.isBlank(parentPath) ? fieldName : parentPath + "." + fieldName;
	}

	@SuppressWarnings("unchecked")
	private static List<Map<String, Object>> castToEventDataList(List<Map> rawEventDataList) {
		return (List<Map<String, Object>>) (List<?>) rawEventDataList;
	}


	public static List<TapEvent> buildTestRunInputTapEvents(TaskDto taskDto, String table, LinkedHashMap<String, TapField> tapFieldLinkedHashMap, TapCodecsFilterManager codecsFilterManager, ObsLogger obsLogger) {
		if (taskDto == null || StringUtils.isBlank(taskDto.getTestRunInputEventJson())) {
			return null;
		}
		List<Map<String, Object>> eventDataList = parseTestRunInputEventJson(taskDto.getTestRunInputEventJson(),obsLogger);
		String explicitEventType = normalizeTestRunInputEventType(taskDto.getTestRunInputEventType());
		List<TapEvent> tapEvents = new ArrayList<>();
		for (Map<String, Object> eventData : eventDataList) {
			tapEvents.add(buildTestRunInputTapEvent(eventData, explicitEventType, table,tapFieldLinkedHashMap,codecsFilterManager));
		}
		return tapEvents;
	}

	private static TapEvent buildTestRunInputTapEvent(Map<String, Object> eventData, String explicitEventType, String table,LinkedHashMap<String, TapField> tapFieldLinkedHashMap,TapCodecsFilterManager codecsFilterManager) {
		String eventType = inferTestRunInputEventType(eventData);
		switch (eventType) {
			case "insert":
				return new TapInsertRecordEvent().init()
						.after(readRecordData(eventData, "after", eventType,tapFieldLinkedHashMap,codecsFilterManager))
						.table(table);
			case "update":
				return new TapUpdateRecordEvent().init()
						.before(readRecordData(eventData, "before", eventType,tapFieldLinkedHashMap,codecsFilterManager))
						.after(readRecordData(eventData, "after", eventType,tapFieldLinkedHashMap,codecsFilterManager))
						.table(table);
			case "delete":
				return new TapDeleteRecordEvent().init()
						.before(readRecordData(eventData, "before", eventType,tapFieldLinkedHashMap,codecsFilterManager))
						.table(table);
			default:
				return null;
		}
	}

	private static String inferTestRunInputEventType(Map<String, Object> eventData) {
		boolean hasBefore = eventData.containsKey("before");
		boolean hasAfter = eventData.containsKey("after");
		if (hasBefore && hasAfter) {
			return "update";
		}
		if (hasAfter) {
			return "insert";
		}
		if (hasBefore) {
			return "delete";
		}
		return null;
	}

	private static String normalizeTestRunInputEventType(String eventType) {
		if (StringUtils.isBlank(eventType)) {
			return null;
		}
		String normalizedEventType = StringUtils.lowerCase(StringUtils.trim(eventType));
		if (StringUtils.equalsAny(normalizedEventType, "insert", "update", "delete")) {
			return normalizedEventType;
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> readRecordData(Map<String, Object> eventData, String key, String eventType, LinkedHashMap<String, TapField> tapFieldLinkedHashMap, TapCodecsFilterManager codecsFilterManager) {
		Object value = eventData.get(key);
		if (value instanceof Map) {
			Map<String, Object> recordData = TestRunInputEventConvertUtil.convertRecordDataBySchema((Map<String, Object>) value, tapFieldLinkedHashMap);
			codecsFilterManager.transformToTapValueMap(recordData,tapFieldLinkedHashMap);
			return recordData;
		}
		return new HashMap<>();
	}
}
