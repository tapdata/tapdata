package io.tapdata.dql.serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tapdata.constant.JSONUtil;
import com.tapdata.tm.dql.config.DqlRuntimeConfig;
import io.tapdata.dql.model.DqlPayloadSnapshot;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Converts replayable DML events to and from the versioned DLQ payload contract.
 */
public class DqlPayloadSerializer {
    public static final String FORMAT = "tap-record-event-json-v1";
    public static final long DEFAULT_MAX_BYTES = 1_048_576L;

    private static final String TAP_EVENT_CLASS = "tapEventClass";
    private static final String TYPE = "type";
    private static final String VALUE_TYPE = "__tapdata_dql_value_type";
    private static final String VALUE = "value";

    private final long maxBytes;
    private final ObjectMapper objectMapper;

    public DqlPayloadSerializer() {
        this(DEFAULT_MAX_BYTES);
    }

    public DqlPayloadSerializer(long maxBytes) {
        this(maxBytes, JSONUtil.mapper);
    }

    public DqlPayloadSerializer(DqlRuntimeConfig config) {
        this(config == null ? DEFAULT_MAX_BYTES : config.getPayloadMaxBytes());
    }

    DqlPayloadSerializer(long maxBytes, ObjectMapper objectMapper) {
        if (maxBytes <= 0L) {
            throw new IllegalArgumentException("maxBytes must be greater than zero");
        }
        this.maxBytes = maxBytes;
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
    }

    public DqlPayloadSnapshot serialize(TapRecordEvent event) {
        if (event == null) {
            throw new IllegalArgumentException("event must not be null");
        }
        validateSupportedEvent(event.getClass().getName(), event.getType());

        Map<String, Object> eventFields;
        try {
            eventFields = objectMapper.convertValue(event, new TypeReference<Map<String, Object>>() {
            });
        } catch (IllegalArgumentException exception) {
            throw new IllegalArgumentException("TapRecordEvent payload cannot be serialized", exception);
        }
        preserveTypedRecordValues(eventFields, event);
        Map<String, Object> payloadData = new LinkedHashMap<>();
        payloadData.put(TAP_EVENT_CLASS, event.getClass().getName());
        payloadData.putAll(eventFields);

        long payloadSize = serializedSize(payloadData);
        DqlPayloadSnapshot snapshot = new DqlPayloadSnapshot();
        snapshot.setPayloadFormat(FORMAT);
        snapshot.setPayloadSize(payloadSize);
        snapshot.setPayloadComplete(payloadSize <= maxBytes);
        if (payloadSize <= maxBytes) {
            snapshot.setPayloadData(payloadData);
        }
        return snapshot;
    }

    public TapRecordEvent deserialize(DqlPayloadSnapshot snapshot) {
        if (snapshot == null) {
            throw new IllegalArgumentException("snapshot must not be null");
        }
        if (!FORMAT.equals(snapshot.getPayloadFormat())) {
            throw new IllegalArgumentException("Unsupported DLQ payload format: " + snapshot.getPayloadFormat());
        }
        if (!Boolean.TRUE.equals(snapshot.getPayloadComplete())) {
            throw new IllegalArgumentException("Incomplete DLQ payload cannot be deserialized");
        }
        if (!(snapshot.getPayloadData() instanceof Map<?, ?> payloadData)) {
            throw new IllegalArgumentException("DLQ payload data must be an object");
        }
        long declaredSize = snapshot.getPayloadSize() == null ? 0L : snapshot.getPayloadSize();
        if (declaredSize < 0L || declaredSize > maxBytes || serializedSize(payloadData) > maxBytes) {
            throw new IllegalArgumentException("DLQ payload exceeds the configured size limit");
        }

        Map<String, Object> decodedPayloadData = decodeMap(payloadData);
        String eventClassName = stringValue(decodedPayloadData.get(TAP_EVENT_CLASS));
        int eventType = intValue(decodedPayloadData.get(TYPE));
        Class<? extends TapRecordEvent> eventClass = validateSupportedEvent(eventClassName, eventType);
        try {
            TapRecordEvent event = objectMapper.convertValue(decodedPayloadData, eventClass);
            restoreTypedRecordValues(event, decodedPayloadData);
            return event;
        } catch (IllegalArgumentException exception) {
            throw new IllegalArgumentException("DLQ payload data cannot be deserialized", exception);
        }
    }

    @SuppressWarnings("unchecked")
    private void restoreTypedRecordValues(TapRecordEvent event, Map<String, Object> payloadData) {
        if (event instanceof TapInsertRecordEvent insertEvent) {
            insertEvent.setAfter((Map<String, Object>) mergeTypedValues(insertEvent.getAfter(), payloadData.get("after")));
        } else if (event instanceof TapUpdateRecordEvent updateEvent) {
            updateEvent.setBefore((Map<String, Object>) mergeTypedValues(updateEvent.getBefore(), payloadData.get("before")));
            updateEvent.setAfter((Map<String, Object>) mergeTypedValues(updateEvent.getAfter(), payloadData.get("after")));
        } else if (event instanceof TapDeleteRecordEvent deleteEvent) {
            deleteEvent.setBefore((Map<String, Object>) mergeTypedValues(deleteEvent.getBefore(), payloadData.get("before")));
        }
    }

    private Object mergeTypedValues(Object convertedValue, Object decodedValue) {
        if (decodedValue instanceof LocalDateTime || decodedValue instanceof LocalDate || decodedValue instanceof LocalTime
                || decodedValue instanceof Instant || decodedValue instanceof OffsetDateTime
                || decodedValue instanceof OffsetTime || decodedValue instanceof ZonedDateTime) {
            return decodedValue;
        }
        if (decodedValue instanceof Map<?, ?> decodedMap && convertedValue instanceof Map<?, ?> convertedMap) {
            Map<Object, Object> merged = new LinkedHashMap<>();
            convertedMap.forEach((key, value) -> merged.put(key, value));
            decodedMap.forEach((key, value) -> merged.put(key,
                    mergeTypedValues(merged.get(key), value)));
            return merged;
        }
        if (decodedValue instanceof List<?> decodedList && convertedValue instanceof List<?> convertedList) {
            List<Object> merged = new ArrayList<>(convertedList);
            for (int i = 0; i < decodedList.size() && i < merged.size(); i++) {
                merged.set(i, mergeTypedValues(merged.get(i), decodedList.get(i)));
            }
            return merged;
        }
        return convertedValue;
    }

    private void preserveTypedRecordValues(Map<String, Object> eventFields, TapRecordEvent event) {
        if (event instanceof TapInsertRecordEvent insertEvent) {
            eventFields.put("after", encodeValue(eventFields.get("after"), insertEvent.getAfter()));
        } else if (event instanceof TapUpdateRecordEvent updateEvent) {
            eventFields.put("before", encodeValue(eventFields.get("before"), updateEvent.getBefore()));
            eventFields.put("after", encodeValue(eventFields.get("after"), updateEvent.getAfter()));
        } else if (event instanceof TapDeleteRecordEvent deleteEvent) {
            eventFields.put("before", encodeValue(eventFields.get("before"), deleteEvent.getBefore()));
        }
    }

    private Object encodeValue(Object serializedValue, Object originalValue) {
        String temporalType = temporalType(originalValue);
        if (temporalType != null) {
            Map<String, Object> encoded = new LinkedHashMap<>();
            encoded.put(VALUE_TYPE, temporalType);
            encoded.put(VALUE, originalValue.toString());
            return encoded;
        }
        if (originalValue instanceof Map<?, ?> originalMap && serializedValue instanceof Map<?, ?> serializedMap) {
            Map<String, Object> encoded = new LinkedHashMap<>();
            serializedMap.forEach((key, value) -> encoded.put(String.valueOf(key),
                    encodeValue(value, originalMap.get(key))));
            return encoded;
        }
        if (originalValue instanceof List<?> originalList && serializedValue instanceof List<?> serializedList) {
            List<Object> encoded = new ArrayList<>(serializedList.size());
            for (int i = 0; i < serializedList.size(); i++) {
                Object originalItem = i < originalList.size() ? originalList.get(i) : null;
                encoded.add(encodeValue(serializedList.get(i), originalItem));
            }
            return encoded;
        }
        return serializedValue;
    }

    private String temporalType(Object value) {
        if (value instanceof LocalDateTime) {
            return "local_date_time";
        }
        if (value instanceof LocalDate) {
            return "local_date";
        }
        if (value instanceof LocalTime) {
            return "local_time";
        }
        if (value instanceof Instant) {
            return "instant";
        }
        if (value instanceof OffsetDateTime) {
            return "offset_date_time";
        }
        if (value instanceof OffsetTime) {
            return "offset_time";
        }
        if (value instanceof ZonedDateTime) {
            return "zoned_date_time";
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> decodeMap(Map<?, ?> source) {
        Map<String, Object> decoded = new LinkedHashMap<>();
        source.forEach((key, value) -> decoded.put(String.valueOf(key), decodeValue(value)));
        return decoded;
    }

    private Object decodeValue(Object value) {
        if (value instanceof Map<?, ?> map) {
            Object type = map.get(VALUE_TYPE);
            Object encodedValue = map.get(VALUE);
            if (type instanceof String typeName && encodedValue instanceof String stringValue) {
                Object temporalValue = decodeTemporalValue(typeName, stringValue);
                if (temporalValue != null) {
                    return temporalValue;
                }
            }
            return decodeMap(map);
        }
        if (value instanceof List<?> list) {
            List<Object> decoded = new ArrayList<>(list.size());
            list.forEach(item -> decoded.add(decodeValue(item)));
            return decoded;
        }
        return value;
    }

    private Object decodeTemporalValue(String type, String value) {
        try {
            return switch (type) {
                case "local_date_time" -> LocalDateTime.parse(value);
                case "local_date" -> LocalDate.parse(value);
                case "local_time" -> LocalTime.parse(value);
                case "instant" -> Instant.parse(value);
                case "offset_date_time" -> OffsetDateTime.parse(value);
                case "offset_time" -> OffsetTime.parse(value);
                case "zoned_date_time" -> ZonedDateTime.parse(value);
                default -> null;
            };
        } catch (RuntimeException ignored) {
            return null;
        }
    }

    private long serializedSize(Map<?, ?> payloadData) {
        try {
            return objectMapper.writeValueAsBytes(payloadData).length;
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException("TapRecordEvent payload cannot be serialized", exception);
        }
    }

    private Class<? extends TapRecordEvent> validateSupportedEvent(String eventClassName, int eventType) {
        Class<? extends TapRecordEvent> eventClass;
        int expectedType;
        if (TapInsertRecordEvent.class.getName().equals(eventClassName)) {
            eventClass = TapInsertRecordEvent.class;
            expectedType = TapInsertRecordEvent.TYPE;
        } else if (TapUpdateRecordEvent.class.getName().equals(eventClassName)) {
            eventClass = TapUpdateRecordEvent.class;
            expectedType = TapUpdateRecordEvent.TYPE;
        } else if (TapDeleteRecordEvent.class.getName().equals(eventClassName)) {
            eventClass = TapDeleteRecordEvent.class;
            expectedType = TapDeleteRecordEvent.TYPE;
        } else {
            throw new IllegalArgumentException("Unsupported TapRecordEvent class: " + eventClassName);
        }
        if (eventType != expectedType) {
            throw new IllegalArgumentException("TapRecordEvent type does not match class: " + eventClassName);
        }
        return eventClass;
    }

    private String stringValue(Object value) {
        if (!(value instanceof String stringValue) || stringValue.isBlank()) {
            throw new IllegalArgumentException("DLQ payload tapEventClass must not be blank");
        }
        return stringValue;
    }

    private int intValue(Object value) {
        if (!(value instanceof Number number)) {
            throw new IllegalArgumentException("DLQ payload type must be a number");
        }
        return number.intValue();
    }
}
