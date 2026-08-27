package io.tapdata.dql.serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tapdata.constant.JSONUtil;
import io.tapdata.dql.model.DqlPayloadSnapshot;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Converts replayable DML events to and from the versioned DQL payload contract.
 */
public class DqlPayloadSerializer {
    public static final String FORMAT = "tap-record-event-json-v1";
    public static final long DEFAULT_MAX_BYTES = 1_048_576L;

    private static final String TAP_EVENT_CLASS = "tapEventClass";
    private static final String TYPE = "type";

    private final long maxBytes;
    private final ObjectMapper objectMapper;

    public DqlPayloadSerializer() {
        this(DEFAULT_MAX_BYTES);
    }

    public DqlPayloadSerializer(long maxBytes) {
        this(maxBytes, JSONUtil.mapper);
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
            throw new IllegalArgumentException("Unsupported DQL payload format: " + snapshot.getPayloadFormat());
        }
        if (!Boolean.TRUE.equals(snapshot.getPayloadComplete())) {
            throw new IllegalArgumentException("Incomplete DQL payload cannot be deserialized");
        }
        if (!(snapshot.getPayloadData() instanceof Map<?, ?> payloadData)) {
            throw new IllegalArgumentException("DQL payload data must be an object");
        }
        long declaredSize = snapshot.getPayloadSize() == null ? 0L : snapshot.getPayloadSize();
        if (declaredSize < 0L || declaredSize > maxBytes || serializedSize(payloadData) > maxBytes) {
            throw new IllegalArgumentException("DQL payload exceeds the configured size limit");
        }

        String eventClassName = stringValue(payloadData.get(TAP_EVENT_CLASS));
        int eventType = intValue(payloadData.get(TYPE));
        Class<? extends TapRecordEvent> eventClass = validateSupportedEvent(eventClassName, eventType);
        try {
            return objectMapper.convertValue(payloadData, eventClass);
        } catch (IllegalArgumentException exception) {
            throw new IllegalArgumentException("DQL payload data cannot be deserialized", exception);
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
            throw new IllegalArgumentException("DQL payload tapEventClass must not be blank");
        }
        return stringValue;
    }

    private int intValue(Object value) {
        if (!(value instanceof Number number)) {
            throw new IllegalArgumentException("DQL payload type must be a number");
        }
        return number.intValue();
    }
}
