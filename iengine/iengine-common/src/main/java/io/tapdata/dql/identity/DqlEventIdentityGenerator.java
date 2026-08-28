package io.tapdata.dql.identity;

import io.tapdata.dql.model.DqlEventIdentity;
import io.tapdata.dql.model.DqlRecordIdentityType;
import io.tapdata.dql.serializer.DqlPayloadSerializer;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Generates deterministic Engine-owned identities before a DQL payload is size-limited.
 */
public class DqlEventIdentityGenerator {
    private static final List<String> SOURCE_OFFSET_KEYS = List.of(
            "sourceoffset", "lsn", "oplogposition", "oplogoffset", "offset"
    );
    private static final int KEY_STRING_MAX_LENGTH = 512;
    private static final String MASKED_VALUE = "******";

    private final DqlPayloadSerializer fullPayloadSerializer;
    private final DqlCanonicalJson canonicalJson;

    public DqlEventIdentityGenerator() {
        this(new DqlPayloadSerializer(Long.MAX_VALUE), new DqlCanonicalJson());
    }

    DqlEventIdentityGenerator(DqlPayloadSerializer fullPayloadSerializer, DqlCanonicalJson canonicalJson) {
        this.fullPayloadSerializer = fullPayloadSerializer;
        this.canonicalJson = canonicalJson;
    }

    public DqlEventIdentity generate(TapRecordEvent event,
                                     TapTable table,
                                     String taskRecordId,
                                     String failedNodeId) {
        return generate(event, table, taskRecordId, failedNodeId, List.of());
    }

    /**
     * Generates an identity using the target task's update-condition fields
     * when the record has no usable primary-key value.  The fields are
     * intentionally supplied by the caller because they are task-node
     * configuration, not table schema metadata.
     */
    public DqlEventIdentity generate(TapRecordEvent event,
                                     TapTable table,
                                     String taskRecordId,
                                     String failedNodeId,
                                     Collection<String> updateConditionFields) {
        if (event == null) {
            throw new IllegalArgumentException("event must not be null");
        }
        String dmlType = dmlType(event);
        Map<String, Object> payloadData = payloadData(event);
        String payloadHash = canonicalJson.sha256(payloadData);
        String tableId = tableId(event, table);
        Long eventTime = eventTime(event);

        KeySelection keySelection = selectKey(event, table, updateConditionFields);
        Map<String, Object> eventKey = keySelection.values();
        DqlRecordIdentityType identityType;
        String recordIdentity;
        List<String> identityFields;
        if (keySelection.complete()) {
            identityType = keySelection.type();
            identityFields = List.copyOf(keySelection.fields());
            recordIdentity = "key:" + tableId + ":" + canonicalJson.sha256(keySelection.rawValues());
        } else if (recordImage(event) != null) {
            identityType = DqlRecordIdentityType.FULL_FIELD_HASH;
            identityFields = List.of();
            recordIdentity = "hash:" + tableId + ":" + canonicalJson.sha256(recordImage(event));
        } else {
            identityType = DqlRecordIdentityType.UNKNOWN;
            identityFields = List.of();
            recordIdentity = null;
        }

        DqlEventIdentity result = new DqlEventIdentity();
        result.setEventKey(eventKey);
        result.setEventKeyMissing(!keySelection.complete());
        result.setPayloadHash(payloadHash);
        result.setRecordIdentity(recordIdentity);
        result.setRecordIdentityType(identityType);
        result.setRecordIdentityFields(identityFields);
        result.setEventIdentity(eventIdentity(event, taskRecordId, failedNodeId, tableId, dmlType,
                eventTime, payloadHash, keySelection));
        return result;
    }

    private Map<String, Object> payloadData(TapRecordEvent event) {
        Object payloadData = fullPayloadSerializer.serialize(event).getPayloadData();
        if (!(payloadData instanceof Map<?, ?> payloadMap)) {
            throw new IllegalArgumentException("DQL payload data must be an object");
        }
        Map<String, Object> result = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : payloadMap.entrySet()) {
            result.put(String.valueOf(entry.getKey()), entry.getValue());
        }
        return result;
    }

    private KeySelection selectKey(TapRecordEvent event,
                                   TapTable table,
                                   Collection<String> updateConditionFields) {
        if (table == null) {
            return KeySelection.missing();
        }
        List<String> primaryKeys = primaryKeys(table);
        if (!primaryKeys.isEmpty()) {
            KeySelection primary = keySelection(event, primaryKeys, DqlRecordIdentityType.PRIMARY_KEY);
            if (primary.complete()) {
                return primary;
            }
        }
        KeySelection updateCondition = keySelection(event,
                normalizeFields(updateConditionFields), DqlRecordIdentityType.UPDATE_CONDITION);
        if (updateCondition.complete()) {
            return updateCondition;
        }
        if (table.getIndexList() != null) {
            for (TapIndex index : table.getIndexList()) {
                if (index == null || !index.isUnique()) {
                    continue;
                }
                List<String> fields = indexFields(index);
                KeySelection unique = keySelection(event, fields, DqlRecordIdentityType.UNIQUE_INDEX);
                if (unique.complete()) {
                    return unique;
                }
            }
        }
        return KeySelection.missing();
    }

    private List<String> normalizeFields(Collection<String> fields) {
        if (fields == null || fields.isEmpty()) {
            return List.of();
        }
        return fields.stream().filter(this::hasText).toList();
    }

    private List<String> primaryKeys(TapTable table) {
        if (table.getNameFieldMap() == null) {
            List<String> defaultKeys = table.getDefaultPrimaryKeys();
            return defaultKeys == null ? List.of() : defaultKeys.stream().filter(this::hasText).toList();
        }
        Collection<String> declaredKeys = table.primaryKeys();
        if (declaredKeys != null && !declaredKeys.isEmpty()) {
            return declaredKeys.stream().filter(this::hasText).toList();
        }
        return table.getNameFieldMap().values().stream()
                .filter(field -> field != null && (Boolean.TRUE.equals(field.getPrimaryKey())
                        || field.getPrimaryKeyPos() != null && field.getPrimaryKeyPos() > 0))
                .sorted(Comparator.comparing(TapField::getPrimaryKeyPos,
                        Comparator.nullsLast(Integer::compareTo)))
                .map(TapField::getName)
                .filter(this::hasText)
                .toList();
    }

    private List<String> indexFields(TapIndex index) {
        if (index.getIndexFields() == null) {
            return List.of();
        }
        return index.getIndexFields().stream()
                .filter(field -> field != null && hasText(field.getName()))
                .map(TapIndexField::getName)
                .toList();
    }

    private KeySelection keySelection(TapRecordEvent event,
                                      List<String> fields,
                                      DqlRecordIdentityType type) {
        if (fields == null || fields.isEmpty()) {
            return KeySelection.missing();
        }
        Map<String, Object> rawValues = new LinkedHashMap<>();
        Map<String, Object> safeValues = new LinkedHashMap<>();
        for (String field : fields) {
            Object value = valueForField(event, field);
            if (value == null) {
                return KeySelection.missing();
            }
            rawValues.put(field, value);
            safeValues.put(field, safeKeyValue(field, value));
        }
        return new KeySelection(type, List.copyOf(fields), rawValues, safeValues, true);
    }

    private Object valueForField(TapRecordEvent event, String field) {
        if (event instanceof TapInsertRecordEvent insert) {
            return value(insert.getAfter(), field);
        }
        if (event instanceof TapUpdateRecordEvent update) {
            return value(update.getAfter(), field);
        }
        if (event instanceof TapDeleteRecordEvent delete) {
            return value(delete.getBefore(), field);
        }
        return null;
    }

    private Object value(Map<String, Object> source, String field) {
        return source == null ? null : source.get(field);
    }

    private Map<String, Object> recordImage(TapRecordEvent event) {
        Map<String, Object> image;
        if (event instanceof TapInsertRecordEvent insert) {
            image = insert.getAfter();
        } else if (event instanceof TapUpdateRecordEvent update) {
            image = update.getAfter() != null ? update.getAfter() : update.getBefore();
        } else if (event instanceof TapDeleteRecordEvent delete) {
            image = delete.getBefore();
        } else {
            image = null;
        }
        return image == null ? null : new LinkedHashMap<>(image);
    }

    private String eventIdentity(TapRecordEvent event,
                                 String taskRecordId,
                                 String failedNodeId,
                                 String tableId,
                                 String dmlType,
                                 Long eventTime,
                                 String payloadHash,
                                 KeySelection keySelection) {
        if (hasText(event.getExactlyOnceId())) {
            return "eo:" + event.getExactlyOnceId();
        }
        Object sourceOffset = sourceOffset(event.getInfo());
        if (sourceOffset != null) {
            return "offset:" + canonicalJson.sha256(sourceOffset);
        }
        if (keySelection.complete()) {
            return String.join(":", "key", component(taskRecordId), tableId, dmlType,
                    component(eventTime), canonicalJson.sha256(keySelection.rawValues()), payloadHash);
        }
        return String.join(":", "payload", component(taskRecordId), tableId, dmlType,
                component(eventTime), payloadHash, component(failedNodeId));
    }

    private Object sourceOffset(Map<String, Object> info) {
        if (info == null || info.isEmpty()) {
            return null;
        }
        for (String preferredKey : SOURCE_OFFSET_KEYS) {
            for (Map.Entry<String, Object> entry : info.entrySet()) {
                if (normalize(entry.getKey()).equals(preferredKey) && hasValue(entry.getValue())) {
                    return entry.getValue();
                }
            }
        }
        return null;
    }

    private Object safeKeyValue(String field, Object value) {
        if (isSensitive(field)) {
            return MASKED_VALUE;
        }
        if (value instanceof String stringValue) {
            return stringValue.length() > KEY_STRING_MAX_LENGTH
                    ? stringValue.substring(0, KEY_STRING_MAX_LENGTH) : stringValue;
        }
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> result = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                String nestedField = String.valueOf(entry.getKey());
                result.put(nestedField, safeKeyValue(nestedField, entry.getValue()));
            }
            return result;
        }
        if (value instanceof Iterable<?> iterable) {
            List<Object> result = new ArrayList<>();
            for (Object item : iterable) {
                result.add(safeKeyValue(field, item));
            }
            return result;
        }
        return value;
    }

    private boolean isSensitive(String field) {
        String normalized = normalize(field);
        return List.of("password", "passwd", "secret", "token", "accesstoken", "authorization",
                "credential", "apikey").contains(normalized);
    }

    private String tableId(TapRecordEvent event, TapTable table) {
        if (hasText(event.getTableId())) {
            return event.getTableId();
        }
        if (table != null && hasText(table.getId())) {
            return table.getId();
        }
        return table == null ? "" : component(table.getName());
    }

    private Long eventTime(TapRecordEvent event) {
        return event.getReferenceTime() != null ? event.getReferenceTime() : event.getTime();
    }

    private String dmlType(TapRecordEvent event) {
        if (event instanceof TapInsertRecordEvent) {
            return "I";
        }
        if (event instanceof TapUpdateRecordEvent) {
            return "U";
        }
        if (event instanceof TapDeleteRecordEvent) {
            return "D";
        }
        throw new IllegalArgumentException("Unsupported DQL event type: " + event.getClass().getName());
    }

    private boolean hasValue(Object value) {
        if (value == null) {
            return false;
        }
        if (value instanceof String stringValue) {
            return hasText(stringValue);
        }
        if (value instanceof Map<?, ?> map) {
            return !map.isEmpty();
        }
        if (value instanceof Iterable<?> iterable) {
            return iterable.iterator().hasNext();
        }
        return true;
    }

    private boolean hasText(String value) {
        return value != null && !value.isBlank();
    }

    private String normalize(String value) {
        return value == null ? "" : value.replace("_", "").replace("-", "").toLowerCase(Locale.ROOT);
    }

    private String component(Object value) {
        return value == null ? "" : String.valueOf(value);
    }

    private record KeySelection(DqlRecordIdentityType type,
                                List<String> fields,
                                Map<String, Object> rawValues,
                                Map<String, Object> values,
                                boolean complete) {
        private static KeySelection missing() {
            return new KeySelection(null, List.of(), Map.of(), Map.of(), false);
        }
    }
}
