package io.tapdata.dql.preview;

import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Builds a recursively masked and bounded preview from one DML event.
 */
public class DqlPayloadPreviewBuilder {
    public static final int DEFAULT_FIELD_MAX_LENGTH = 512;
    public static final int DEFAULT_MAX_DEPTH = 4;
    public static final int DEFAULT_MAX_ITEMS = 50;

    private static final String MASKED_VALUE = "******";
    private static final String TRUNCATED_MARKER = "...";
    private static final Set<String> SENSITIVE_FIELDS = Set.of(
            "password", "passwd", "secret", "token", "accesstoken", "authorization", "credential", "apikey"
    );

    private final int fieldMaxLength;
    private final int maxDepth;
    private final int maxItems;

    public DqlPayloadPreviewBuilder() {
        this(DEFAULT_FIELD_MAX_LENGTH, DEFAULT_MAX_DEPTH, DEFAULT_MAX_ITEMS);
    }

    public DqlPayloadPreviewBuilder(int fieldMaxLength, int maxDepth, int maxItems) {
        if (fieldMaxLength <= 0) {
            throw new IllegalArgumentException("fieldMaxLength must be greater than zero");
        }
        if (maxDepth < 0) {
            throw new IllegalArgumentException("maxDepth must not be negative");
        }
        if (maxItems <= 0) {
            throw new IllegalArgumentException("maxItems must be greater than zero");
        }
        this.fieldMaxLength = fieldMaxLength;
        this.maxDepth = maxDepth;
        this.maxItems = maxItems;
    }

    public DqlPayloadPreview build(TapRecordEvent event, Map<String, Object> eventKey) {
        if (event == null) {
            throw new IllegalArgumentException("event must not be null");
        }
        PreviewContext context = new PreviewContext();
        Map<String, Object> preview = new LinkedHashMap<>();
        if (eventKey != null && !eventKey.isEmpty()) {
            preview.put("key", sanitizeMap(eventKey, 0, context));
        }
        if (event instanceof TapInsertRecordEvent insert && insert.getAfter() != null) {
            preview.put("after", sanitizeMap(insert.getAfter(), 0, context));
        } else if (event instanceof TapUpdateRecordEvent update) {
            if (update.getBefore() != null) {
                preview.put("before", sanitizeMap(update.getBefore(), 0, context));
            }
            if (update.getAfter() != null) {
                preview.put("after", sanitizeMap(update.getAfter(), 0, context));
            }
        } else if (event instanceof TapDeleteRecordEvent delete && delete.getBefore() != null) {
            preview.put("before", sanitizeMap(delete.getBefore(), 0, context));
        }
        if (!context.truncatedFields.isEmpty()) {
            preview.put("truncatedFields", List.copyOf(context.truncatedFields));
        }
        if (!context.maskedFields.isEmpty()) {
            preview.put("maskedFields", List.copyOf(context.maskedFields));
        }
        return new DqlPayloadPreview(preview, !context.truncatedFields.isEmpty());
    }

    private Map<String, Object> sanitizeMap(Map<?, ?> source, int depth, PreviewContext context) {
        Map<String, Object> result = new LinkedHashMap<>();
        int count = 0;
        for (Map.Entry<?, ?> entry : source.entrySet()) {
            if (count >= maxItems) {
                context.truncatedFields.add("<items>");
                break;
            }
            String field = String.valueOf(entry.getKey());
            if (isSensitive(field)) {
                context.maskedFields.add(field);
                result.put(field, MASKED_VALUE);
            } else {
                result.put(field, sanitizeValue(entry.getValue(), depth + 1, field, context));
            }
            count++;
        }
        return result;
    }

    private Object sanitizeValue(Object value, int depth, String field, PreviewContext context) {
        if (value == null || value instanceof Number || value instanceof Boolean) {
            return value;
        }
        if (value instanceof CharSequence sequence) {
            String stringValue = sequence.toString();
            if (stringValue.length() <= fieldMaxLength) {
                return stringValue;
            }
            context.truncatedFields.add(field);
            return stringValue.substring(0, fieldMaxLength);
        }
        if (value instanceof Map<?, ?> map) {
            if (depth > maxDepth) {
                context.truncatedFields.add(field);
                return TRUNCATED_MARKER;
            }
            return sanitizeMap(map, depth, context);
        }
        if (value instanceof Collection<?> collection) {
            if (depth > maxDepth) {
                context.truncatedFields.add(field);
                return TRUNCATED_MARKER;
            }
            return sanitizeCollection(collection, depth, field, context);
        }
        if (value.getClass().isArray()) {
            if (depth > maxDepth) {
                context.truncatedFields.add(field);
                return TRUNCATED_MARKER;
            }
            return sanitizeArray(value, depth, field, context);
        }
        return sanitizeValue(String.valueOf(value), depth, field, context);
    }

    private List<Object> sanitizeCollection(Collection<?> source,
                                            int depth,
                                            String field,
                                            PreviewContext context) {
        List<Object> result = new ArrayList<>(Math.min(source.size(), maxItems));
        int count = 0;
        for (Object value : source) {
            if (count >= maxItems) {
                context.truncatedFields.add(field);
                break;
            }
            result.add(sanitizeValue(value, depth + 1, field, context));
            count++;
        }
        return result;
    }

    private List<Object> sanitizeArray(Object source, int depth, String field, PreviewContext context) {
        int length = Array.getLength(source);
        int kept = Math.min(length, maxItems);
        List<Object> result = new ArrayList<>(kept);
        for (int index = 0; index < kept; index++) {
            result.add(sanitizeValue(Array.get(source, index), depth + 1, field, context));
        }
        if (length > maxItems) {
            context.truncatedFields.add(field);
        }
        return result;
    }

    private boolean isSensitive(String field) {
        String normalized = field == null ? "" : field.replace("_", "").replace("-", "").toLowerCase(Locale.ROOT);
        return SENSITIVE_FIELDS.contains(normalized);
    }

    private static final class PreviewContext {
        private final List<String> maskedFields = new ArrayList<>();
        private final List<String> truncatedFields = new ArrayList<>();
    }
}
