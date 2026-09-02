package io.tapdata.dql.recovery;

import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;

import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Restores temporal scalar values from legacy DQL payloads.
 *
 * <p>Older payloads contain Java time values as Jackson timestamp arrays. The
 * generic DQL event map cannot recover their runtime type, so this conversion
 * is applied only during target-side DQL replay and only when the target
 * schema identifies the field as a scalar temporal column.</p>
 */
public final class DqlRecoveryValueNormalizer {

    private DqlRecoveryValueNormalizer() {
    }

    public static void normalize(TapRecordEvent event, TapTable targetTable) {
        if (event == null || targetTable == null) {
            return;
        }
        if (event instanceof TapInsertRecordEvent insertEvent) {
            normalizeMap(insertEvent.getAfter(), targetTable);
        } else if (event instanceof TapUpdateRecordEvent updateEvent) {
            normalizeMap(updateEvent.getBefore(), targetTable);
            normalizeMap(updateEvent.getAfter(), targetTable);
        } else if (event instanceof TapDeleteRecordEvent deleteEvent) {
            normalizeMap(deleteEvent.getBefore(), targetTable);
        }
    }

    private static void normalizeMap(Map<String, Object> values, TapTable targetTable) {
        if (values == null || targetTable.getNameFieldMap() == null) {
            return;
        }
        Map<String, TapField> fields = targetTable.getNameFieldMap();
        values.replaceAll((fieldName, value) -> normalizeValue(value, fields.get(fieldName)));
    }

    private static Object normalizeValue(Object value, TapField field) {
        if (!(value instanceof List<?> components) || field == null || field.getDataType() == null) {
            return value;
        }
        String dataType = field.getDataType().trim().toLowerCase(Locale.ROOT);
        if (isArrayType(dataType)) {
            return value;
        }
        try {
            if (dataType.startsWith("timestamp")) {
                return toLocalDateTime(components, value);
            }
            if (dataType.equals("date") || dataType.startsWith("date(")) {
                return toLocalDate(components, value);
            }
            if (dataType.startsWith("time")) {
                return toLocalTime(components, value);
            }
        } catch (DateTimeException | ArithmeticException ignored) {
            // Keep malformed or non-Java-time arrays unchanged so replay
            // reports the original connector error instead of masking it.
        }
        return value;
    }

    private static boolean isArrayType(String dataType) {
        return dataType.endsWith(" array") || dataType.endsWith("[]");
    }

    private static Object toLocalDateTime(List<?> components, Object originalValue) {
        if (components.size() != 6 && components.size() != 7) {
            return originalValue;
        }
        Integer year = integerComponent(components.get(0));
        Integer month = integerComponent(components.get(1));
        Integer day = integerComponent(components.get(2));
        Integer hour = integerComponent(components.get(3));
        Integer minute = integerComponent(components.get(4));
        Integer second = integerComponent(components.get(5));
        Integer nano = components.size() == 7 ? integerComponent(components.get(6)) : 0;
        if (year == null || month == null || day == null || hour == null || minute == null || second == null || nano == null) {
            return originalValue;
        }
        return LocalDateTime.of(year, month, day, hour, minute, second, nano);
    }

    private static Object toLocalDate(List<?> components, Object originalValue) {
        if (components.size() != 3) {
            return originalValue;
        }
        Integer year = integerComponent(components.get(0));
        Integer month = integerComponent(components.get(1));
        Integer day = integerComponent(components.get(2));
        if (year == null || month == null || day == null) {
            return originalValue;
        }
        return LocalDate.of(year, month, day);
    }

    private static Object toLocalTime(List<?> components, Object originalValue) {
        if (components.size() != 3 && components.size() != 4) {
            return originalValue;
        }
        Integer hour = integerComponent(components.get(0));
        Integer minute = integerComponent(components.get(1));
        Integer second = integerComponent(components.get(2));
        Integer nano = components.size() == 4 ? integerComponent(components.get(3)) : 0;
        if (hour == null || minute == null || second == null || nano == null) {
            return originalValue;
        }
        return LocalTime.of(hour, minute, second, nano);
    }

    private static Integer integerComponent(Object value) {
        if (!(value instanceof Number number)) {
            return null;
        }
        long component = number.longValue();
        if (component < Integer.MIN_VALUE || component > Integer.MAX_VALUE) {
            return null;
        }
        return (int) component;
    }
}
