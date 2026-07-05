package io.tapdata.flow.engine.V2.node.duckdb;

import io.tapdata.entity.schema.TapField;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.Period;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

public class WideTableRowTypeNormalizer {
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");

    public static List<Map<String, Object>> normalizeRows(List<Map<String, Object>> rows, NodeSchemaInfo schemaInfo) {
        if (rows == null || rows.isEmpty() || schemaInfo == null) {
            return rows;
        }
        List<Map<String, Object>> normalized = new ArrayList<>(rows.size());
        for (Map<String, Object> row : rows) {
            if (row == null || row.isEmpty()) {
                normalized.add(row);
                continue;
            }
            Map<String, Object> copy = new LinkedHashMap<>(row.size());
            for (Map.Entry<String, Object> entry : row.entrySet()) {
                String fieldName = entry.getKey();
                Object value = entry.getValue();
                TapField tapField = schemaInfo.getField(fieldName);
                copy.put(fieldName, normalizeValue(value, tapField));
            }
            normalized.add(copy);
        }
        return normalized;
    }

    private static Object normalizeValue(Object value, TapField tapField) {
        if (value == null) {
            return null;
        }
        if (tapField == null) {
            return normalizeDuckDbDriverValue(value);
        }
        String type = tapField.getDataType();
        if (type == null || type.isBlank()) {
            return normalizeDuckDbDriverValue(value);
        }
        String baseType = normalizeType(type);
        if (isIntegerType(baseType)) {
            return toLong(value);
        }
        if (isFloatingType(baseType)) {
            return toDouble(value);
        }
        if (isDecimalType(baseType)) {
            return toBigDecimal(value);
        }
        if (isBooleanType(baseType)) {
            return toBoolean(value);
        }
        if (isStringType(baseType)) {
            return toStringValue(value);
        }
        if (isBinaryType(baseType)) {
            return toBytes(value);
        }
        if (isDateType(baseType)) {
            return toDateValue(value);
        }
        if (isTimeType(baseType)) {
            return toTimeValue(value);
        }
        if (isTimestampType(baseType)) {
            return toTimestampValue(value);
        }
        if (isJsonType(baseType)) {
            return toJsonValue(value);
        }
        if (isUuidType(baseType) || isBitType(baseType) || isIntervalType(baseType)) {
            return toStringValue(value);
        }
        if (isUnsupportedProductBoundaryType(baseType)) {
            // Product boundary: complex/nested DuckDB values are not part of the current wide-table contract.
            return toStringValue(value);
        }
        return normalizeDuckDbDriverValue(value);
    }

    private static Object normalizeDuckDbDriverValue(Object value) {
        if (value instanceof Blob || value instanceof ByteBuffer || value instanceof byte[] || value instanceof InputStream) {
            return toBytes(value);
        }
        if (value instanceof LocalDate localDate) {
            return java.sql.Date.valueOf(localDate);
        }
        if (value instanceof LocalTime localTime) {
            return TIME_FORMATTER.format(localTime);
        }
        if (value instanceof LocalDateTime || value instanceof OffsetDateTime
                || value instanceof ZonedDateTime || value instanceof Instant) {
            return toTimestampValue(value);
        }
        if (value instanceof UUID) {
            return value.toString();
        }
        String className = value.getClass().getName();
        if (className.startsWith("org.duckdb.")) {
            Object timestamp = invokeNoArg(value, "toSqlTimestamp");
            if (timestamp instanceof Timestamp) {
                return timestamp;
            }
            Object localDateTime = invokeNoArg(value, "toLocalDateTime");
            if (localDateTime instanceof LocalDateTime) {
                return Timestamp.valueOf((LocalDateTime) localDateTime);
            }
            Object offsetDateTime = invokeNoArg(value, "toOffsetDateTime");
            if (offsetDateTime instanceof OffsetDateTime) {
                return Timestamp.from(((OffsetDateTime) offsetDateTime).toInstant());
            }
            return value.toString();
        }
        return value;
    }

    private static Object invokeNoArg(Object value, String methodName) {
        try {
            return value.getClass().getMethod(methodName).invoke(value);
        } catch (Exception ignored) {
            return null;
        }
    }

    private static byte[] toBytes(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof byte[] bytes) {
            return bytes;
        }
        if (value instanceof ByteBuffer buffer) {
            ByteBuffer copy = buffer.slice();
            byte[] bytes = new byte[copy.remaining()];
            copy.get(bytes);
            return bytes;
        }
        if (value instanceof Blob blob) {
            try {
                long length = blob.length();
                if (length > Integer.MAX_VALUE) {
                    return readAllBytes(blob.getBinaryStream());
                }
                return blob.getBytes(1, (int) length);
            } catch (SQLException | IOException e) {
                try {
                    return readAllBytes(blob.getBinaryStream());
                } catch (Exception ignored) {
                    return value.toString().getBytes(StandardCharsets.UTF_8);
                }
            }
        }
        if (value instanceof InputStream inputStream) {
            try {
                return readAllBytes(inputStream);
            } catch (IOException ignored) {
                return null;
            }
        }
        if (value instanceof CharSequence chars) {
            byte[] parsed = parseHexBlob(chars.toString());
            return parsed != null ? parsed : chars.toString().getBytes(StandardCharsets.UTF_8);
        }
        return value.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] readAllBytes(InputStream inputStream) throws IOException {
        try (InputStream in = inputStream; ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[8192];
            int read;
            while ((read = in.read(buffer)) != -1) {
                out.write(buffer, 0, read);
            }
            return out.toByteArray();
        }
    }

    private static byte[] parseHexBlob(String raw) {
        if (raw == null) {
            return null;
        }
        String text = raw.trim();
        if (text.isEmpty()) {
            return new byte[0];
        }
        if ((text.startsWith("X'") || text.startsWith("x'")) && text.endsWith("'")) {
            text = text.substring(2, text.length() - 1);
        }
        if (text.startsWith("\\x") || text.startsWith("\\X")) {
            text = text.replace("\\x", "").replace("\\X", "");
        } else if ((text.startsWith("0x") || text.startsWith("0X")) && text.length() > 2) {
            text = text.substring(2);
        } else if ((text.startsWith("x") || text.startsWith("X")) && text.length() > 1 && isHex(text.substring(1))) {
            text = text.substring(1);
        }
        text = text.replaceAll("[^0-9A-Fa-f]", "");
        if (text.isEmpty() || (text.length() & 1) == 1 || !isHex(text)) {
            return null;
        }
        byte[] bytes = new byte[text.length() / 2];
        for (int i = 0; i < text.length(); i += 2) {
            bytes[i / 2] = (byte) Integer.parseInt(text.substring(i, i + 2), 16);
        }
        return bytes;
    }

    private static boolean isHex(String text) {
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F'))) {
                return false;
            }
        }
        return true;
    }

    private static Object toDateValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof java.sql.Date date) {
            return date;
        }
        if (value instanceof LocalDate localDate) {
            return java.sql.Date.valueOf(localDate);
        }
        if (value instanceof LocalDateTime localDateTime) {
            return java.sql.Date.valueOf(localDateTime.toLocalDate());
        }
        if (value instanceof OffsetDateTime offsetDateTime) {
            return java.sql.Date.valueOf(offsetDateTime.toLocalDate());
        }
        if (value instanceof ZonedDateTime zonedDateTime) {
            return java.sql.Date.valueOf(zonedDateTime.toLocalDate());
        }
        if (value instanceof Date date) {
            return new java.sql.Date(date.getTime());
        }
        LocalDate parsed = parseLocalDate(value.toString());
        return parsed == null ? value.toString() : java.sql.Date.valueOf(parsed);
    }

    private static Object toTimeValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof java.sql.Time time) {
            return TIME_FORMATTER.format(time.toLocalTime());
        }
        if (value instanceof LocalTime localTime) {
            return TIME_FORMATTER.format(localTime);
        }
        if (value instanceof LocalDateTime localDateTime) {
            return TIME_FORMATTER.format(localDateTime.toLocalTime());
        }
        if (value instanceof OffsetDateTime offsetDateTime) {
            return TIME_FORMATTER.format(offsetDateTime.toLocalTime());
        }
        if (value instanceof ZonedDateTime zonedDateTime) {
            return TIME_FORMATTER.format(zonedDateTime.toLocalTime());
        }
        if (value instanceof Duration duration) {
            long seconds = duration.getSeconds();
            long daySeconds = Math.floorMod(seconds, 24 * 60 * 60);
            return String.format(Locale.ROOT, "%02d:%02d:%02d",
                    daySeconds / 3600, (daySeconds % 3600) / 60, daySeconds % 60);
        }
        return value.toString();
    }

    private static Object toTimestampValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Timestamp timestamp) {
            return timestamp;
        }
        if (value instanceof Instant instant) {
            return Timestamp.from(instant);
        }
        if (value instanceof OffsetDateTime offsetDateTime) {
            return Timestamp.from(offsetDateTime.toInstant());
        }
        if (value instanceof ZonedDateTime zonedDateTime) {
            return Timestamp.from(zonedDateTime.toInstant());
        }
        if (value instanceof LocalDateTime localDateTime) {
            return Timestamp.valueOf(localDateTime);
        }
        if (value instanceof LocalDate localDate) {
            return Timestamp.valueOf(localDate.atStartOfDay());
        }
        if (value instanceof Date date) {
            return new Timestamp(date.getTime());
        }
        Timestamp parsed = parseTimestamp(value.toString());
        return parsed == null ? value.toString() : parsed;
    }

    private static LocalDate parseLocalDate(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        String text = raw.trim();
        if (text.length() >= 10) {
            text = text.substring(0, 10);
        }
        try {
            return LocalDate.parse(text);
        } catch (DateTimeParseException ignored) {
            return null;
        }
    }

    private static Timestamp parseTimestamp(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        String text = raw.trim().replace('T', ' ');
        try {
            return Timestamp.from(OffsetDateTime.parse(raw.trim().replace(' ', 'T')).toInstant());
        } catch (DateTimeParseException ignored) {
        }
        try {
            return Timestamp.valueOf(text.length() == 10 ? text + " 00:00:00" : text);
        } catch (IllegalArgumentException ignored) {
            return null;
        }
    }

    private static Object toJsonValue(Object value) {
        if (value == null || value instanceof Map || value instanceof Collection
                || value instanceof Number || value instanceof Boolean || value instanceof String) {
            return value;
        }
        return value.toString();
    }

    private static Object toStringValue(Object value) {
        if (value == null || value instanceof String) {
            return value;
        }
        if (value instanceof byte[] bytes) {
            return bytesToHex(bytes);
        }
        if (value instanceof ByteBuffer || value instanceof Blob || value instanceof InputStream) {
            byte[] bytes = toBytes(value);
            return bytes == null ? null : bytesToHex(bytes);
        }
        if (value instanceof Period period) {
            return period.toString();
        }
        if (value instanceof Duration duration) {
            long days = duration.toDays();
            if (duration.minusDays(days).isZero()) {
                return days + " days";
            }
            return duration.toString();
        }
        return value.toString();
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format(Locale.ROOT, "%02x", b & 0xFF));
        }
        return sb.toString();
    }

    private static String normalizeType(String raw) {
        String upper = raw.trim().toUpperCase(Locale.ROOT);
        int paren = upper.indexOf('(');
        if (paren > 0) {
            upper = upper.substring(0, paren);
        }
        int space = upper.indexOf(' ');
        if (space > 0) {
            upper = upper.substring(0, space);
        }
        return upper;
    }

    private static boolean isIntegerType(String type) {
        return "INT".equals(type) || "INTEGER".equals(type) || "BIGINT".equals(type) || "SMALLINT".equals(type) || "TINYINT".equals(type);
    }

    private static boolean isFloatingType(String type) {
        return "FLOAT".equals(type) || "DOUBLE".equals(type) || "REAL".equals(type);
    }

    private static boolean isDecimalType(String type) {
        return "DECIMAL".equals(type) || "NUMERIC".equals(type);
    }

    private static boolean isBooleanType(String type) {
        return "BOOLEAN".equals(type) || "BOOL".equals(type);
    }

    private static boolean isStringType(String type) {
        return "VARCHAR".equals(type) || "TEXT".equals(type) || "CHAR".equals(type)
                || "BPCHAR".equals(type) || "STRING".equals(type);
    }

    private static boolean isBinaryType(String type) {
        return "BLOB".equals(type) || "BYTEA".equals(type) || "BINARY".equals(type) || "VARBINARY".equals(type);
    }

    private static boolean isDateType(String type) {
        return "DATE".equals(type);
    }

    private static boolean isTimeType(String type) {
        return "TIME".equals(type);
    }

    private static boolean isTimestampType(String type) {
        return "TIMESTAMP".equals(type) || "DATETIME".equals(type)
                || "TIMESTAMPTZ".equals(type) || "TIMESTAMP_TZ".equals(type)
                || "TIMESTAMP_MS".equals(type) || "TIMESTAMP_NS".equals(type)
                || "TIMESTAMP_S".equals(type);
    }

    private static boolean isJsonType(String type) {
        return "JSON".equals(type);
    }

    private static boolean isUuidType(String type) {
        return "UUID".equals(type);
    }

    private static boolean isBitType(String type) {
        return "BIT".equals(type) || "BITSTRING".equals(type);
    }

    private static boolean isIntervalType(String type) {
        return "INTERVAL".equals(type);
    }

    private static boolean isUnsupportedProductBoundaryType(String type) {
        return "ARRAY".equals(type) || "LIST".equals(type) || "MAP".equals(type)
                || "STRUCT".equals(type) || "UNION".equals(type) || "ENUM".equals(type);
    }

    private static Long toLong(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Long l) {
            return l;
        }
        if (value instanceof Integer i) {
            return i.longValue();
        }
        if (value instanceof Short s) {
            return s.longValue();
        }
        if (value instanceof Byte b) {
            return b.longValue();
        }
        if (value instanceof BigInteger bi) {
            return bi.longValue();
        }
        if (value instanceof BigDecimal bd) {
            return bd.longValue();
        }
        if (value instanceof Number n) {
            return n.longValue();
        }
        if (value instanceof String s) {
            try {
                return new BigDecimal(s.trim()).longValue();
            } catch (Exception ignored) {
                return null;
            }
        }
        return null;
    }

    private static Double toDouble(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Double d) {
            return d;
        }
        if (value instanceof Float f) {
            return f.doubleValue();
        }
        if (value instanceof BigDecimal bd) {
            return bd.doubleValue();
        }
        if (value instanceof BigInteger bi) {
            return bi.doubleValue();
        }
        if (value instanceof Number n) {
            return n.doubleValue();
        }
        if (value instanceof String s) {
            try {
                return new BigDecimal(s.trim()).doubleValue();
            } catch (Exception ignored) {
                return null;
            }
        }
        return null;
    }

    private static BigDecimal toBigDecimal(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof BigDecimal bd) {
            return bd;
        }
        if (value instanceof BigInteger bi) {
            return new BigDecimal(bi);
        }
        if (value instanceof Number n) {
            return new BigDecimal(n.toString());
        }
        if (value instanceof String s) {
            try {
                return new BigDecimal(s.trim());
            } catch (Exception ignored) {
                return null;
            }
        }
        return null;
    }

    private static Boolean toBoolean(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Boolean b) {
            return b;
        }
        if (value instanceof Number n) {
            return n.intValue() != 0;
        }
        if (value instanceof String s) {
            String trimmed = s.trim().toLowerCase(Locale.ROOT);
            if ("true".equals(trimmed) || "1".equals(trimmed) || "yes".equals(trimmed) || "y".equals(trimmed)) {
                return true;
            }
            if ("false".equals(trimmed) || "0".equals(trimmed) || "no".equals(trimmed) || "n".equals(trimmed)) {
                return false;
            }
        }
        return null;
    }
}
