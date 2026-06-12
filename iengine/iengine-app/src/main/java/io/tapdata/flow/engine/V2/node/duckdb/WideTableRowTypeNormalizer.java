package io.tapdata.flow.engine.V2.node.duckdb;

import io.tapdata.entity.schema.TapField;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class WideTableRowTypeNormalizer {
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
        if (value == null || tapField == null) {
            return value;
        }
        String type = tapField.getDataType();
        if (type == null || type.isBlank()) {
            return value;
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
        return value;
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
