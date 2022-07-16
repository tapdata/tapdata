package io.tapdata.inspect.compare;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.tapdata.constant.JSONUtil;
import com.tapdata.entity.MysqlJson;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.types.ObjectId;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/8/18 1:10 下午
 * @description
 */
public class DefaultCompare implements CompareFunction<Map<String, Object>, String> {

    private List<String> columns;
    private Logger logger = LogManager.getLogger(DefaultCompare.class);

    public DefaultCompare() {
    }

    @Override
    public String apply(Map<String, Object> t1, Map<String, Object> t2, String sourceId, String targetId) {
        if (t1 == null && t2 == null) {
            return null;
        }
        if (t1 == null) {
            return "Source record is null";
        } else if (t2 == null) {
            return "Target record is null";
        }

        Set<String> comparedFields = new HashSet<>();
        // compare base on t1's columns
        columns = t1.keySet().stream().sorted().collect(Collectors.toList());
        Set<String> differentFields = columns.parallelStream().map(key -> {
            Object val1 = t1.get(key);
            Object val2 = t2.get(key);

            comparedFields.add(key);
            return compare(val1, val2) ? key : null;
        }).filter(Objects::nonNull).collect(Collectors.toSet());

        if (differentFields.size() != 0) {
            if (logger.isDebugEnabled()) {
                int maxLength = differentFields.stream().map(String::length).max(Comparator.comparingInt(r -> r)).orElse(20);
                String msg = differentFields.stream().map(field -> {
                    Object val1 = t1.get(field);
                    Object val2 = t2.get(field);
                    String msg1 = val1 == null ? "null" : (val1 + " (" + val1.getClass().getTypeName() + ")");
                    String msg2 = val2 == null ? "null" : (val2 + " (" + val2.getClass().getTypeName() + ")");
                    return appendSpace(field, maxLength) + ": " + appendSpace(msg1, 50) + " ->     " + msg2;
                }).collect(Collectors.joining("\n ", "\n ", "\n"));
                logger.debug(sourceId + " -> " + targetId + ": " + msg);
//      } else {
//        logger.info(sourceId + " -> " + targetId + ": " + String.join(", ", differentFields));
            }
        }

        if (differentFields.isEmpty()) {
            return null;
        } else {
            return "Different fields:" + String.join(",", differentFields);
        }
    }

    public String appendSpace(String str, int length) {
        StringBuilder sb = new StringBuilder(str == null ? "" : str);
        while (sb.length() < length) {
            sb.append(' ');
        }
        sb.append(' ');
        return sb.toString();
    }

    // true: error   false: ok
    private boolean compare(Object val1, Object val2) {
        try {
            if (val1 == null && val2 == null) return false;
            if (val1 == null || val2 == null) return true;

            if (val1 instanceof MysqlJson) return compare((MysqlJson) val1, val2);
            if (val2 instanceof MysqlJson) return compare((MysqlJson) val2, val1);
            if (val1 instanceof Map) return compare((Map) val1, val2);
            if (val2 instanceof Map) return compare((Map) val2, val1);
            if (val1 instanceof Collection) return compare((Collection) val1, val2);
            if (val2 instanceof Collection) return compare((Collection) val2, val1);

            val1 = try2String(val1);
            val2 = try2String(val2);

            if (val1 instanceof String || val2 instanceof String) {
                val1 = val1.toString().trim();
                val2 = val2.toString().trim();
            } else if (val1 instanceof Byte || val2 instanceof Byte
                || val1 instanceof Short || val2 instanceof Short
                || val1 instanceof Integer || val2 instanceof Integer
                || val1 instanceof Long || val2 instanceof Long) {
                val1 = new BigDecimal(val1.toString()).longValue();
                val2 = new BigDecimal(val2.toString()).longValue();
            } else if (val1 instanceof Float || val2 instanceof Float
                || val1 instanceof Double || val2 instanceof Double
                || val1 instanceof BigDecimal || val2 instanceof BigDecimal) {
                val1 = new BigDecimal(val1.toString()).doubleValue();
                val2 = new BigDecimal(val2.toString()).doubleValue();
            }

            return !val1.equals(val2);
        } catch (Throwable e) {
            e.printStackTrace();
        }
        return true;
    }

    private boolean compare(Map val, Object obj) {
        if (obj instanceof Map) {
            if (val.size() != ((Map) obj).size()) return true;
            Map objVal = (Map) obj;
            Object v1, v2;
            for (Object k : val.keySet()) {
                v1 = val.get(k);
                v2 = objVal.get(k);
                if (compare(v1, v2)) return true;
            }
            return false;
        } else if (obj instanceof String) {
            return obj2JsonCompare(val, obj);
        }
        return true;
    }

    private boolean compare(Collection val, Object obj) {
        Object v1, v2;
        if (obj instanceof Collection) {
            if (val.size() != ((Collection) obj).size()) return true;
            Iterator i1 = val.iterator(), i2 = ((Collection) obj).iterator();
            for (int i = 0, len = val.size(); i < len; i++) {
                v1 = i1.next();
                v2 = i2.next();
                if (compare(v1, v2)) return true;
            }
            return false;
        } else if (obj instanceof Array) {
            if (val.size() != Array.getLength(obj)) return true;
            Iterator i1 = val.iterator();
            for (int i = 0, len = val.size(); i < len; i++) {
                v1 = i1.next();
                v2 = Array.get(obj, i);
                if (compare(v1, v2)) return true;
            }
            return false;
        } else if (obj instanceof String) {
            return obj2JsonCompare(val, obj);
        }
        return true;
    }

    private boolean compareArray(Object val, Object obj) {
        int len = Array.getLength(val);
        Object v1, v2;
        if (obj instanceof Collection) {
            if (len != ((Collection) obj).size()) return true;
            Iterator i2 = ((Collection) obj).iterator();
            for (int i = 0; i < len; i++) {
                v1 = Array.get(val, i);
                v2 = i2.next();
                if (compare(v1, v2)) return true;
            }
            return false;
        } else if (obj instanceof Array) {
            if (len != Array.getLength(obj)) return true;
            for (int i = 0; i < len; i++) {
                v1 = Array.get(val, i);
                v2 = Array.get(obj, i);
                if (compare(v1, v2)) return true;
            }
            return false;
        } else if (obj instanceof String) {
            return obj2JsonCompare(val, obj);
        }
        return true;
    }

    private boolean compare(MysqlJson val, Object obj) {
        try {
            if (obj instanceof Map) {
                return compare((Map) obj, val.toObject());
            } else if (obj instanceof Collection) {
                return compare((Collection) obj, val.toObject());
            } else if (obj instanceof Array) {
                return compareArray(obj, val.toObject());
            }
            return !val.getData().equals(obj.toString());
        } catch (Exception e) {
            return true;
        }
    }

    private boolean obj2JsonCompare(Object val, Object obj) {
        try {
            JSONUtil.disableFeature(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
            return !JSONUtil.obj2Json(val).equals(obj.toString());
        } catch (JsonProcessingException e) {
            return true;
        } finally {
            JSONUtil.enableFeature(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        }
    }

    private Object try2String(Object val) {
        if (val instanceof ObjectId) {
            return ((ObjectId) val).toHexString();
        } else if (val instanceof byte[]) {
            return new String((byte[]) val, StandardCharsets.UTF_8);
        } else if (val instanceof Date) {
            return ((Date) val).toInstant().toString();
        } else if (val instanceof Instant) {
            return ((Instant) val).toString();
        }
        return val;
    }
}
