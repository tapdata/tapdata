package com.tapdata.tm.utils;

import java.util.*;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections.SetUtils;
import org.springframework.util.StringUtils;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/10/12 3:40 下午
 * @description
 */
public class MapUtils {

    public static String getAsString(Map<String, Object> data, String fieldName) {
        if (data != null && data.containsKey(fieldName)) {
            Object value = data.get(fieldName);
            if (value != null) {
                return (String) value;
            }
        }
        return null;
    }

    public static List getAsList(Map<String, Object> data, String fieldName) {
        if (data != null && data.containsKey(fieldName)) {
            Object value = data.get(fieldName);
            if (value != null) {
                return (List) value;
            }
        }
        return null;
    }

    public static Map getAsMap(Map<String, Object> data, String fieldName) {
        if (data != null && data.containsKey(fieldName)) {
            Object value = data.get(fieldName);
            if (value != null) {
                return (Map) value;
            }
        }
        return null;
    }

    public static Long getAsLong(Map<String, Object> data, String fieldName) {
        if (data != null && data.containsKey(fieldName)) {
            Object value = data.get(fieldName);
            if (value instanceof Integer) {
                return Long.valueOf((Integer) value);
            } else if (value instanceof Double) {
                return ((Double) value).longValue();
            } else if (value instanceof Long) {
                return (Long) value;
            }
        }
        return null;
    }

    public static Boolean getAsBoolean(Map<String, Object> data, String fieldName) {
        if (data != null && data.containsKey(fieldName)) {
            Object value = data.get(fieldName);
            if (value != null) {
                return (Boolean) value;
            }
        }
        return null;
    }

    public static Object getValueByPatchPath(Object map, String key) {
        Object value = map;
        String[] split = key.split("/");
        for (String keyStr : split) {
            if (!StringUtils.hasLength(keyStr)) {
                continue;
            }
            if (value instanceof Map) {
                value = ((Map) value).get(keyStr);
            } else {
                return null;
            }
        }
        return value;

    }

    public static String getAsStringByPath(Object map, String key) {
        Object value = getValueByPatchPath(map, key);
        return value != null ? value.toString() : null;
    }


    public static boolean IncludeKey(Object obj, String... keys) {
        if (obj instanceof Map) {
            return Arrays.stream(keys).anyMatch(((Map) obj)::containsKey);
        }
        return false;
    }

    public static boolean isEmpty(final Map<?, ?> map) {
        return map == null || map.isEmpty();
    }

    public static boolean isNotEmpty(final Map<?, ?> map) {
        return !isEmpty(map);
    }

    public static void increase(Map<String, Long> map, String key) {
        if (!map.containsKey(key)) {
            map.put(key, 1L);
        } else {
            Long value = map.get(key);
            value++;
            map.put(key, value);
        }
    }

    public static List<Object> packageDelOption() {
        // 补充逻辑删除字段 {$or :[{"is_deleted":{$exists: false }},{"is_deleted":false}]}
        Map<String, Object> delMap = Maps.newHashMap();
        Map<String, Object> delExistMap = Maps.newHashMap();
        delExistMap.put("$exists", false);

        delMap.put("is_deleted", delExistMap);
        Map<String, Object> delFalseMap = Maps.newHashMap();
        delFalseMap.put("is_deleted", false);

        return Lists.newArrayList(delMap, delFalseMap);
    }




}
