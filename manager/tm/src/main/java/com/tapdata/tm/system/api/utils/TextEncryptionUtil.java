package com.tapdata.tm.system.api.utils;

import com.alibaba.fastjson.JSON;
import com.tapdata.tm.system.api.dto.TextEncryptionRuleDto;
import com.tapdata.tm.system.api.enums.OutputType;
import com.tapdata.tm.system.api.vo.DebugVo;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/8/12 11:18 Create
 * @description
 */
@Slf4j
public class TextEncryptionUtil {
    public static final List<String> SYSTEM_FIELDS = List.of("limit", "page", "filter.fields", "filter.sort", "filter.order", "sort", "order");
    public static final String PARAM_REPLACE_CHAR = "******";

    private TextEncryptionUtil() {
    }

    public static List<Map<String, Object>> textEncryptionBySwitch(Boolean open, List<Map<String, Object>> data) {
        if (!Boolean.TRUE.equals(open) || null == data || data.isEmpty()) {
            return data;
        }
        data.forEach(TextEncryptionUtil::textEncryptionBySwitch);
        return data;
    }

    static Map<String, Object> textEncryptionBySwitch(Map<String, Object> data) {
        final List<String> keys = new ArrayList<>(data.keySet());
        for (String key : keys) {
            if (SYSTEM_FIELDS.contains(key)) {
                continue;
            }
            handleFilter(key, data);
            Object value = data.get(key);
            if (value instanceof String || value instanceof Character) {
                data.put(key, parseAndEncryptionJson(key, value));
            } else if (value instanceof Map) {
                data.put(key, textEncryptionBySwitchOnce(key, value));
            } else if (value instanceof Collection<?>) {
                List<Object> after = new ArrayList<>();
                for (Object item : ((Collection<?>) value)) {
                    after.add(textEncryptionBySwitchOnce(key, item));
                }
                data.put(key, after);
            } else {
                data.put(key, PARAM_REPLACE_CHAR);
            }
        }
        return data;
    }

    protected static void handleFilter(String key, Map<String, Object> data) {
        if ("filter".equals(key) && data.get(key) instanceof String json) {
            try {
                data.put(key, JSON.parseObject(json, Map.class));
            } catch (Exception e) {
                log.warn("filter not a json", e);
            }
        }
    }

    static Object textEncryptionBySwitchOnce(String parent, Object value) {
        if (null == value || SYSTEM_FIELDS.contains(parent)) {
            return value;
        }
        if (value instanceof String || value instanceof Character) {
            return parseAndEncryptionJson(parent, value);
        } else if (value instanceof Map) {
            ((Map<String, Object>) value).forEach((k, v) -> ((Map<String, Object>) value).put(k, textEncryptionBySwitchOnce(parent + "." + k, v)));
            return value;
        } else if (value instanceof Collection<?>) {
            List<Object> newOne = new ArrayList<>();
            for (Object item : ((Collection<?>) value)) {
                newOne.add(textEncryptionBySwitchOnce(parent, item));
            }
            return newOne;
        }
        return PARAM_REPLACE_CHAR;
    }

    static Object parseAndEncryptionJson(String parent, Object jsonItem) {
        if (jsonItem instanceof Character) {
            return PARAM_REPLACE_CHAR;
        }
        try {
            return textEncryptionBySwitchOnce(parent, JSON.parseObject(String.valueOf(jsonItem)));
        } catch (Exception e) {
            log.warn("unable to parse json, return encryption chars as string. json: {}, msg: {}", parent, e.getMessage());
            return PARAM_REPLACE_CHAR;
        }
    }

    public static DebugVo map(Map<String, List<TextEncryptionRuleDto>> config, DebugVo debugVo) {
        if (null == debugVo) {
            return null;
        }
        List<Map<String, Object>> data = debugVo.getData();
        map(config, data);
        return debugVo;
    }

    public static List<Map<String, Object>> map(Map<String, List<TextEncryptionRuleDto>> config, List<Map<String, Object>> data) {
        if (null == config || config.isEmpty()
                || null == data || data.isEmpty()) {
            return data;
        }
        config.keySet().forEach(fieldName -> data.forEach(item -> deepSearch(fieldName.split("\\."), item, config.get(fieldName))));
        return data;
    }

    static void deepSearch(String[] fieldPath, Object value, List<TextEncryptionRuleDto> config) {
        if (null == value || null == config || config.isEmpty()) {
            return;
        }
        List<Map<String, Object>> results = new ArrayList<>();
        findRecursive(value, null, fieldPath, 0, results);
        for (Map<String, Object> result : results) {
            Object itemObject = result.get("value");
            String key = (String) result.get("key");
            if (result.get("parent") instanceof Map parent) {
                Object mappedValue = mapFieldValue(config, itemObject);
                if (null == mappedValue) {
                    continue;
                }
                ((Map<String, Object>) parent).put(key, mappedValue);
            }
        }
    }

    static void findRecursive(Object current,
                              Object parent,
                              String[] fields,
                              int index,
                              List<Map<String, Object>> results) {
        if (current == null) {
            return;
        }
        if (index == fields.length) {
            Map<String, Object> match = new HashMap<>();
            match.put("parent", parent);
            match.put("value", current);
            match.put("key", fields[fields.length - 1]);
            results.add(match);
            return;
        }
        String key = fields[index];
        if (current instanceof Map<?, ?> cMap) {
            Object next = cMap.get(key);
            findRecursive(next, cMap, fields, index + 1, results);
        } else if (current instanceof List<?> list) {
            for (Object element : list) {
                findRecursive(element, parent, fields, index, results);
            }
        }
    }

    static Object mapFieldValue(List<TextEncryptionRuleDto> textEncryptionRules, Object value) {
        // 替换
        if (null == value) {
            return null;
        }
        String target;
        int type;
        if (value instanceof String || value instanceof Character) {
            target = String.valueOf(value);
            type = 0;
        } else if (value instanceof Map map) {
            List<String> keys = new ArrayList<>(map.keySet());
            Map<String, Object> targetMap = new HashMap<>();
            for (String key : keys) {
                targetMap.put(key, mapFieldValue(textEncryptionRules, map.get(key)));
            }
            return targetMap;
        } else if (value instanceof Collection<?> list) {
            List<Object> targetList = new ArrayList<>();
            for (Object item : list) {
                targetList.add(mapFieldValue(textEncryptionRules, item));
            }
            return targetList;
        } else if (value.getClass().isArray()) {
            Object[] objects = new Object[((Object[]) value).length];
            for (int index = 0; index < ((Object[]) value).length; index++) {
                objects[index] = mapFieldValue(textEncryptionRules, ((Object[]) value)[index]);
            }
            return objects;
        } else if (value instanceof Number || value instanceof Boolean) {
            target = value.toString();
            type = value instanceof Boolean ? 20 : 21;
        } else if (value instanceof java.util.Date) {
            target = value.toString();
            type = 30;
        } else {
            log.info("Unsupported type: {}, can not be encrypted.", value.getClass().getName());
            return value;
        }
        for (TextEncryptionRuleDto rule : textEncryptionRules) {
            target = replace(rule, target);
        }
        return switch (type) {
            case 21 -> tryBackNumber(target, (Number) value);
            case 0, 20, 30 -> target;
            default -> value;
        };
    }

    interface Cover {
        Object back(String value);

        static Object call(String value, String tag, Cover cover) {
            try {
                return cover.back(value);
            } catch (Exception e) {
                log.debug("unable to convert string to {}, return as string. encrypted value:{}, msg: {}", tag, value, e.getMessage());
                return value;
            }
        }
    }

    static Object tryBackNumber(String value, Number originValue) {
        if (originValue instanceof Byte) {
            return Cover.call(value, "byte", Byte::parseByte);
        } else if (originValue instanceof Short) {
            return Cover.call(value, "short", Short::parseShort);
        } else if (originValue instanceof Integer) {
            return Cover.call(value, "integer", Integer::parseInt);
        } else if (originValue instanceof Long) {
            return Cover.call(value, "long", Long::parseLong);
        } else if (originValue instanceof Float) {
            return Cover.call(value, "float", Float::parseFloat);
        } else if (originValue instanceof Double) {
            return Cover.call(value, "double", Double::parseDouble);
        } else if (originValue instanceof BigDecimal) {
            return Cover.call(value, "big-decimal", BigDecimal::new);
        } else if (originValue instanceof BigInteger) {
            return Cover.call(value, "big-integer", e -> BigInteger.valueOf(Long.parseLong(e)));
        } else {
            return value;
        }
    }

    static String replace(TextEncryptionRuleDto rule, String text) {
        OutputType outputType = OutputType.by(rule.getOutputType());
        String regex = rule.getRegex();
        String outputChar = rule.getOutputChar();
        return switch (outputType) {
            case CUSTOM -> text.replaceAll(regex, String.valueOf(outputChar).repeat(rule.getOutputCount() <= 0 ? 1 : rule.getOutputCount()));
            case AUTO -> Pattern.compile(regex)
                    .matcher(text)
                    .replaceAll(m -> outputChar.repeat(m.group().length()));
        };
    }
}
