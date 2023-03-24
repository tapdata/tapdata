package io.tapdata.supervisor.convert.entity;

import java.util.*;

public class WZTags {
    protected static final String W_IGNORE = "ignore";

    public static final String W_CLASS = "classConfigurations";
    protected static final String W_TARGET = "target";
    protected static final String W_METHOD = "method";
    protected static final String W_CONSTRUCTOR = "constructor";

    protected static final String W_TYPE = "type";
    protected static final String W_PATH = "path";
    protected static final String W_SCAN_PACKAGE = "scanPackage";
    protected static final String W_IS_CREATE = "isCreate";
    protected static final String W_SAVE_TO = "saveTo";
    protected static final String W_RETURN_TYPE = "returnType";
    protected static final String W_JAR_FILE_PATH = "jarPath";

    protected static final String W_NAME = "name";
    protected static final String W_CODE = "code";
    protected static final String CREATE_WITH = "createWith";
    protected static final String W_LINE = "line";
    protected static final String W_LINE_INDEX = "index";
    protected static final String W_IS_FINALLY = "isFinally";
    protected static final String W_IS_APPEND = "isAppend";
    protected static final String W_IS_REDUNDANT = "isRedundant";
    protected static final String W_ARGS = "args";

    public final static String CODE_LINE_TYPE_AFTER = "after";
    public final static String CODE_LINE_TYPE_BEFORE = "before";
    public final static String CODE_LINE_TYPE_CATCH = "catch";
    public final static String CODE_LINE_TYPE_NORMAL = "normal";
    public final static String CODE_EXCEPTION = "exception";

    public final static String DEFAULT_THROWABLE = "java.lang.Throwable";
    public final static String DEFAULT_THROWABLE_NAME = "$e";
    public final static String DEFAULT_EMPTY = "";
    public final static String DEFAULT_VOID = "void";
    public final static int DEFAULT_ONE = 1;
    public final static int DEFAULT_ZERO = 0;

    public final static String TYPE_EXTENDS = "extends";
    public final static String TYPE_NAME = "name";
    public final static String TYPE_PATH = "path";

    public final static String CODE_BEFORE = "before";
    public final static String CODE_AFTER = "after";
    public final static String CODE_CATCH = "catch";
    public final static String CODE_NORMAL = "normal";


    public static Map<String, Object> toMap(Map<String, Object> map, String key) {
        Object mapObj = Optional.ofNullable(map.get(key)).orElse(Collections.EMPTY_MAP);
        if (mapObj instanceof Map) {
            return (Map<String, Object>) mapObj;
        } else {
            return Collections.EMPTY_MAP;
        }
    }

    public static Collection<?> toList(Map<String, Object> map, String key) {
        Object obj = Optional.ofNullable(map.get(key)).orElse(Collections.emptyList());
        if (obj instanceof Collection) {
            return (Collection<?>) obj;
        } else {
            return Collections.emptyList();
        }
    }

    public static Object toObject(Map<String, Object> map, String key, Object defaultValue) {
        return Optional.ofNullable(map.get(key)).orElse(defaultValue);
    }

    public static Object[] toArray(Map<String, Object> map, String key) {
        Object obj = Optional.ofNullable(map.get(key)).orElse(new Object[0]);
        if (obj.getClass().isArray()) {
            return (Objects[]) obj;
        } else {
            return new Object[0];
        }
    }

    public static String toString(Map<String, Object> map, String key, String defaultValue) {
        Object obj = Optional.ofNullable(map.get(key)).orElse(defaultValue);
        if (obj instanceof String) {
            return (String) obj;
        } else {
            return defaultValue;
        }
    }

    public static int toInt(Map<String, Object> map, String key, int defaultValue) {
        Object obj = Optional.ofNullable(map.get(key)).orElse(Collections.EMPTY_MAP);
        if (obj instanceof Number) {
            return ((Number) obj).intValue();
        } else if (obj instanceof String) {
            try {
                return Integer.parseInt(((String) obj).trim());
            } catch (Exception e) {
                return 0;
            }
        } else {
            return defaultValue;
        }
    }

    public static double toDouble(Map<String, Object> map, String key, double defaultValue) {
        Object obj = Optional.ofNullable(map.get(key)).orElse(Collections.EMPTY_MAP);
        if (obj instanceof Number) {
            return ((Number) obj).doubleValue();
        } else if (obj instanceof String) {
            try {
                return Double.parseDouble(((String) obj).trim());
            } catch (Exception e) {
                return defaultValue;
            }
        } else {
            return defaultValue;
        }
    }

    public static double toFloat(Map<String, Object> map, String key, float defaultValue) {
        Object obj = Optional.ofNullable(map.get(key)).orElse(Collections.EMPTY_MAP);
        if (obj instanceof Number) {
            return ((Number) obj).floatValue();
        } else if (obj instanceof String) {
            try {
                return Float.parseFloat(((String) obj).trim());
            } catch (Exception e) {
                return defaultValue;
            }
        } else {
            return defaultValue;
        }
    }

    public static double toByte(Map<String, Object> map, String key, byte defaultValue) {
        Object obj = Optional.ofNullable(map.get(key)).orElse(Collections.EMPTY_MAP);
        if (obj instanceof Number) {
            return ((Number) obj).byteValue();
        } else if (obj instanceof String) {
            try {
                return Byte.parseByte(((String) obj).trim());
            } catch (Exception e) {
                return defaultValue;
            }
        } else {
            return defaultValue;
        }
    }

    public static double toShort(Map<String, Object> map, String key, short defaultValue) {
        Object obj = Optional.ofNullable(map.get(key)).orElse(Collections.EMPTY_MAP);
        if (obj instanceof Number) {
            return ((Number) obj).shortValue();
        } else if (obj instanceof String) {
            try {
                return Short.parseShort(((String) obj).trim());
            } catch (Exception e) {
                return defaultValue;
            }
        } else {
            return defaultValue;
        }
    }

    public static double toChar(Map<String, Object> map, String key, char defaultValue) {
        Object obj = Optional.ofNullable(map.get(key)).orElse(Collections.EMPTY_MAP);
        if (obj instanceof Number) {
            return (char) ((Number) obj).intValue();
        } else if (obj instanceof String) {
            try {
                return (((String) obj).trim()).charAt(0);
            } catch (Exception e) {
                return defaultValue;
            }
        } else {
            return defaultValue;
        }
    }

    public static double toLong(Map<String, Object> map, String key, long defaultValue) {
        Object obj = Optional.ofNullable(map.get(key)).orElse(Collections.EMPTY_MAP);
        if (obj instanceof Number) {
            return (char) ((Number) obj).doubleValue();
        } else if (obj instanceof String) {
            try {
                return Long.parseLong(((String) obj).trim());
            } catch (Exception e) {
                return defaultValue;
            }
        } else {
            return defaultValue;
        }
    }

    public static boolean toBoolean(Map<String, Object> map, String key, boolean defaultValue) {
        Object obj = Optional.ofNullable(map.get(key)).orElse(Collections.EMPTY_MAP);
        if (obj instanceof Boolean) {
            return (Boolean) (Optional.ofNullable(obj).orElse(defaultValue));
        } else if (obj instanceof String) {
            try {
                return Boolean.parseBoolean(((String) obj).trim());
            } catch (Exception e) {
                return defaultValue;
            }
        } else {
            return defaultValue;
        }
    }
}
