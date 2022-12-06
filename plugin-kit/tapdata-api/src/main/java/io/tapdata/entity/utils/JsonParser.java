package io.tapdata.entity.utils;

import java.lang.reflect.Type;
import java.util.List;

public interface JsonParser {
    JsonParser configGlobalAbstractClassDetectors(Class<?> type, List<AbstractClassDetector> abstractClassDetectors);
    
    String toJson(Object obj, ToJsonFeature... features);

    byte[] toJsonBytes(Object obj, ToJsonFeature... features);
    <T> T fromJsonBytes(byte[] jsonBytes, Class<T> clazz);

    DataMap fromJsonObject(String json);
    List<?> fromJsonArray(String json);
    Object fromJson(String json);

    <T> T fromJson(String json, Type clazz);
    <T> T fromJson(String json, Class<T> clazz);

    @Deprecated
    <T> T fromJson(String json, Class<T> clazz, List<AbstractClassDetector> abstractClassDetectors);

    <T> T fromJson(String json, TypeHolder<T> typeHolder);

    @Deprecated
    <T> T fromJson(String json, TypeHolder<T> typeHolder, List<AbstractClassDetector> abstractClassDetectors);

    String toJsonWithClass(Object obj);

    Object fromJsonWithClass(String json);

    Object fromJsonWithClass(String json, ClassLoader classLoader);

    enum ToJsonFeature {
        PrettyFormat,
        WriteMapNullValue;
    }


    class AbstractClassDetector {
        public static AbstractClassDetector create() {
            return new AbstractClassDetector();
        }

        private String key;

        public AbstractClassDetector key(String key) {
            this.key = key;
            return this;
        }

        private Object value;

        public AbstractClassDetector value(Object value) {
            this.value = value;
            return this;
        }

        private Class<?> deserializeClass;

        public AbstractClassDetector deserializeClass(Class<?> deserializeClass) {
            this.deserializeClass = deserializeClass;
            return this;
        }

        public String matchingString() {
            StringBuilder builder = new StringBuilder().append("\"").append(key).append("\"").append(":");
            if (value instanceof Number) {
                builder.append(value);
            } else {
                builder.append("\"").append(value.toString()).append("\"");
            }
            return builder.toString();
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public Object getValue() {
            return value;
        }

        public void setValue(Object value) {
            this.value = value;
        }

        public Class<?> getDeserializeClass() {
            return deserializeClass;
        }

        public void setDeserializeClass(Class<?> deserializeClass) {
            this.deserializeClass = deserializeClass;
        }

        public boolean verify() {
            return deserializeClass != null && value != null && key != null;
        }
    }
}
