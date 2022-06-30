package io.tapdata.entity.utils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class InstanceFactory {
    private static Map<Class<?>, Object> instanceMap = new ConcurrentHashMap<>();
    private static Map<String, Object> instanceTypeMap = new ConcurrentHashMap<>();
    private InstanceFactory() {}
    public static <T> T instance(Class<T> instanceClass) {
        return (T) instanceMap.computeIfAbsent(instanceClass, aClass -> ClassFactory.create(instanceClass));
    }

    public static <T> T instance(Class<T> instanceClass, String type) {
        String key = instanceClass.getName() + "#" + type;
        return (T) instanceTypeMap.computeIfAbsent(key, theKey -> ClassFactory.create(instanceClass, type));
    }
}
