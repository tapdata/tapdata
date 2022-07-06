package io.tapdata.entity.utils;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class InstanceFactory {
    private static Map<Class<?>, Object> beanMap = new ConcurrentHashMap<>();
    private static Map<Class<?>, Object> instanceMap = new ConcurrentHashMap<>();
    private static Map<String, Object> instanceTypeMap = new ConcurrentHashMap<>();
    private InstanceFactory() {}
    public static <T> T bean(Class<T> beanClass) {
        //noinspection unchecked
        return (T) beanMap.computeIfAbsent(beanClass, aClass -> {
            try {
                return beanClass.getConstructor().newInstance();
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException |
                     NoSuchMethodException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public static <T> T instance(Class<T> instanceClass) {
        //noinspection unchecked
        return (T) instanceMap.computeIfAbsent(instanceClass, aClass -> ClassFactory.create(instanceClass));
    }

    public static <T> T instance(Class<T> instanceClass, String type) {
        String key = instanceClass.getName() + "#" + type;
        //noinspection unchecked
        return (T) instanceTypeMap.computeIfAbsent(key, theKey -> ClassFactory.create(instanceClass, type));
    }
}
