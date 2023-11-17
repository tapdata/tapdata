package io.tapdata.util;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

public class TestUtil {
    public static Object invokerPrivateMethod(Class<?> clazz, Object clazzObj, String methodName, Class<?>[] paramsTypes, Object... params){
        try {
            Method privateMethod = clazz.getDeclaredMethod(methodName, paramsTypes);
            privateMethod.setAccessible(true);
            return privateMethod.invoke(clazzObj, params);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    public static Object invokerStaticPrivateMethod(Class<?> clazzObj, String methodName, Class<?>[] paramsTypes, Object... params) {
        return invokerPrivateMethod(clazzObj, null, methodName, paramsTypes, params);
    }
    public static Object invokerPrivateMethod(Object clazzObj, String methodName, Class<?>[] paramsTypes, Object... params) {
        return invokerPrivateMethod(clazzObj.getClass(), clazzObj, methodName, paramsTypes, params);
    }
    public static <T>T[] getArray(T ... classes) {
        return classes;
    }

    public static Object getStaticField(Class<?> clazz, String filedName) {
        try {
            Field modifiersField = clazz.getDeclaredField(filedName);
            modifiersField.setAccessible(true);
            return modifiersField.get(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Object setAndGetPrivateField(Object obj, Class<?> clazz, String filedName, Object newValue) {
        try {
            Field modifiersField = clazz.getDeclaredField(filedName);
            modifiersField.setAccessible(true);
            modifiersField.set(obj, newValue);
            return modifiersField.get(obj);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Object setAndGetPrivateFinalStaticField(Object obj, Class<?> clazz, String filedName, Object newValue) {
        try {
            Field modifiersField = clazz.getDeclaredField(filedName);
            modifiersField.setAccessible(true);

            Field modifiers = modifiersField.getClass().getDeclaredField("modifiers");
            modifiers.setAccessible(true);
            modifiers.setInt(modifiersField, modifiersField.getModifiers() & ~Modifier.FINAL);
            modifiersField.set(obj, newValue);
            return modifiersField.get(obj);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
