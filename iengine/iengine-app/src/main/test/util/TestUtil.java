package util;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

public class TestUtil {
    public static Object invokerPrivateMethod(Object clazzObj, String methodName, Class<?>[] paramsTypes, Object... params){
        try {
            Method privateMethod = clazzObj.getClass().getDeclaredMethod(methodName, paramsTypes);
            privateMethod.setAccessible(true);
            return privateMethod.invoke(clazzObj, params);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
}
