package io.tapdata.entity.codec;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class CodecUtils {
    public static boolean canBeInitiated(Class<?> clazz) {
        if(clazz == null)
            return false;
        if (isPrimitiveOrWrapper(clazz))
            return true;
        if (clazz.isArray()) {
            return canBeInitiated(clazz.getComponentType());
        }
        if (clazz.isInterface()) {
            return false;
        }
        try {
            clazz.getConstructor();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public static Class<?> getInitiatableClass(Class<?> clazz) {
        Class<?> theClass = null;
        if (!canBeInitiated(clazz)) {
            if (Collection.class.isAssignableFrom(clazz)) {
                theClass = ArrayList.class;
            } else if (Map.class.isAssignableFrom(clazz)) {
                theClass = HashMap.class;
            }
        } else {
            theClass = clazz;
        }

        if (theClass != null) {
//            System.out.println("Convert class from " + clazz + " to " + theClass);
            return theClass;
        }
        return clazz;
    }

    public static boolean isPrimitiveWrapper(Class<?> clazz) {
        if(clazz == null)
            return false;
        return primitiveWrapperTypeMap.containsKey(clazz);
    }

    public static boolean isPrimitiveOrWrapper(Class<?> clazz) {
        if(clazz == null)
            return false;
        return clazz.isPrimitive() || isPrimitiveWrapper(clazz);
    }

    public static boolean isPrimitiveArray(Class<?> clazz) {
        if(clazz == null)
            return false;
        return clazz.isArray() && clazz.getComponentType().isPrimitive();
    }

    public static boolean isPrimitiveWrapperArray(Class<?> clazz) {
        if(clazz == null)
            return false;
        return clazz.isArray() && isPrimitiveWrapper(clazz.getComponentType());
    }

    private static final Map<Class<?>, Class<?>> primitiveWrapperTypeMap = new HashMap<>(8);
    static {
        primitiveWrapperTypeMap.put(Boolean.class, Boolean.TYPE);
        primitiveWrapperTypeMap.put(Byte.class, Byte.TYPE);
        primitiveWrapperTypeMap.put(Character.class, Character.TYPE);
        primitiveWrapperTypeMap.put(Double.class, Double.TYPE);
        primitiveWrapperTypeMap.put(Float.class, Float.TYPE);
        primitiveWrapperTypeMap.put(Integer.class, Integer.TYPE);
        primitiveWrapperTypeMap.put(Long.class, Long.TYPE);
        primitiveWrapperTypeMap.put(Short.class, Short.TYPE);
    }
}
