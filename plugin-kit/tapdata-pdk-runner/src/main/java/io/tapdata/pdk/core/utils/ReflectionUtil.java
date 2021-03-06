package io.tapdata.pdk.core.utils;

import io.tapdata.entity.logger.TapLogger;

import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.util.*;
import java.util.zip.CRC32;

/**
 * @author
 */
public class ReflectionUtil {
    private static final String TAG = ReflectionUtil.class.getSimpleName();
    /**
     * public abstract void test.Test.init(test.Test)
     * <p>
     * will generate "init"
     *
     * @param
     * @return
     */
    public static String generateMethodKey(Method method) {
        String str = method.toString();
        int endPos = str.indexOf('(');
        int startPos = str.lastIndexOf('.', endPos) + 1;
        return str.substring(startPos, endPos);
//		return method.toGenericString();
    }

    public static void main(String[] args) {
    }

    public static Class<?> getInitiatableClass(Class<?> clazz) {
        Class<?> theClass = null;
        if (!ReflectionUtil.canBeInitiated(clazz)) {
            if (Collection.class.isAssignableFrom(clazz)) {
                theClass = ArrayList.class;
            } else if (Map.class.isAssignableFrom(clazz)) {
                theClass = HashMap.class;
            }
        } else {
            theClass = clazz;
        }

        if (theClass != null) {
            TapLogger.debug(TAG, "Convert class from " + clazz + " to " + theClass);
            return theClass;
        }
        return clazz;
    }

    public static boolean isPrimitiveWrapper(Class<?> clazz) {
//		Assert.notNull(clazz, "Class must not be null");
        return primitiveWrapperTypeMap.containsKey(clazz);
    }

    public static boolean isPrimitiveOrWrapper(Class<?> clazz) {
//		Assert.notNull(clazz, "Class must not be null");
        return clazz.isPrimitive() || isPrimitiveWrapper(clazz);
    }

    public static boolean isPrimitiveArray(Class<?> clazz) {
//		Assert.notNull(clazz, "Class must not be null");
        return clazz.isArray() && clazz.getComponentType().isPrimitive();
    }

    public static boolean isPrimitiveWrapperArray(Class<?> clazz) {
//		Assert.notNull(clazz, "Class must not be null");
        return clazz.isArray() && isPrimitiveWrapper(clazz.getComponentType());
    }

    private static final Map<Class<?>, Class<?>> primitiveWrapperTypeMap = new HashMap(8);
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

    public static boolean canBeInitiated(Class<?> clazz) {
        if (isPrimitiveOrWrapper(clazz))
            return true;
        if (clazz.isArray()) {
            return ReflectionUtil.canBeInitiated(clazz.getComponentType());
        }
        if (clazz.isInterface()) {
            return false;
        }
//		ConstructorUtils.getMatchingAccessibleConstructor(clazz, null);
        try {
            Constructor<?> contructor = clazz.getConstructor();
            if (contructor == null)
                return false;
        } catch (NoSuchMethodException e) {
//            e.printStackTrace();
            return false;
        }
        return true;
    }

    public static Method[] getMethods(Class<?> clazz) {
        HashMap<String, Method> allFields = new HashMap<String, Method>();
        Method[] sFields = null;
        Method[] fields = clazz.getDeclaredMethods();
        for (Method m : fields) {
            allFields.put(generateMethodKey(m), m);
        }
        Class<?>[] cInterfaces = clazz.getInterfaces();
        for (Class<?> c : cInterfaces) {
            if (c != null) {
                sFields = getInterfaceMethods(c);
            }
            if (sFields != null) {
                for (Method f : sFields) {
                    if (!allFields.containsKey(generateMethodKey(f))) {
                        allFields.put(generateMethodKey(f), f);
                    }
                }
            }
        }
        Class<?> superClass = clazz.getSuperclass();
        if (superClass != null && !superClass.equals(Object.class)) {
            sFields = getMethods(superClass);
            if (sFields != null) {
                for (Method f : sFields) {
                    if (!allFields.containsKey(generateMethodKey(f))) {
                        allFields.put(generateMethodKey(f), f);
                    }
                }
            }
        }

        Method[] fs = new Method[allFields.size()];
        allFields.values().toArray(fs);
        return fs;
    }

    public static Method[] getInterfaceMethods(Class<?> clazz) {
        if (!Modifier.isInterface(clazz.getModifiers())) {
            return null;
        }
        HashMap<String, Method> allFields = new HashMap<String, Method>();
        Method[] sFields = null;
        Method[] fields = clazz.getDeclaredMethods();
        for (Method m : fields) {
            allFields.put(generateMethodKey(m), m);
        }
        Class<?>[] cInterfaces = clazz.getInterfaces();
        for (Class<?> c : cInterfaces) {
            if (c != null) {
                sFields = getInterfaceMethods(c);
            }
            if (sFields != null) {
                for (Method f : sFields) {
                    if (!allFields.containsKey(generateMethodKey(f))) {
                        allFields.put(generateMethodKey(f), f);
                    }
                }
            }
        }
        Method[] fs = new Method[allFields.size()];
        allFields.values().toArray(fs);
        return fs;
    }

    public static Field getField(Class clazz, String fieldName)
            throws NoSuchFieldException {
        try {
            return clazz.getDeclaredField(fieldName);
        } catch (NoSuchFieldException e) {
            Class superClass = clazz.getSuperclass();
            if (superClass == null) {
                throw e;
            } else {
                return getField(superClass, fieldName);
            }
        }
    }

    public static Field[] getFields(Class<?> clazz) {
        Field[] sFields = null;
        Field[] fields = clazz.getDeclaredFields();
        Class<?> c = clazz.getSuperclass();
        if (c != null && !c.equals(Object.class)) {
            sFields = getFields(c);
        }
        if (sFields == null) {
            return fields;
        } else {
            HashMap<String, Field> allFields = new HashMap<String, Field>();

            //Exclude the same fields existing between subclass and superclass.
            for (Field f : sFields) {
                allFields.put(f.getName(), f);
            }
            for (Field f : fields) {
                allFields.put(f.getName(), f);
            }

            Field[] fs = new Field[allFields.size()];
            allFields.values().toArray(fs);
            return fs;
        }

    }

    public static void makeAccessible(Field field) {
        if (!Modifier.isPublic(field.getModifiers())
                || !Modifier.isPublic(field.getDeclaringClass().getModifiers())) {
            field.setAccessible(true);
        }
    }


    public static Object getFieldValueByGetter(Object owner, Method method) throws Exception {
//		fieldName = ChatUtils.captureName(fieldName);
//		Method me = getDeclaredMethod(owner.getClass(), "get"+fieldName, null);
        if (method != null) {
            return method.invoke(owner);
        }
        return null;
    }

    public static Method getMethodByField(Class clazz, String fieldName) {
        char[] cs = fieldName.toCharArray();
        cs[0] -= 32;
        fieldName = String.valueOf(cs);
        return getDeclaredMethod(clazz, "get" + fieldName, null);
    }

    /**
     * ??????????????????, ??????????????? DeclaredMethod
     *
     * @param
     * @param methodName
     * @param parameterTypes
     * @return
     */
    public static Method getDeclaredMethod(Class clazz, String methodName, Class<?>[] parameterTypes) throws SecurityException {

        for (Class<?> superClass = clazz; superClass != Object.class; superClass = superClass.getSuperclass()) {
            try {
                //superClass.getMethod(methodName, parameterTypes);
                return superClass.getDeclaredMethod(methodName, parameterTypes);
            } catch (NoSuchMethodException e) {
                //Method ?????????????????????, ??????????????????
            }
            //..
        }

        return null;
    }


    /**
     * 1. ???????????????????????????
     *
     * @param owner
     * @param fieldName
     * @return
     * @throws Exception
     */
    public static Object getFieldValue(Object owner, String fieldName)
            throws Exception {
        Class<? extends Object> ownerClass = owner.getClass(); // ??????????????????Class
        Field field = getField(ownerClass, fieldName); // ??????Class????????????????????????
        field.setAccessible(true);
        Object property = field.get(owner); // ???????????????????????????????????????????????????????????????????????????????????????IllegalAccessException
        return property;
    }

    public static Object getFieldValue(Field field, Object owner) throws Exception {
        field.setAccessible(true);
        return field.get(owner);
    }

    /**
     * 2. ??????????????????????????????
     *
     * @param className
     * @param fieldName
     * @return
     * @throws Exception
     */
    public static Object getStaticProperty(String className, String fieldName)
            throws Exception {
        Class<?> ownerClass = Class.forName(className); // ????????????????????????Class
        Field field = ownerClass.getField(fieldName); // ????????????????????????Class????????????????????????
        field.setAccessible(true);
        Object property = field.get(ownerClass); // ?????????????????????????????????????????????????????????????????????????????????Class??????
        return property;
    }

    /**
     * 3. ????????????????????????
     *
     * @param owner
     * @param methodName
     * @param args
     * @return
     * @throws Exception
     */
    public static Object invokeMethod(Object owner, String methodName,
                                      Object[] args) throws Exception {
        Class<? extends Object> ownerClass = owner.getClass(); // ???????????????????????????????????????Class
        Class[] argsClass = null;
        if (args != null) {
            argsClass = new Class[args.length];
            // ??????????????????????????????Class?????????????????????Method?????????
            for (int i = 0, j = args.length; i < j; i++) {
                argsClass[i] = args[i].getClass();
            }
        }
        Method method = null;
        if (argsClass != null)
            method = ownerClass.getMethod(methodName, argsClass); // ??????Method???????????????Class????????????????????????Method
        else
            method = ownerClass.getMethod(methodName); // ??????Method???????????????Class????????????????????????Method

        if (args != null)
            return method.invoke(owner, args); // ?????????Method???invoke??????????????????????????????????????????????????????????????????????????????Object?????????????????????????????????
        else
            return method.invoke(owner);
    }

    /**
     * 4. ?????????????????????????????? ????????????????????????3????????????????????????????????????invoke??????????????????null?????????????????????????????????????????????????????????
     *
     * @param className
     * @param methodName
     * @param args
     * @return
     * @throws Exception
     */
    public static Object invokeStaticMethod(String className,
                                            String methodName, Object[] args) throws Exception {
        Class<?> ownerClass = Class.forName(className);
        Class[] argsClass = new Class[args.length];
        for (int i = 0, j = args.length; i < j; i++) {
            argsClass[i] = args[i].getClass();
        }
        Method method = ownerClass.getMethod(methodName, argsClass);
        return method.invoke(null, args);
    }

    public static Object invokeStaticMethod(String className,
                                            String methodName) throws Exception {
        return invokeStaticMethod(className, methodName, new Object[0]);
    }

    /**
     * 5. ????????????
     * ????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????newoneClass.newInstance()????????????
     *
     * @param className
     * @param args
     * @return
     * @throws Exception
     */
    public static Object newInstance(Class<?> clazz, Object[] args) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException, ClassNotFoundException {
        // ??????????????????????????????Class??????
        Class[] argsClass = null;
        if (args != null) {
            argsClass = new Class[args.length];
            for (int i = 0, j = args.length; i < j; i++) {
                argsClass[i] = args[i].getClass();
            }
        }
        Constructor cons = null;
        if (argsClass == null)
            cons = clazz.getConstructor(); // ??????????????????
        else
            cons = clazz.getConstructor(argsClass); // ??????????????????
        return cons.newInstance(args); // ????????????
    }
    public static Object newInstance(String className, Object[] args) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException, ClassNotFoundException {
        Class<?> newoneClass = Class.forName(className); // ???????????????????????????????????????Class
        return newInstance(newoneClass, args);
    }

    /**
     * 6. ?????????????????????????????????
     *
     * @param obj
     * @param cls
     * @return
     */
    public static boolean isInstance(Object obj, Class<?> cls) {
        return cls.isInstance(obj);
    }

    /**
     * 7. ??????????????????????????????
     *
     * @param array
     * @param index
     * @return
     */
    public static Object getByArray(Object array, int index) {
        return Array.get(array, index);
    }

    public static void log(String name, Object obj) {
        Class ownerClass = obj.getClass();

        //??????????????????????????????private????????????getField()????????????public????????????????????????????????????????????????????????????????????????
        Field[] fields = getFields(ownerClass);

        String objName = ownerClass.getName();//??????????????????????????????

        System.out.println(name + objName + ": ");

        for (int i = 0; i < fields.length; i++) {
            Field field = fields[i];

            String fieldName = field.getName();//??????????????????
            Object value = null;
            try {
                value = getFieldValue(obj, fieldName);
            } catch (Exception e) {
            }
            if (value != null) {
                System.out.println("    [" + fieldName + " = " + value.toString() + "]");
            }
        }

        if (fields.length == 0) {
            System.out.println("    [" + obj.toString() + "]");
        }
    }

//	public static long getCrc(Method method) {
//		return getCrc(method, null);
//	}

    public static long getCrc(Class<?> clazz, String methodName) {
        return getCrc(clazz.getSimpleName(), methodName);
    }

    public static long getCrc(String className, String methodName) {
        if (methodName == null || className == null)
            return -1;
        String name = methodName;
        CRC32 crc = new CRC32();
        String str = name + "#" + className;
        crc.update(str.getBytes());
        long value = crc.getValue();
        return value;
    }

    public static long getCrc(Method method, String service) {
        return getCrc(method.getDeclaringClass(), method.getName(), service);
    }

    public static long getCrc(Class<?> clazz, String methodName, String service) {
        return getCrc(clazz.getSimpleName(), methodName, service);
    }

    public static long getCrc(String className, String methodName, String service) {
        if (methodName == null || className == null || service == null)
            return -1;
        String name = methodName;
//		String className = clazz.getSimpleName();
        CRC32 crc = new CRC32();
        String str = name + "#" + className;
        if (service != null) {
            str = service + "#" + str;
        }
        crc.update(str.getBytes());
        long value = crc.getValue();
//        LoggerEx.info("ReflectionUtil", "Get crc, str: " + str + ",value: " + value);
//		if(value == 2380642687L)
//			System.out.print("");
        return value;
    }

    public static String getMethodKey(String service, Class<?> clazz, String methodName) {
        if (service == null || clazz == null || methodName == null) {
            return "";
        }
        return service + "#" + clazz.getSimpleName() + "#" + methodName;
    }

    public static String[] getParamNames(Method method) {
        List<String> paramNameList = new ArrayList<>();
        if (method != null) {
            Parameter[] parameters = method.getParameters();
            for (int i = 0; i < parameters.length; i++) {
                Parameter parameter = parameters[i];
                paramNameList.add(parameter.getName());
            }
        }
        return paramNameList.toArray(new String[]{});
    }

	public static List<Annotation> getMethodDeclaredAnnotations(Class<?> clazz, Method method) {
        ArrayList<Annotation> list = new ArrayList<>();
        Annotation[] anntations = method.getDeclaredAnnotations();
        if(anntations != null)
            list.addAll(Arrays.asList(anntations));
        Class<?>[] parentInterfaces = clazz.getInterfaces();
        Class<?> superClass = clazz.getSuperclass();
        if(parentInterfaces != null) {
            for(Class<?> c : parentInterfaces) {
                try {
                    Method m = c.getMethod(method.getName(), method.getParameterTypes());
                    if(m == null) continue;
                    Annotation[] annotations = m.getDeclaredAnnotations();
                    if(annotations != null)
                        list.addAll(Arrays.asList(annotations));
                } catch (NoSuchMethodException ignored) {
                }
            }
        }
        if(superClass != null) {
            try {
                Method m = superClass.getMethod(method.getName(), method.getParameterTypes());
                if(m != null) {
                    Annotation[] annotations = m.getDeclaredAnnotations();
                    if(annotations != null)
                        list.addAll(Arrays.asList(annotations));
                }
            } catch (NoSuchMethodException e) {
            }
        }
        return list;
	}
}
