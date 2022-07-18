package com.tapdata.constant;

import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Reflect util
 *
 * @author samuel
 */
public class ReflectUtil {

	/**
	 * Invoke method in specified interface
	 * Attention: find method only by method name, if same method name, diff params, will only invoke first method that found
	 *
	 * @param instance
	 * @param interfaceClassPathName
	 * @param methodName
	 * @param args
	 * @return
	 * @throws InvocationTargetException
	 * @throws IllegalAccessException
	 * @throws RuntimeException
	 */
	public static Object invokeInterfaceMethod(Object instance,
											   String interfaceClassPathName,
											   String methodName,
											   Object... args) throws InvocationTargetException, IllegalAccessException, RuntimeException {
		if (instance == null || StringUtils.isAnyBlank(interfaceClassPathName, methodName)) {
			return null;
		}

		String[] interfaceClassPathNames = interfaceClassPathName.split(";");

		List<Class<?>> allInterfaces = getAllInterfaces(instance.getClass(), null);

		for (Class<?> anInterface : allInterfaces) {
			if (Arrays.stream(interfaceClassPathNames).filter(name -> anInterface.getName().equals(name.trim())).findFirst().orElse(null) != null) {
				Method[] methods = anInterface.getMethods();
				Method method = Arrays.stream(methods).filter(mt -> mt.getName().equals(methodName) && mt.getParameterCount() == args.length).findFirst().orElse(null);
				if (method == null) {
					break;
				}

				Object result = method.invoke(instance, args);
				return result;
			}
		}

		return null;
	}

	public static <T> Predicate<T> distinctByKey(Function<? super T, Object> keyExtractor) {
		Map<Object, Boolean> map = new ConcurrentHashMap<>();
		return t -> map.putIfAbsent(keyExtractor.apply(t), Boolean.TRUE) == null;
	}

	public static List<Class<?>> getAllInterfaces(Class<?> clazz, List<Class<?>> interfaces) {
		if (null == interfaces) {
			interfaces = new ArrayList<>();
		}
		if (null == clazz) {
			return interfaces;
		}
		if (clazz.getName().equals(Object.class.getName())) {
			return interfaces;
		}
		Class<?>[] list = clazz.getInterfaces();
		if (list.length > 0) {
			interfaces.addAll(Arrays.asList(list));
		}
		Class<?> superclass = clazz.getSuperclass();
		if (null != superclass) {
			getAllInterfaces(superclass, interfaces);
		}
		return interfaces;
	}
}
