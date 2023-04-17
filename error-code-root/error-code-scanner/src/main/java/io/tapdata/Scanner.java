package io.tapdata;

import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;
import org.reflections.Reflections;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ConfigurationBuilder;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * @author samuel
 * @Description
 * @create 2023-03-17 23:36
 **/
public class Scanner {
	public static Map<String, ErrorCodeEntity> getErrorCodeMap() {
		Map<String, ErrorCodeEntity> errorCodeEntityMap = new HashMap<>();
		scan(errorCode -> errorCodeEntityMap.put(errorCode.getCode(), errorCode));
		return errorCodeEntityMap;
	}

	public static Map<Class<?>, List<ErrorCodeEntity>> getExClassMap() {
		Map<Class<?>, List<ErrorCodeEntity>> exClassMap = new HashMap<>();
		scan(errorCode -> exClassMap.computeIfAbsent(errorCode.getSourceExClass(), key -> new ArrayList<>()).add(errorCode));
		return exClassMap;
	}

	private static void scan(Consumer<ErrorCodeEntity> consumer) {
		ConfigurationBuilder builder = new ConfigurationBuilder()
				.addScanners(new TypeAnnotationsScanner())
				.addClassLoaders(Scanner.class.getClassLoader())
				.forPackages("io.tapdata", "com.tapdata");

		Reflections reflections = new Reflections(builder);
		Set<Class<?>> exCodeClasses = reflections.getTypesAnnotatedWith(TapExClass.class);
		for (Class<?> exCodeClass : exCodeClasses) {
			Field[] declaredFields = exCodeClass.getDeclaredFields();
			for (Field field : declaredFields) {
				field.setAccessible(true);
				TapExCode annotation = field.getAnnotation(TapExCode.class);
				if (null == annotation) {
					continue;
				}
				Object fieldValue;
				String code;
				try {
					fieldValue = field.get(exCodeClass);
				} catch (IllegalAccessException e) {
					throw new RuntimeException("Get field value error", e);
				}
				if (!(fieldValue instanceof String)) {
					continue;
				}
				code = fieldValue.toString();
				ErrorCodeEntity errorCodeEntity = ErrorCodeEntity.create()
						.name(field.getName())
						.code(code)
						.describe(annotation.describe())
						.describeCN(annotation.describeCN())
						.solution(annotation.solution())
						.solutionCN(annotation.solutionCN())
						.howToReproduce(annotation.howToReproduce())
						.level(annotation.level())
						.recoverable(annotation.recoverable())
						.skippable(annotation.skippable())
						.sourceExClass(exCodeClass)
						.seeAlso(annotation.seeAlso());

				if (null != consumer) {
					consumer.accept(errorCodeEntity);
				}
			}
		}
	}
}
