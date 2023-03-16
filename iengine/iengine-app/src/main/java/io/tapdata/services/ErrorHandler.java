package io.tapdata.services;

import io.tapdata.ErrorCodeEntity;
import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.annotations.MainMethod;
import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;
import org.reflections.Reflections;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ConfigurationBuilder;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author samuel
 * @Description
 * @create 2023-03-16 17:10
 **/
@Bean
@MainMethod("main")
public class ErrorHandler {

	private final Map<String, ErrorCodeEntity> errorCodeEntityMap = new ConcurrentHashMap<>();

	public void main() {
		ConfigurationBuilder builder = new ConfigurationBuilder()
				.addScanners(new TypeAnnotationsScanner())
				.addClassLoader(Thread.currentThread().getContextClassLoader())
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
				ErrorCodeEntity errorCodeEntity = ErrorCodeEntity.create()
						.describe(annotation.describe())
						.describeCN(annotation.describeCN())
						.solution(annotation.solution())
						.solutionCN(annotation.solutionCN())
						.howToReproduce(annotation.howToReproduce())
						.seeAlso(annotation.seeAlso());
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
				errorCodeEntityMap.put(code, errorCodeEntity);
			}
		}
	}

	public ErrorCodeEntity getErrorCode(String code) {
		if (null == code) {
			return null;
		}
		return errorCodeEntityMap.get(code);
	}
}
