package io.tapdata.services;

import io.tapdata.ErrorCodeEntity;
import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.annotations.MainMethod;
import io.tapdata.entity.memory.MemoryFetcher;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;
import io.tapdata.pdk.core.api.PDKIntegration;
import org.apache.commons.lang3.StringUtils;
import org.reflections.Reflections;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ConfigurationBuilder;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
public class ErrorHandler implements MemoryFetcher {
	private final Map<String, ErrorCodeEntity> errorCodeEntityMap = new ConcurrentHashMap<>();
	private final Map<String, List<ErrorCodeEntity>> errorClassMap = new ConcurrentHashMap<>();

	public void main() {
		ConfigurationBuilder builder = new ConfigurationBuilder()
				.addScanners(new TypeAnnotationsScanner())
				.addClassLoader(this.getClass().getClassLoader())
				.forPackages("io.tapdata", "com.tapdata");

		Reflections reflections = new Reflections(builder);
		Set<Class<?>> exCodeClasses = reflections.getTypesAnnotatedWith(TapExClass.class);
		for (Class<?> exCodeClass : exCodeClasses) {
			String exCodeClassName = exCodeClass.getName();
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
						.sourceExClass(exCodeClass.getName())
						.seeAlso(annotation.seeAlso());
				errorCodeEntityMap.put(code, errorCodeEntity);

				errorClassMap.computeIfAbsent(exCodeClassName, key -> new ArrayList<>())
						.add(errorCodeEntity);
			}
		}

		PDKIntegration.registerMemoryFetcher("Error Code", this);
	}

	public ErrorCodeEntity getErrorCode(String code) {
		if (null == code) {
			return null;
		}
		return errorCodeEntityMap.get(code);
	}

	@Override
	public DataMap memory(String keyRegex, String memoryLevel) {
		DataMap dataMap = new DataMap();
		if (StringUtils.isNotBlank(keyRegex)) {
			ErrorCodeEntity errorCodeEntity = errorCodeEntityMap.get(keyRegex);
			if (null != errorCodeEntity) {
				dataMap.put(keyRegex, errorCodeEntity);
			}
		} else {
			if (memoryLevel.equals(MemoryFetcher.MEMORY_LEVEL_SUMMARY)) {
				for (Map.Entry<String, List<ErrorCodeEntity>> entry : errorClassMap.entrySet()) {
					String exCodeClzName = entry.getKey();
					List<ErrorCodeEntity> errorCodes = entry.getValue();
					Map<String, Object> errorCodeMap = new HashMap<>();
					for (ErrorCodeEntity errorCode : errorCodes) {
						errorCodeMap.put(
								String.format("[%s]%s", errorCode.getCode(), errorCode.getName()),
								StringUtils.isNotBlank(errorCode.getDescribeCN()) ? errorCode.getDescribeCN() : errorCode.getDescribe()
						);
					}
					dataMap.put(exCodeClzName, errorCodeMap);
				}
			} else if (memoryLevel.equals(MEMORY_LEVEL_IN_DETAIL)) {
				dataMap.putAll(errorClassMap);
			}
		}
		return dataMap;
	}
}
