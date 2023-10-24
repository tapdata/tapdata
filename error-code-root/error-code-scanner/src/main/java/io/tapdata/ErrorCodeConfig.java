package io.tapdata;

import io.tapdata.exception.TapExClass;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author samuel
 * @Description
 * @create 2023-03-16 17:10
 **/
public class ErrorCodeConfig {
	private Map<String, ErrorCodeEntity> errorCodeEntityMap = new ConcurrentHashMap<>();
	private Map<Class<?>, List<ErrorCodeEntity>> errorClassMap = new ConcurrentHashMap<>();

	public static ErrorCodeConfig getInstance() {
		return ErrorCodeConfigSingleton.INSTANCE.errorCodeConfig;
	}

	/**
	 * Scan error code class{@link TapExClass} and save in memory map
	 */
	public void init() {
		errorCodeEntityMap = new ConcurrentHashMap<>(Scanner.getErrorCodeMap());
		errorClassMap = new ConcurrentHashMap<>(Scanner.getExClassMap());
	}

	public ErrorCodeEntity getErrorCode(String code) {
		if (null == code) {
			return null;
		}
		return errorCodeEntityMap.get(code);
	}

	public Map<String, ErrorCodeEntity> getErrorCodeEntityMap() {
		return errorCodeEntityMap;
	}

	public Map<Class<?>, List<ErrorCodeEntity>> getErrorClassMap() {
		return errorClassMap;
	}

	private enum ErrorCodeConfigSingleton {
		INSTANCE,
		;
		private final ErrorCodeConfig errorCodeConfig;

		ErrorCodeConfigSingleton() {
			errorCodeConfig = new ErrorCodeConfig();
			errorCodeConfig.init();
		}
	}
}
