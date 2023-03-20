package io.tapdata.services.errorcode;

import io.tapdata.ErrorCodeConfig;
import io.tapdata.ErrorCodeEntity;
import io.tapdata.entity.memory.MemoryFetcher;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.exception.TapExClass;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.service.skeleton.annotation.RemoteService;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author samuel
 * @Description
 * @create 2023-03-16 16:54
 **/
@RemoteService
public class ErrorCodeService implements MemoryFetcher {

	public ErrorCodeService() {
		PDKIntegration.registerMemoryFetcher("ErrorCode", this);
	}

	public Map<String, Object> getErrorCode(String code, String language) {
		ErrorCodeEntity errorCode = ErrorCodeConfig.getInstance().getErrorCode(code);
		Map<String, Object> res = new HashMap<>();
		if (null == errorCode) {
			return res;
		}
		language = StringUtils.isBlank(language) ? "en" : language;
		Language languageEnum = Language.fromValue(language);
		languageEnum = null == languageEnum ? Language.EN : languageEnum;

		String describe = "";
		String solution;

		Class<?> sourceExClass = errorCode.getSourceExClass();
		TapExClass tapExClass = null;
		if (null != sourceExClass) {
			tapExClass = sourceExClass.getAnnotation(TapExClass.class);
		}
		switch (languageEnum) {
			case CN:
				if (null != tapExClass) {
					describe = String.format("模块名: %s(%s)", tapExClass.module(), tapExClass.code());
					if (StringUtils.isNotBlank(tapExClass.describe())) {
						describe += String.format("\n模块描述: %s\n\n", tapExClass.describe());
					} else {
						describe += "\n\n";
					}
				}
				describe += "错误描述\n" + errorCode.getDescribeCN();
				solution = errorCode.getSolutionCN();
				if (StringUtils.isNotBlank(solution)) {
					solution = "\n\n解决方案\n" + solution;
				}
				break;
			default:
				if (null != tapExClass) {
					describe = String.format("Module name: %s(%s)", tapExClass.module(), tapExClass.code());
					if (StringUtils.isNotBlank(tapExClass.describe())) {
						describe += String.format("\nModule describe: %s\n\n", tapExClass.describe());
					} else {
						describe += "\n\n";
					}
				}
				describe += "Error describe\n" + errorCode.getDescribe();
				solution = errorCode.getSolution();
				if (StringUtils.isNotBlank(solution)) {
					solution = "\n\nSolution\n" + solution;
				}
				break;
		}
		if (StringUtils.isNotBlank(describe) && StringUtils.isNotBlank(solution)) {
			describe = describe + solution;
		}
		res.put("describe", describe);
		res.put("seeAlso", errorCode.getSeeAlso());
		return res;
	}

	private enum Language {
		EN("en"),

		CN("cn"),
		;

		private String language;

		Language(String language) {
			this.language = language;
		}

		public String getLanguage() {
			return language;
		}

		private static final Map<String, Language> map = new HashMap<>();

		static {
			for (Language language : Language.values()) {
				map.put(language.getLanguage(), language);
			}
		}

		public static Language fromValue(String value) {
			return map.get(value);
		}
	}

	@Override
	public DataMap memory(String keyRegex, String memoryLevel) {
		DataMap dataMap = new DataMap();
		if (StringUtils.isNotBlank(keyRegex)) {
			ErrorCodeEntity errorCodeEntity = ErrorCodeConfig.getInstance().getErrorCode(keyRegex);
			if (null != errorCodeEntity) {
				dataMap.put(keyRegex, errorCodeEntity);
			}
		} else {
			Map<Class<?>, List<ErrorCodeEntity>> errorClassMap = ErrorCodeConfig.getInstance().getErrorClassMap();
			if (memoryLevel.equals(MemoryFetcher.MEMORY_LEVEL_SUMMARY)) {
				LinkedHashMap<Class<?>, List<ErrorCodeEntity>> linkedHashMap = errorClassMap.entrySet().stream()
						.sorted((o1, o2) -> {
							TapExClass exClass1 = o1.getKey().getAnnotation(TapExClass.class);
							TapExClass exClass2 = o2.getKey().getAnnotation(TapExClass.class);
							return Integer.compare(exClass1.code(), exClass2.code());
						}).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (a, b) -> a, LinkedHashMap::new));
				for (Map.Entry<Class<?>, List<ErrorCodeEntity>> entry : linkedHashMap.entrySet()) {
					Class<?> exCodeClz = entry.getKey();
					List<ErrorCodeEntity> errorCodes = entry.getValue();
					List<String> list = errorCodes.stream().map(ErrorCodeEntity::fullErrorCode).collect(Collectors.toList());
					dataMap.put(exCodeClz.getSimpleName(), list);
				}
			} else if (memoryLevel.equals(MEMORY_LEVEL_IN_DETAIL)) {
				errorClassMap.forEach((k, v) -> dataMap.put(k.getSimpleName(), v));
			}
		}
		return dataMap;
	}
}
