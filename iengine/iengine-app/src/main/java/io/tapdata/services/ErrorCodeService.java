package io.tapdata.services;

import io.tapdata.ErrorCodeEntity;
import io.tapdata.entity.annotations.Bean;
import io.tapdata.service.skeleton.annotation.RemoteService;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2023-03-16 16:54
 **/
@RemoteService
public class ErrorCodeService {

	@Bean
	private ErrorHandler errorHandler;

	public Map<String, Object> getErrorCode(String code, String language) {
		ErrorCodeEntity errorCode = errorHandler.getErrorCode(code);
		Map<String, Object> res = new HashMap<>();
		if (null == errorCode) {
			return res;
		}
		language = StringUtils.isBlank(language) ? "en" : language;
		Language languageEnum = Language.fromValue(language);
		languageEnum = null == languageEnum ? Language.EN : languageEnum;
		String describe = "";
		String solution = "";
		switch (languageEnum) {
			case CN:
				describe = errorCode.getDescribeCN();
				solution = errorCode.getSolutionCN();
				if (StringUtils.isNotBlank(solution)) {
					solution = "\n\n解决方案\n" + solution;
				}
				break;
			default:
				describe = errorCode.getDescribe();
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
}
