package io.tapdata.services.errorcode;

import io.tapdata.ErrorCodeConfig;
import io.tapdata.ErrorCodeEntity;
import io.tapdata.entity.memory.MemoryFetcher;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.exception.TapExClass;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.service.skeleton.annotation.RemoteService;
import io.tapdata.utils.AppType;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.message.ParameterizedMessage;

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
	private static final String DEFAULT_SOLUTION_DAAS_CN = "运行出现未知异常，建议: " +
			"\n 1. 检查任务配置;" +
			"\n 2. 检查每个节点的数据模型是否正确，如果不正确，请尝试点击源节点的\"重新加载\";" +
			"\n 3. 重新启动任务进行重试;" +
			"\n 4. 根据显示的错误堆栈信息进行初步排查并尝试解决。" +
			"\n如仍未能解决问题，点击\"复制\"按钮，将错误详情发送给售后支持团队，我们将竭诚为您提供专业协助。";
	private static final String DEFAULT_SOLUTION_DFS_CN = "运行出现未知异常，建议: " +
			"\n 1. 检查任务配置;" +
			"\n 2. 检查每个节点的数据模型是否正确，如果不正确，请尝试点击源节点的\"重新加载\";" +
			"\n 3. 重新启动任务进行重试;" +
			"\n 4. 根据显示的错误堆栈信息进行初步排查并尝试解决。" +
			"\n如仍未能解决问题，点击下方\"创建工单\"按钮，将错误详情发送给售后支持团队，我们将竭诚为您提供专业协助。";
	private static final String DEFAULT_SOLUTION_DAAS_EN = "An unknown exception occurred during operation. Recommended:" +
			"\n 1. Check task configuration;" +
			"\n 2. Check whether the data model of each node is correct. If it is incorrect, please try to click \"Reload\" of the source node;" +
			"\n 3. Restart the task and try again;" +
			"\n 4. Conduct preliminary troubleshooting based on the displayed error stack information and try to solve it." +
			"\nIf the problem still cannot be solved, click the \"Copy\" button and send the error details to the after-sales support team. We will wholeheartedly provide you with professional assistance.";
	private static final String DEFAULT_SOLUTION_DFS_EN = "An unknown exception occurred during operation. Recommended:" +
			"\n 1. Check task configuration;" +
			"\n 2. Check whether the data model of each node is correct. If it is incorrect, please try to click \"Reload\" of the source node;" +
			"\n 3. Restart the task and try again;" +
			"\n 4. Conduct preliminary troubleshooting based on the displayed error stack information and try to solve it." +
			"\nIf the problem still cannot be solved, click the \"Create Ticket\" button and send the error details to the after-sales support team. We will wholeheartedly provide you with professional assistance.";

	public ErrorCodeService() {
		PDKIntegration.registerMemoryFetcher("ErrorCode", this);
	}

	public Map<String, Object> getErrorCode(String code, String language) {
		return getErrorCodeWithDynamic(code, language, null);
	}

	public Map<String, Object> getErrorCodeWithDynamic(String code, String language, String[] dynamicDescriptionParameters) {
		ErrorCodeEntity errorCode = ErrorCodeConfig.getInstance().getErrorCode(code);
		Map<String, Object> res = new HashMap<>();
		if (null == errorCode) {
			return res;
		}
		language = StringUtils.isBlank(language) ? "en" : language;
		Language languageEnum = Language.fromValue(language);
		languageEnum = null == languageEnum ? Language.EN : languageEnum;

		String module = "";
		int moduleCode = 0;
		String describe;
		String solution;
		String dynamicDescribe;
		String fullErrorCode = code;

		Class<?> sourceExClass = errorCode.getSourceExClass();
		TapExClass tapExClass = null;
		if (null != sourceExClass) {
			tapExClass = sourceExClass.getAnnotation(TapExClass.class);
		}
		if (null != tapExClass) {
			module = tapExClass.module();
			moduleCode = tapExClass.code();
			fullErrorCode = (StringUtils.isNotBlank(tapExClass.prefix()) ? tapExClass.prefix() : tapExClass.module()) + code;
		}
		if (languageEnum == Language.CN) {
			describe = errorCode.getDescribeCN();
			solution = errorCode.getSolutionCN();
			dynamicDescribe = getDynamicDescribe(dynamicDescriptionParameters, errorCode.getDynamicDescriptionCN());
		} else {
			describe = errorCode.getDescribe();
			solution = errorCode.getSolution();
			dynamicDescribe = getDynamicDescribe(dynamicDescriptionParameters, errorCode.getDynamicDescription());
		}
		if (StringUtils.isBlank(solution)) {
			solution = defaultSolution(AppType.currentType(), languageEnum);
		}
		res.put("errorCode", code);
		res.put("fullErrorCode", fullErrorCode);
		res.put("module", module);
		res.put("moduleCode", moduleCode);
		res.put("describe", describe);
		res.put("solution", solution);
		res.put("seeAlso", errorCode.getSeeAlso());
		res.put("dynamicDescribe", dynamicDescribe);
		return res;
	}

	protected String getDynamicDescribe(String[] dynamicDescriptionParameters, String dynamicDescribe) {
		String res = "";
		if (null != dynamicDescriptionParameters && dynamicDescriptionParameters.length > 0 && StringUtils.isNotBlank(dynamicDescribe)) {
			res = new ParameterizedMessage(dynamicDescribe, (Object[]) dynamicDescriptionParameters).getFormattedMessage();
		}
		return res;
	}

	protected enum Language {
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
				long total = 0L;
				long describeTotal = 0L;
				long solutionTotal = 0L;
				long descAndSolutionTotal = 0L;
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
					total += list.size();
					describeTotal += errorCodes.stream().filter(e -> StringUtils.isNotBlank(e.getDescribe())).count();
					solutionTotal += errorCodes.stream().filter(e -> StringUtils.isNotBlank(e.getSolution())).count();
					descAndSolutionTotal += errorCodes.stream().filter(e -> StringUtils.isNotBlank(e.getDescribe()) && StringUtils.isNotBlank(e.getSolution())).count();
				}
				DataMap stats = DataMap.create()
						.kv("total", total)
						.kv("describeTotal", describeTotal)
						.kv("solutionTotal", solutionTotal)
						.kv("descAndSolutionTotal", descAndSolutionTotal);
				dataMap.put("stats", stats);
			} else if (memoryLevel.equals(MEMORY_LEVEL_IN_DETAIL)) {
				errorClassMap.forEach((k, v) -> dataMap.put(k.getSimpleName(), v));
			}
		}
		return dataMap;
	}

	protected String defaultSolution(AppType appType, Language languageEnum) {
		if (null == appType) {
			appType = AppType.DAAS;
		}
		String defaultSolution = "";
		switch (appType) {
			case DAAS:
				if (languageEnum == Language.CN) {
					defaultSolution = DEFAULT_SOLUTION_DAAS_CN;
				} else {
					defaultSolution = DEFAULT_SOLUTION_DAAS_EN;
				}
				break;
			case DRS:
			case DFS:
				if (languageEnum == Language.CN) {
					defaultSolution = DEFAULT_SOLUTION_DFS_CN;
				} else {
					defaultSolution = DEFAULT_SOLUTION_DFS_EN;
				}
				break;
			default:
				break;
		}
		return defaultSolution;
	}
}
