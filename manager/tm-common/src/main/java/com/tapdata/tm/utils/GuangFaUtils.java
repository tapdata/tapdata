package com.tapdata.tm.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

/**
 * 广发工具类，用于识别、处理广发相关定制逻辑
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/4/22 14:39 Create
 */
public class GuangFaUtils {
	private static final Logger LOGGER = LoggerFactory.getLogger(GuangFaUtils.class);

	private static final String ENV_PROPERTY_NAME = "TAPD8_GUANG_FA";
	private static final String ENV_PROPERTY_ACCESS_CODE = "TAPD8_ACCESS_CODE";
	private static Boolean isGuangFa = null;

	private GuangFaUtils() {
	}

	private static String getConf(String key) {
		String tmp = System.getenv(key);
		if (null == tmp) {
			tmp = System.getProperty(key);
		}
		return tmp;
	}

	private static String getConf(String key, String def) {
		String tmp = getConf(key);
		if (null == tmp) {
			tmp = def;
		}
		return tmp;
	}

	public static boolean isGuangFa() {
		if (null != isGuangFa) return isGuangFa;

		synchronized (GuangFaUtils.class) {
			if (null == isGuangFa) {
				String guangfaTag = getConf(ENV_PROPERTY_NAME);
				isGuangFa = null != guangfaTag;
				if (isGuangFa) {
					LOGGER.info("Use GuangFa configuration.");
				}
			}
		}
		return isGuangFa;
	}

	public static boolean setAccessCode(Consumer<String> setAccessCode) {
		if (isGuangFa()) {
			String accessCode = getConf(ENV_PROPERTY_ACCESS_CODE, "3324cfdf-7d3e-4792-bd32-571638d4562f");
			setAccessCode.accept(accessCode);
			return true;
		}
		return false;
	}
}
