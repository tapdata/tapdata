package io.tapdata.exception;

import org.apache.commons.lang3.StringUtils;

/**
 * 异常工具类
 */
public class ExceptionUtil {
	private ExceptionUtil() {

	}

	/**
	 * 获取导致异常的错误信息
	 *
	 * @param throwable
	 * @return
	 */
	public static String getMessage(Throwable throwable) {
		String message = throwable.getMessage();

		if (StringUtils.isEmpty(message) && throwable.getCause() != null) {
			message = getMessage(throwable.getCause());
		}

		return message;
	}


}
