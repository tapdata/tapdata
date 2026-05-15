package io.tapdata.exception;

import org.apache.commons.lang3.StringUtils;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * 异常工具类
 */
public class ExceptionUtil {
	private ExceptionUtil() {

	}

	/**
	 * 获取完整的堆栈信息字符串
	 *
	 * @param throwable
	 * @return
	 */
	public static String getStackString(Throwable throwable) {
		if (throwable == null) {
			return "";
		}
		StringWriter sw = new StringWriter();
		try (PrintWriter pw = new PrintWriter(sw)) {
			throwable.printStackTrace(pw);
			return sw.toString();
		}
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
