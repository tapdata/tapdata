package io.tapdata.flow.engine.V2.cleaner;

import com.tapdata.constant.Log4jUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.StringJoiner;

/**
 * @author samuel
 * @Description
 * @create 2024-01-03 12:19
 **/
public class CleanResult {

	private int result;
	private String errorMessage;
	private String errorStack;

	private CleanResult(int result) {
		this.result = result;
	}

	private CleanResult(int result, String errorMessage, String errorStack) {
		this.result = result;
		this.errorMessage = errorMessage;
		this.errorStack = errorStack;
	}

	public static CleanResult success() {
		return new CleanResult(CleanerConstant.CLEAN_RESULT_SUCCESS);
	}

	public static CleanResult fail(Exception e) {
		if (null == e) {
			throw new IllegalArgumentException("Exception cannot be null");
		}
		String message = e.getMessage();
		message = StringUtils.isBlank(message) ? "No error message" : message;
		String stackString = Log4jUtil.getStackString(e);
		return new CleanResult(CleanerConstant.CLEAN_RESULT_FAIL, message, stackString);
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", CleanResult.class.getSimpleName() + "[", "]")
				.add("result=" + result)
				.add("errorMessage='" + errorMessage + "'")
				.add("errorStack='" + errorStack + "'")
				.toString();
	}
}
