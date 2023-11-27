package io.tapdata.flow.engine.V2.exception;

import com.tapdata.constant.Log4jUtil;
import io.tapdata.error.TaskProcessorExCode_11;
import io.tapdata.exception.TapCodeException;

/**
 * @author samuel
 * @Description
 * @create 2023-11-24 10:12
 **/
public class ErrorHandleException extends TapCodeException {
	private Throwable originalException;

	public ErrorHandleException(Throwable cause, Throwable originalException) {
		super(TaskProcessorExCode_11.ERROR_HANDLE_FAILED, cause);
		this.originalException = originalException;
	}

	public Throwable getOriginalException() {
		return originalException;
	}

	@Override
	public String getMessage() {
		String msg = "Handle some exception failed, unable to stop the task in error status.";
		if (null != originalException) {
			msg += "Original exception: " + originalException + ". Stack: " + Log4jUtil.getStackString(originalException);
		}
		return msg;
	}
}
