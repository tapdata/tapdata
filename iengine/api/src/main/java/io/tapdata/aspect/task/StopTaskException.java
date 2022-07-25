package io.tapdata.aspect.task;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.error.TapAPIErrorCodes;
import io.tapdata.error.EngineErrorCodes;

public class StopTaskException extends CoreException {
	public StopTaskException(String message, Throwable throwable) {
		super(EngineErrorCodes.STOP_TASK, message, LEVEL_ERROR);
		initCause(throwable);
	}
	public StopTaskException(String message) {
		this(message, null);
	}
	public StopTaskException(Throwable throwable) {
		this("Stop task", throwable);
	}
}
