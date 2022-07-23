package io.tapdata.aspect.task;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.error.TapAPIErrorCodes;
import io.tapdata.error.EngineErrorCodes;

public class StopTaskException extends CoreException {
	public StopTaskException() {
		super(EngineErrorCodes.STOP_TASK, "Stop task", LEVEL_ERROR);
	}
}
