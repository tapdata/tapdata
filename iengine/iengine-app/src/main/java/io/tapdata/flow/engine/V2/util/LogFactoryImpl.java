package io.tapdata.flow.engine.V2.util;

import com.tapdata.entity.task.context.ProcessorBaseContext;
import io.tapdata.entity.logger.TapLog;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.flow.engine.V2.log.LogFactory;
import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.logger.Log;
import io.tapdata.flow.engine.V2.script.ObsScriptLogger;
import io.tapdata.observable.logging.ObsLoggerFactory;

/**
 * @author aplomb
 */
@Implementation(LogFactory.class)
public class LogFactoryImpl implements LogFactory {
	@Override
	public Log getLog(ProcessorBaseContext processorBaseContext) {
		return new ObsScriptLogger(ObsLoggerFactory.getInstance().getObsLogger(
				processorBaseContext.getTaskDto(),
				processorBaseContext.getNode().getId(),
				processorBaseContext.getNode().getName()
		));
	}
	@Override
	public Log getLog(TaskDto task, String nodeId, String nodeName) {
		return new ObsScriptLogger(ObsLoggerFactory.getInstance().getObsLogger(
				task,
				nodeId,
				nodeName
		));
	}

	@Override
	public Log getLog(TaskDto taskDto) {
		return new ObsScriptLogger(ObsLoggerFactory.getInstance().getObsLogger(taskDto));
	}

	@Override
	public Log getLog() {
		return new TapLog();
	}
}
