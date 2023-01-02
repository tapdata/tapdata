package io.tapdata.flow.engine.V2.log;

import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.logger.Log;

/**
 * @author aplomb
 */
public interface LogFactory {
	Log getLog(ProcessorBaseContext processorBaseContext);

	Log getLog(TaskDto task, String nodeId, String nodeName);

	Log getLog(TaskDto taskDto);

	Log getLog();
}
