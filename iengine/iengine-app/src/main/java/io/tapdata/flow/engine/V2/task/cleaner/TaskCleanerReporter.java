package io.tapdata.flow.engine.V2.task.cleaner;

import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.task.dto.TaskResetEventDto;

/**
 * @author samuel
 * @Description
 * @create 2022-10-14 14:35
 **/
public interface TaskCleanerReporter {
	void addEvent(ClientMongoOperator clientMongoOperator, TaskResetEventDto taskResetEventDto);
}
