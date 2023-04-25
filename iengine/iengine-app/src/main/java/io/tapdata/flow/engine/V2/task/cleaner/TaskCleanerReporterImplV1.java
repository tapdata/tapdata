package io.tapdata.flow.engine.V2.task.cleaner;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.task.dto.TaskResetEventDto;

/**
 * @author samuel
 * @Description
 * @create 2022-10-14 15:22
 **/
public class TaskCleanerReporterImplV1 implements TaskCleanerReporter {
	@Override
	public void addEvent(ClientMongoOperator clientMongoOperator, TaskResetEventDto taskResetEventDto) {
		clientMongoOperator.insertOne(taskResetEventDto, ConnectorConstant.TASK_RESET_LOGS_COLLECTION);
	}
}
