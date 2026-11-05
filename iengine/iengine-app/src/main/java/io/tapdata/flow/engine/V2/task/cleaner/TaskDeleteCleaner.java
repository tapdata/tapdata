package io.tapdata.flow.engine.V2.task.cleaner;

import io.tapdata.observable.logging.appender.AppenderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author samuel
 * @Description
 * @create 2022-10-14 10:48
 **/
public class TaskDeleteCleaner extends TaskCleaner {
	private static final Logger LOGGER = LoggerFactory.getLogger(TaskDeleteCleaner.class);

	public TaskDeleteCleaner(TaskCleanerContext taskCleanerContext) {
		super(taskCleanerContext);
	}

	@Override
	protected void beforeEndClean() throws TaskCleanerException {
		try {
			AppenderFactory.getInstance().deleteTaskCache(taskCleanerContext.getTaskId());
		} catch (RuntimeException e) {
			LOGGER.warn("Delete task CacheObserveLogs failed, taskId={}", taskCleanerContext.getTaskId(), e);
		}
	}
}
