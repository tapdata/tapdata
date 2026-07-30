package io.tapdata.flow.engine.V2.task.cleaner;

import io.tapdata.observable.logging.appender.AppenderFactory;

/**
 * @author samuel
 * @Description
 * @create 2022-10-14 10:48
 **/
public class TaskDeleteCleaner extends TaskCleaner {
	public TaskDeleteCleaner(TaskCleanerContext taskCleanerContext) {
		super(taskCleanerContext);
	}

	@Override
	protected void beforeEndClean() throws TaskCleanerException {
		try {
			AppenderFactory.getInstance().deleteTaskCache(taskCleanerContext.getTaskId());
		} catch (RuntimeException e) {
			throw new TaskCleanerException(
					"Delete task CacheObserveLogs failed: " + taskCleanerContext.getTaskId(),
					e,
					true);
		}
	}
}
