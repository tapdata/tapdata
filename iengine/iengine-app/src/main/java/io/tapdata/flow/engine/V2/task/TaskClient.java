package io.tapdata.flow.engine.V2.task;


import io.tapdata.task.Task;

/**
 * A handle to Jet computation job created by starting a {@link Task} to the cluster
 */
public interface TaskClient<T> {

	/**
	 * return task config.
	 */
	T getTask();

	/**
	 * return task status
	 */
	String getStatus();

	/**
	 * cache task
	 *
	 * @return
	 */
	default String getCacheName() {
		return null;
	}

	/**
	 * stop task
	 */
	boolean stop();

	void join();

	default void error(Throwable throwable) {

	}

	default Throwable getError() {
		return null;
	}
}
