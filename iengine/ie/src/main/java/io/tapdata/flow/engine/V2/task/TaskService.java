package io.tapdata.flow.engine.V2.task;


import java.util.List;

/**
 * operate task
 *
 * @author jackin
 * @date 2021/12/1 3:47 PM
 **/
public interface TaskService<T> {

	/**
	 * start task
	 *
	 * @param task
	 * @return
	 */
	TaskClient<T> startTask(T task);

	/**
	 * Returns the active or last started task client with the given taskId or {@code
	 * null}
	 *
	 * @param taskId
	 */
	TaskClient<T> getTaskClient(String taskId);

	/**
	 * Returns all task clients
	 */
	List<TaskClient<T>> getTaskClients();
}
