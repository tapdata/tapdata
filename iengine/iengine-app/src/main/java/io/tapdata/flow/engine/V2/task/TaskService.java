package io.tapdata.flow.engine.V2.task;


import com.tapdata.tm.commons.task.dto.TaskDto;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

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

	TaskClient<TaskDto> startTestTask(TaskDto taskDto);

	default TaskClient<TaskDto> startTestTask(TaskDto taskDto, AtomicReference<Object> clientResult){
		return null;
	}

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
