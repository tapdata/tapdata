package io.tapdata.task;

import java.util.function.Consumer;

/**
 * @author jackin
 */
public interface Task {

	/**
	 * 初始化task实例方法，仅调用一次
	 *
	 * @param taskContext
	 */
	void initialize(TaskContext taskContext);

	/**
	 * task每次执行调用该方法
	 *
	 * @param callback 执行结果
	 */
	void execute(Consumer<TaskResult> callback);
}
