package io.tapdata.flow.engine.V2.util;

import io.tapdata.flow.engine.V2.task.OpType;
import io.tapdata.flow.engine.V2.task.operation.TaskOperation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 任务操作队列，作用：
 * <ol>
 *   <li>按任务ID去重（解决：积压情况下产生大量操作）</li>
 *   <li>启动操作低优先级（解决：停止超时，页面状态已停止，实际任务还在运行问题）</li>
 *   <li>先进先出（解决：高优任务先调度）</li>
 * </ol>
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/12/31 10:57 Create
 */
public class TaskOperationQueue {
	private static final Logger LOGGER = LogManager.getLogger(TaskOperationQueue.class);

	private final int capacity;
	private final Map<String, TaskOperation> operationMap;
	private final LinkedList<String> startIds;
	private final LinkedList<String> otherIds;

	public TaskOperationQueue(int capacity) {
		this.capacity = capacity;
		operationMap = new HashMap<>();
		startIds = new LinkedList<>();
		otherIds = new LinkedList<>();
	}

	public boolean offer(TaskOperation op) {
		synchronized (operationMap) {
			if (operationMap.size() >= capacity) {
				return false;
			}

			String taskId = op.getTaskId();
			if (taskId == null) {
				LOGGER.debug("Task id cannot be null");
				return false;
			}

			// 如果任务已存在操作，则直接返回
			TaskOperation existOp = operationMap.get(taskId);
			if (null != existOp) {
				if (existOp.getOpType() == op.getOpType()) {
					// 相同操作，返回 true
					return true;
				}
				LOGGER.debug("Task operation already exists, task id: {}", taskId);
				return false;
			}

			if (op.getOpType() == OpType.START) {
				if (startIds.offer(taskId)) {
					operationMap.put(taskId, op);
					return true;
				}
			} else if (otherIds.offer(taskId)) {
				operationMap.put(taskId, op);
				return true;
			}

			LOGGER.debug("The 'TaskOperation' queue is full. Skip the '{}' operation by task id '{}'", op.getOpType(), taskId);
			return false;
		}
	}

	public TaskOperation poll() {
		synchronized (operationMap) {
			String taskId = otherIds.poll();
			if (null == taskId) {
				taskId = startIds.poll();
			}
			if (null != taskId) {
				return operationMap.remove(taskId);
			}
			return null;
		}
	}

	public boolean offer(TaskOperation op, long timeout, TimeUnit unit) throws InterruptedException {
		long end = System.currentTimeMillis() + unit.toMillis(timeout);
		while (System.currentTimeMillis() < end) {
			if (offer(op)) {
				return true;
			}

			// 防止 CPU 空转
			TimeUnit.MILLISECONDS.sleep(500);
		}
		return false;
	}

	public TaskOperation poll(long timeout, TimeUnit unit) throws InterruptedException {
		long end = System.currentTimeMillis() + unit.toMillis(timeout);
		while (System.currentTimeMillis() < end) {
			TaskOperation op = poll();
			if (op != null) {
				return op;
			}

			// 防止 CPU 空转
			TimeUnit.MILLISECONDS.sleep(500);
		}
		return null;
	}
}
