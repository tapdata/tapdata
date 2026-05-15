package io.tapdata.flow.engine.V2.util;

import io.tapdata.flow.engine.V2.task.OpType;
import io.tapdata.flow.engine.V2.task.operation.TaskOperation;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2026/1/4 09:14 Create
 */
class TaskOperationQueueTest {

	@Test
	void test_FIFO() throws Exception {
		int counts = 10;
		List<TaskOperation> taskOps = new ArrayList<>();
		for (int i = 0; i < counts; i++) {
			taskOps.add(create(OpType.START));
		}

		TaskOperationQueue queue = new TaskOperationQueue(counts);
		for (TaskOperation op : taskOps) {
			Assertions.assertTrue(queue.offer(op, 1, TimeUnit.SECONDS));
		}

		for (TaskOperation op : taskOps) {
			TaskOperation newOp = queue.poll(1, TimeUnit.SECONDS);
			Assertions.assertNotNull(newOp);
			Assertions.assertEquals(op, newOp);
		}
	}

	@Test
	void test_taskIdUnique() {
		String taskId = new ObjectId().toHexString();
		TaskOperationQueue queue = new TaskOperationQueue(10);

		Assertions.assertTrue(queue.offer(create(OpType.START, taskId)), "offer first");
		Assertions.assertTrue(queue.offer(create(OpType.START, taskId)), "offer the same operation");
		Assertions.assertFalse(queue.offer(create(OpType.STOP, taskId)), "offer difference operation");
	}

	@Test
	void test_outOfCapacity() {
		int capacity = 5;
		TaskOperationQueue queue = new TaskOperationQueue(capacity);

		TaskOperation op;
		for (int i = 0; i < capacity; i++) {
			op = create(OpType.START);
			Assertions.assertTrue(queue.offer(op));
		}

		op = create(OpType.START);
		Assertions.assertFalse(queue.offer(op));
	}

	@Test
	void test_startSecondPriority() throws Exception {
		// 模拟数据
		TaskOperationQueue queue = new TaskOperationQueue(10);

		TaskOperation startOp = create(OpType.START);
		Assertions.assertTrue(queue.offer(startOp));

		List<TaskOperation> otherOpList = new ArrayList<>();
		for (OpType opType : OpType.values()) {
			if (opType == OpType.START) continue;

			TaskOperation op = create(opType);
			Assertions.assertTrue(queue.offer(op));
			otherOpList.add(op);
		}

		// 验证顺序
		for (TaskOperation op : otherOpList) {
			Assertions.assertEquals(op, queue.poll());
		}

		// 验证顺序（启动优先级最低）
		Assertions.assertEquals(startOp, queue.poll());
	}

	static TaskOperation create(OpType type) {
		String taskId = new ObjectId().toHexString();
		return create(type, taskId);
	}

	static TaskOperation create(OpType type, String taskId) {
		return new TaskOperation(type) {
			@Override
			public String getTaskId() {
				return taskId;
			}
		};
	}
}
