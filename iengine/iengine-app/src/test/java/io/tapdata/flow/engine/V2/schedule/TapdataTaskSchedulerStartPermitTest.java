package io.tapdata.flow.engine.V2.schedule;

import com.tapdata.tm.commons.task.dto.TaskDto;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.concurrent.Semaphore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;

/**
 * TAP-12108 回归测试：启动限流信号量必须 acquire/release 严格成对。
 *
 * <p>旧实现（applyBatchStartTaskRateLimit）只在"凑满一整批 batchSize"时才批量 release，
 * 且初始许可数与运行期归还阈值可能不一致，导致 release 从不触发、许可只减不还，
 * 最终耗尽为 0，所有任务永久阻塞在 acquire，单通道消费线程随之卡死，任务卡在"启动中"。
 * 本测试锁定修复后的两点契约：<br>
 * 1) 每次启动结束都归还许可，反复启停后许可池不泄漏；<br>
 * 2) 许可用尽时新的启动会阻塞（并发上限生效），一旦有任务归还许可即被放行（不再死锁）。
 */
public class TapdataTaskSchedulerStartPermitTest {

	private TapdataTaskScheduler schedulerWithSemaphore(int permits) {
		TapdataTaskScheduler scheduler = mock(TapdataTaskScheduler.class);
		ReflectionTestUtils.setField(scheduler, "startTaskSemaphore", new Semaphore(permits));
		doCallRealMethod().when(scheduler).applyBatchStartTaskRateLimit(any());
		doCallRealMethod().when(scheduler).releaseStartTaskPermit();
		return scheduler;
	}

	@Test
	@DisplayName("TAP-12108: acquire/release 成对，反复启停后许可不泄漏")
	void permitReturnedAfterEachStart_noLeak() {
		int permits = 2;
		TapdataTaskScheduler scheduler = schedulerWithSemaphore(permits);
		Semaphore sem = (Semaphore) ReflectionTestUtils.getField(scheduler, "startTaskSemaphore");
		TaskDto task = mock(TaskDto.class);

		// 远多于许可数的启停循环；旧实现会把许可逐步漏光直至 0
		for (int i = 0; i < 100; i++) {
			scheduler.applyBatchStartTaskRateLimit(task);
			scheduler.releaseStartTaskPermit();
		}

		assertEquals(permits, sem.availablePermits(),
			"每次启动都应归还许可，100 次启停后可用许可应回到初始值");
	}

	@Test
	@DisplayName("TAP-12108: 许可用尽时阻塞，release 后立即放行（旧实现 release 从不触发导致死锁）")
	void releaseUnblocksWaiter() throws Exception {
		TapdataTaskScheduler scheduler = schedulerWithSemaphore(1);
		TaskDto task = mock(TaskDto.class);

		scheduler.applyBatchStartTaskRateLimit(task); // 占用唯一许可

		Thread waiter = new Thread(() -> scheduler.applyBatchStartTaskRateLimit(task), "permit-waiter");
		waiter.setDaemon(true);
		waiter.start();
		waiter.join(500);
		assertTrue(waiter.isAlive(), "许可已用尽，第二个启动应被阻塞（并发上限生效）");

		scheduler.releaseStartTaskPermit(); // 归还许可 → 阻塞者应被唤醒
		waiter.join(2000);
		assertFalse(waiter.isAlive(), "release 后阻塞的启动应立即放行；旧实现只在凑满整批才归还，会永久阻塞");

		scheduler.releaseStartTaskPermit(); // 清理 waiter 占用的许可
	}
}
