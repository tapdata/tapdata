package io.tapdata.async.master;

import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.pdk.core.utils.test.AsyncTestBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author aplomb
 */
public class AsyncParallelWorkerTest extends AsyncTestBase {
	@AfterEach
	public void tearDown() {
		JobMaster asyncMaster = InstanceFactory.instance(JobMaster.class);
		ParallelWorker parallelWorker = asyncMaster.destroyAsyncParallelWorker("Test");
		Assertions.assertNull(parallelWorker);
	}

	@Test
	public void testAsyncParallelWorker() throws Throwable {
		JobMaster asyncMaster = InstanceFactory.instance(JobMaster.class);
		ParallelWorker asyncParallelWorker = asyncMaster.createAsyncParallelWorker("Test", 1);
		asyncParallelWorker.setParallelWorkerStateListener((id, fromState, toState) -> {
			System.out.println("id " + id + " from " + fromState + " to " + toState);
		});
		List<String> ids = new ArrayList<>();
		asyncParallelWorker.job("1", null, asyncQueueWorker -> asyncQueueWorker.job("1", jobContext -> {
			ids.add(asyncQueueWorker.getId());
			return null;
		}).finished());
		asyncParallelWorker.job("2", null, asyncQueueWorker -> asyncQueueWorker.job("1", jobContext -> {
			ids.add(asyncQueueWorker.getId());
			return null;
		}).finished());
		asyncParallelWorker.job("3", null, asyncQueueWorker -> asyncQueueWorker.job("1", jobContext -> {
			ids.add(asyncQueueWorker.getId());
			return null;
		}).finished());
		asyncParallelWorker.job("4", null, asyncQueueWorker -> asyncQueueWorker.job("1", jobContext -> {
			ids.add(asyncQueueWorker.getId());
			return null;
		}).finished());
		asyncParallelWorker.start();
		new Thread(() -> {
			try {
				Thread.sleep(100L);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
			$(() -> Assertions.assertArrayEquals(new String[]{"1", "2", "3", "4"}, ids.toArray()));
			completed();
		}).start();
		waitCompleted(3111111);
	}

	@Test
	public void testAsyncParallelWorkerFinished() throws Throwable {
		JobMaster asyncMaster = InstanceFactory.instance(JobMaster.class);
		ParallelWorker asyncParallelWorker = asyncMaster.createAsyncParallelWorker("Test", 1);
		asyncParallelWorker.setParallelWorkerStateListener((id, fromState, toState) -> {
			System.out.println("id " + id + " from " + fromState + " to " + toState);
		});
		List<String> ids = new ArrayList<>();
		asyncParallelWorker.job("1", null, asyncQueueWorker -> asyncQueueWorker.job("1", jobContext -> {
			ids.add(asyncQueueWorker.getId());
			return null;
		}).finished());
		asyncParallelWorker.job("2", null, asyncQueueWorker -> asyncQueueWorker.job("1", jobContext -> {
			ids.add(asyncQueueWorker.getId());
			return null;
		}).finished());
		asyncParallelWorker.job("3", null, asyncQueueWorker -> asyncQueueWorker.job("1", jobContext -> {
			ids.add(asyncQueueWorker.getId());
			asyncParallelWorker.finished(() -> {
				try {
					Thread.sleep(100L);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
				$(() -> Assertions.assertArrayEquals(new String[]{"1", "2", "3", "4"}, ids.toArray()));
				Assertions.assertEquals(ParallelWorkerStateListener.STATE_FINISHED, asyncParallelWorker.getState());
				try {
					asyncParallelWorker.job("5", null, asyncQueueWorker1 -> {
						$(Assertions::fail);
					});
					$(Assertions::fail);
				} catch(Throwable throwable) {
					System.out.println("throwable " + throwable.getMessage());
				}
				completed();
			});
			return null;
		}).finished());
		asyncParallelWorker.job("4", null, asyncQueueWorker -> asyncQueueWorker.job("1", jobContext -> {
			ids.add(asyncQueueWorker.getId());
			return null;
		}).finished());
		asyncParallelWorker.start();

		waitCompleted(3);
	}

}
