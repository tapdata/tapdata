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
		AsyncMaster asyncMaster = InstanceFactory.instance(AsyncMaster.class);
		AsyncParallelWorker parallelWorker = asyncMaster.destroyAsyncParallelWorker("Test");
		Assertions.assertNull(parallelWorker);
	}

	@Test
	public void testAsyncParallelWorker() throws Throwable {
		AsyncMaster asyncMaster = InstanceFactory.instance(AsyncMaster.class);
		AsyncParallelWorker asyncParallelWorker = asyncMaster.createAsyncParallelWorker("Test", 1);
		asyncParallelWorker.setParallelWorkerStateListener((id, fromState, toState) -> {
			System.out.println("id " + id + " from " + fromState + " to " + toState);
		});
		List<String> ids = new ArrayList<>();
		asyncParallelWorker.job("1", null, asyncQueueWorker -> asyncQueueWorker.job("1", jobContext -> {
			ids.add(asyncQueueWorker.getId());
			return null;
		}));
		asyncParallelWorker.job("2", null, asyncQueueWorker -> asyncQueueWorker.job("1", jobContext -> {
			ids.add(asyncQueueWorker.getId());
			return null;
		}));
		asyncParallelWorker.job("3", null, asyncQueueWorker -> asyncQueueWorker.job("1", jobContext -> {
			ids.add(asyncQueueWorker.getId());
			return null;
		}));
		asyncParallelWorker.job("4", null, asyncQueueWorker -> asyncQueueWorker.job("1", jobContext -> {
			ids.add(asyncQueueWorker.getId());
			return null;
		}));
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
		waitCompleted(3);
	}

	@Test
	public void testAsyncParallelWorkerStatusLongIdle() throws Throwable {
		AsyncMaster asyncMaster = InstanceFactory.instance(AsyncMaster.class);
		AsyncParallelWorker asyncParallelWorker = asyncMaster.createAsyncParallelWorker("Test", 1);
		AtomicInteger lastState = new AtomicInteger();
		asyncParallelWorker.setParallelWorkerStateListener((id, fromState, toState) -> {
			System.out.println("id " + id + " from " + fromState + " to " + toState);
			lastState.set(toState);
		});
		List<String> ids = new ArrayList<>();
		asyncParallelWorker.job("1", null, asyncQueueWorker -> asyncQueueWorker.job("1", jobContext -> {
			ids.add(asyncQueueWorker.getId());
			return null;
		}));
		asyncParallelWorker.start();
		new Thread(() -> {
			try {
				Thread.sleep(200L);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
			$(() -> Assertions.assertEquals(ParallelWorkerStateListener.STATE_IDLE, lastState.get()));
			try {
				Thread.sleep(1200L);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
			$(() -> Assertions.assertEquals(ParallelWorkerStateListener.STATE_LONG_IDLE, lastState.get()));
			completed();
		}).start();
		waitCompleted(3);
	}

}
