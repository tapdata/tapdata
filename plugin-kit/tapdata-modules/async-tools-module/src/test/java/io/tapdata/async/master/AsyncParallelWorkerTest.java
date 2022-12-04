package io.tapdata.async.master;

import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.pdk.core.utils.test.AsyncTestBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * @author aplomb
 */
public class AsyncParallelWorkerTest extends AsyncTestBase {
	@AfterEach
	public void tearDown() {
		AsyncMaster asyncMaster = InstanceFactory.instance(AsyncMaster.class);
		asyncMaster.destroyAsyncParallelWorker("Test");
	}

	@Test
	public void testAsyncParallelWorker() throws Throwable {
		AsyncMaster asyncMaster = InstanceFactory.instance(AsyncMaster.class);
		AsyncParallelWorker asyncParallelWorker = asyncMaster.createAsyncParallelWorker("Test", 1);

		List<String> ids = new ArrayList<>();
		asyncParallelWorker.start("1", null, asyncQueueWorker -> asyncQueueWorker.job("1", jobContext -> {
			ids.add(asyncQueueWorker.getId());
			return null;
		}));
		asyncParallelWorker.start("2", null, asyncQueueWorker -> asyncQueueWorker.job("1", jobContext -> {
			ids.add(asyncQueueWorker.getId());
			return null;
		}));
		asyncParallelWorker.start("3", null, asyncQueueWorker -> asyncQueueWorker.job("1", jobContext -> {
			ids.add(asyncQueueWorker.getId());
			return null;
		}));
		asyncParallelWorker.start("4", null, asyncQueueWorker -> asyncQueueWorker.job("1", jobContext -> {
			ids.add(asyncQueueWorker.getId());
			return null;
		}));
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
}
