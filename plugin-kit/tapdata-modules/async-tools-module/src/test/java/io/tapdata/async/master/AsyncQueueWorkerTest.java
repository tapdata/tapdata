package io.tapdata.async.master;

import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.modules.api.async.master.*;
import io.tapdata.pdk.core.utils.test.AsyncTestBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.tapdata.entity.simplify.TapSimplify.*;

/**
 * @author aplomb
 */
public class AsyncQueueWorkerTest extends AsyncTestBase {
	@AfterEach
	public void tearDown() {
		AsyncMaster asyncMaster = InstanceFactory.instance(AsyncMaster.class);
		asyncMaster.destroyAsyncQueueWorker("Test");
	}
	@Test
	public void testAsyncQueueWorkerWithError() {
		AsyncMaster asyncMaster = InstanceFactory.instance(AsyncMaster.class);
		AsyncQueueWorker asyncQueueWorker = asyncMaster.createAsyncQueueWorker("Test");
		asyncQueueWorker.setQueueWorkerStateListener((id, fromState, toState) -> {
			System.out.println("id " + id + " from " + fromState + " to " + toState);
		});
		AtomicReference<AsyncJob> errorAsyncJob = new AtomicReference<>();
		AtomicReference<Throwable> theThrowable = new AtomicReference<>();
		AtomicReference<String> errorId = new AtomicReference<>();
		asyncQueueWorker.setAsyncJobErrorListener((id, asyncJob, throwable) -> {
			errorId.set(id);
			errorAsyncJob.set(asyncJob);
			theThrowable.set(throwable);
		});
		AtomicInteger counter = new AtomicInteger(0);
		JobContext initialContext = JobContext.create("initial");
		asyncQueueWorker.job("job1", previousJobContext -> {
			Assertions.assertEquals("initial", previousJobContext.getResult());
			counter.incrementAndGet();
			return JobContext.create("Hello world");
		}).job("job2", previousJobContext -> {
			throw new NullPointerException("npe");
//			return null;
		});
		asyncQueueWorker.start(initialContext, true);
		Assertions.assertEquals("job2", errorId.get());
		Assertions.assertNotNull(errorAsyncJob.get());
		Assertions.assertEquals("npe", theThrowable.get().getMessage());
	}

	@Test
	public void testAsyncQueueWorkerLoop() {
		AsyncMaster asyncMaster = InstanceFactory.instance(AsyncMaster.class);
		AsyncQueueWorker asyncQueueWorker = asyncMaster.createAsyncQueueWorker("Test");
		asyncQueueWorker.setQueueWorkerStateListener((id, fromState, toState) -> {
			System.out.println("id " + id + " from " + fromState + " to " + toState);
		});
		AtomicInteger counter = new AtomicInteger(0);
		JobContext initialContext = JobContext.create("initial").context("Context");
		asyncQueueWorker.job("job1", jobContext -> {
			Assertions.assertEquals("initial", jobContext.getResult());
			Assertions.assertEquals("Context", jobContext.getContext());
			List<String> strs = list("1", "2", "3", "4", "5");
			AtomicInteger listCounter = new AtomicInteger(0);
			jobContext.foreach(strs, s -> {
				listCounter.incrementAndGet();
				return null;
			});
			Assertions.assertEquals(strs.size(), listCounter.get());
			jobContext.foreach(strs, s -> {
				if(s.equals("2")) {
					return false;
				}
				if(s.equals("3")) {
					Assertions.fail();
				}
				return null;
			});
			Map<String, Object> map = map(entry("A", 1), entry("B", 2), entry("C", 3));
			AtomicInteger mapCounter = new AtomicInteger(0);
			jobContext.foreach(map, stringObjectEntry -> {
				mapCounter.incrementAndGet();
				return null;
			});
			Assertions.assertEquals(map.size(), mapCounter.get());
			jobContext.foreach(map, stringObjectEntry -> {
				if(stringObjectEntry.getKey().equals("B")) {
					return false;
				}
				if(stringObjectEntry.getKey().equals("C")) {
					Assertions.fail();
				}
				return null;
			});
			counter.incrementAndGet();
			return JobContext.create("Hello world");
		}).job("job2", jobContext -> {
			Assertions.assertEquals("Hello world", jobContext.getResult());
			Assertions.assertEquals("Context", jobContext.getContext());
			counter.incrementAndGet();
			return null;
		});
		asyncQueueWorker.start(initialContext, true);
		Assertions.assertEquals(2, counter.get());

		asyncQueueWorker.job("job3", jobContext -> {
			Assertions.assertEquals("Context", jobContext.getContext());
			Assertions.assertNull(jobContext.getResult());

			return JobContext.create("AAA");
		}).job("job4", jobContext -> {
			Assertions.assertEquals("AAA", jobContext.getResult());
			Assertions.assertEquals("Context", jobContext.getContext());
			return null;
		});
	}

	@Test
	public void testAsyncQueueWorkerLoopAsync() throws Throwable {
		AsyncMaster asyncMaster = InstanceFactory.instance(AsyncMaster.class);
		AsyncQueueWorker asyncQueueWorker = asyncMaster.createAsyncQueueWorker("Test");
		asyncQueueWorker.setQueueWorkerStateListener((id, fromState, toState) -> {
			System.out.println("id " + id + " from " + fromState + " to " + toState);
		});
		AtomicInteger counter = new AtomicInteger(0);
		JobContext initialContext = JobContext.create("initial").context("Context");
		asyncQueueWorker.job("job1", jobContext -> {
			$(() -> Assertions.assertEquals("initial", jobContext.getResult()));
			$(() -> Assertions.assertEquals("Context", jobContext.getContext()));
			List<String> strs = list("1", "2", "3", "4", "5");
			AtomicInteger listCounter = new AtomicInteger(0);
			jobContext.foreach(strs, s -> {
				listCounter.incrementAndGet();
				return null;
			});
			$(() -> Assertions.assertEquals(strs.size(), listCounter.get()));
			jobContext.foreach(strs, s -> {
				if(s.equals("2")) {
					return false;
				}
				if(s.equals("3")) {
					$(Assertions::fail);
				}
				return null;
			});
			Map<String, Object> map = map(entry("A", 1), entry("B", 2), entry("C", 3));
			AtomicInteger mapCounter = new AtomicInteger(0);
			jobContext.foreach(map, stringObjectEntry -> {
				mapCounter.incrementAndGet();
				return null;
			});
			Assertions.assertEquals(map.size(), mapCounter.get());
			jobContext.foreach(map, stringObjectEntry -> {
				if(stringObjectEntry.getKey().equals("B")) {
					return false;
				}
				if(stringObjectEntry.getKey().equals("C")) {
					$(Assertions::fail);
				}
				return null;
			});
			counter.incrementAndGet();
			return JobContext.create("Hello world");
		}).job("job2", jobContext -> {
			$(() -> Assertions.assertEquals("Hello world", jobContext.getResult()));
			$(() -> Assertions.assertEquals("Context", jobContext.getContext()));
			counter.incrementAndGet();
			completed();
			return null;
		});
		asyncQueueWorker.start(initialContext);
		waitCompleted(3);
	}

	@Test
	public void testCancelFirst() throws Throwable {
		AsyncMaster asyncMaster = InstanceFactory.instance(AsyncMaster.class);
		AsyncQueueWorker asyncQueueWorker = asyncMaster.createAsyncQueueWorker("Test");
		AtomicInteger counter = new AtomicInteger(0);
		JobContext initialContext = JobContext.create("initial").context("Context");
		asyncQueueWorker.job("job1", jobContext -> {
			new Thread(() -> {
				try {
					Thread.sleep(100L);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
				asyncQueueWorker.cancel("job1");
			}).start();
			jobContext.foreach(10000, integer -> {
				try {
					Thread.sleep(500L);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				return null;
			});

			return JobContext.create("Hello world");
		}).job("job2", jobContext -> {
			$(() -> Assertions.assertEquals("initial", jobContext.getResult()));
			$(() -> Assertions.assertEquals("Context", jobContext.getContext()));
			counter.incrementAndGet();
			completed();
			return null;
		});
		asyncQueueWorker.start(initialContext);
		waitCompleted(3);
	}

	@Test
	public void testCancelMiddle() throws Throwable {
		AsyncMaster asyncMaster = InstanceFactory.instance(AsyncMaster.class);
		AsyncQueueWorker asyncQueueWorker = asyncMaster.createAsyncQueueWorker("Test");
		AtomicInteger counter = new AtomicInteger(0);
		JobContext initialContext = JobContext.create("initial").context("Context");
		AtomicBoolean job2ShouldNotExecute = new AtomicBoolean(false);
		asyncQueueWorker.job("job1", jobContext -> {
			jobContext.foreach(1, integer -> {
				try {
					Thread.sleep(400L);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				return null;
			});

			return JobContext.create("Hello world");
		}).job("job2", jobContext -> {
			job2ShouldNotExecute.set(true);
			return null;
		}).job("job3", jobContext -> {
			$(() -> Assertions.assertFalse(job2ShouldNotExecute.get()));
			completed();
			return null;
		});
		asyncQueueWorker.start(initialContext);
		new Thread(() -> {
			try {
				Thread.sleep(100L);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
			asyncQueueWorker.cancel("job2");
		}).start();
		waitCompleted(3);
	}

	@Test
	public void testCancelAll() throws Throwable {
		AsyncMaster asyncMaster = InstanceFactory.instance(AsyncMaster.class);
		AsyncQueueWorker asyncQueueWorker = asyncMaster.createAsyncQueueWorker("Test");
		AtomicInteger counter = new AtomicInteger(0);
		JobContext initialContext = JobContext.create("initial").context("Context");
		AtomicBoolean job1ShouldNotExecute = new AtomicBoolean(false);
		AtomicBoolean job2ShouldNotExecute = new AtomicBoolean(false);
		AtomicBoolean job3ShouldNotExecute = new AtomicBoolean(false);
		asyncQueueWorker.job("job1", jobContext -> {
			jobContext.foreach(1, integer -> {
				try {
					Thread.sleep(400L);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				return null;
			});
			job1ShouldNotExecute.set(true);
			return JobContext.create("Hello world");
		}).job("job2", jobContext -> {
			job2ShouldNotExecute.set(true);
			return null;
		}).job("job3", jobContext -> {
			job3ShouldNotExecute.set(true);
			return null;
		});
		asyncQueueWorker.start(initialContext);
		new Thread(() -> {
			try {
				Thread.sleep(100L);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
			asyncQueueWorker.cancelAll();
			asyncQueueWorker.job("final", jobContext -> {
				$(() -> Assertions.assertFalse(job1ShouldNotExecute.get()));
				$(() -> Assertions.assertFalse(job2ShouldNotExecute.get()));
				$(() -> Assertions.assertFalse(job3ShouldNotExecute.get()));
				completed();
				return null;
			});
		}).start();
		waitCompleted(3);
	}

	@Test
	public void testJumpTo() throws Throwable {
		AsyncMaster asyncMaster = InstanceFactory.instance(AsyncMaster.class);
		AsyncQueueWorker asyncQueueWorker = asyncMaster.createAsyncQueueWorker("Test");
		AtomicInteger counter = new AtomicInteger(0);
		JobContext initialContext = JobContext.create("initial").context("Context");
		AtomicBoolean job2ShouldNotExecute = new AtomicBoolean(false);
		asyncQueueWorker.job("job1", jobContext -> {
			return JobContext.create("Hello world").jumpToId("job3");
		}).job("job2", jobContext -> {
			job2ShouldNotExecute.set(true);
			return null;
		}).job("job3", jobContext -> {
			$(() -> Assertions.assertFalse(job2ShouldNotExecute.get()));
			completed();
			return null;
		});
		asyncQueueWorker.start(initialContext);
		waitCompleted(3);
	}

	@Test
	public void testEnterLeaveLongIdleState() throws Throwable {
		AsyncMaster asyncMaster = InstanceFactory.instance(AsyncMaster.class);
		AsyncQueueWorker asyncQueueWorker = asyncMaster.createAsyncQueueWorker("Test");
		AtomicBoolean enterLongIdle = new AtomicBoolean(false);
		asyncQueueWorker.setQueueWorkerStateListener((id, fromState, toState) -> {
			System.out.println("id " + id + " from " + fromState + " to " + toState);
			if(!enterLongIdle.get() && fromState == QueueWorkerStateListener.STATE_IDLE && toState == QueueWorkerStateListener.STATE_LONG_IDLE) {
				enterLongIdle.set(true);
				asyncQueueWorker.job("aaa", jobContext -> {
					return null;
				});
			} else if(enterLongIdle.get() && fromState == QueueWorkerStateListener.STATE_RUNNING && toState == QueueWorkerStateListener.STATE_IDLE) {
				completed();
			}
		});
		JobContext initialContext = JobContext.create("initial").context("Context");
		asyncQueueWorker.job("job1", jobContext -> {
			return JobContext.create("Hello world");
		}).job("job2", jobContext -> {
			return null;
		}).job("job3", jobContext -> {
			return null;
		});
		asyncQueueWorker.start(initialContext);
		waitCompleted(6);
	}
	@Test
	public void testSchedule() throws Throwable {
		AsyncMaster asyncMaster = InstanceFactory.instance(AsyncMaster.class);
		AsyncQueueWorker asyncQueueWorker = asyncMaster.createAsyncQueueWorker("Test");
		asyncQueueWorker.setQueueWorkerStateListener((id, fromState, toState) -> {
//			System.out.println("id " + id + " from " + fromState + " to " + toState);
		});
		AtomicReference<AsyncJob> errorAsyncJob = new AtomicReference<>();
		AtomicReference<Throwable> theThrowable = new AtomicReference<>();
		AtomicReference<String> errorId = new AtomicReference<>();
		asyncQueueWorker.setAsyncJobErrorListener((id, asyncJob, throwable) -> {
			errorId.set(id);
			errorAsyncJob.set(asyncJob);
			theThrowable.set(throwable);
			completed();
		});
		AtomicInteger counter = new AtomicInteger(0);
		JobContext initialContext = JobContext.create("initial");
		asyncQueueWorker.job("job1", previousJobContext -> {
//			$(() -> Assertions.assertEquals("initial", previousJobContext.getResult()));
			counter.incrementAndGet();
			System.out.println("job1 " + Thread.currentThread());
			return JobContext.create("Hello world");
		}).job("job2", previousJobContext -> {
			System.out.println("job2 " + Thread.currentThread());
			throw new NullPointerException("npe");
//			return null;
		});
		asyncQueueWorker.start(initialContext, 0, 5000);
//		Assertions.assertEquals("job2", errorId.get());
//		Assertions.assertNotNull(errorAsyncJob.get());
//		Assertions.assertEquals("npe", theThrowable.get().getMessage());
		waitCompleted(6);
	}

	@Test
	public void testScheduleJobs() throws Throwable {
		AsyncMaster asyncMaster = InstanceFactory.instance(AsyncMaster.class);
		AsyncQueueWorker asyncQueueWorker = asyncMaster.createAsyncQueueWorker("Test");
		asyncQueueWorker.setQueueWorkerStateListener((id, fromState, toState) -> {
//			System.out.println("id " + id + " from " + fromState + " to " + toState);
		});
		asyncQueueWorker.setAsyncJobErrorListener((id, asyncJob, throwable) -> {

		});
		AtomicInteger counter = new AtomicInteger(0);
		JobContext initialContext = JobContext.create("initial");
		asyncQueueWorker.job("job1", previousJobContext -> {
//			$(() -> Assertions.assertEquals("initial", previousJobContext.getResult()));
			counter.incrementAndGet();
			System.out.println("job1 " + Thread.currentThread());
			return JobContext.create("Hello world");
		}).job("job2", previousJobContext -> {
			counter.incrementAndGet();
			System.out.println("job2 " + Thread.currentThread());
			return null;
		});
		asyncQueueWorker.start(initialContext, 0, 500);
		asyncQueueWorker.job("job3", jobContext -> {
			counter.incrementAndGet();
			System.out.println("job3 " + Thread.currentThread());
			return null;
		});
		new Thread(() -> {
			try {
				Thread.sleep(700L);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
			$(() -> Assertions.assertEquals(6, counter.get()));
			completed();
		}).start();

		waitCompleted(6);
	}

	@Test
	public void testScheduleJump() throws Throwable {
		AsyncMaster asyncMaster = InstanceFactory.instance(AsyncMaster.class);
		AsyncQueueWorker asyncQueueWorker = asyncMaster.createAsyncQueueWorker("Test");
		asyncQueueWorker.setQueueWorkerStateListener((id, fromState, toState) -> {
//			System.out.println("id " + id + " from " + fromState + " to " + toState);
		});
		asyncQueueWorker.setAsyncJobErrorListener((id, asyncJob, throwable) -> {

		});
		AtomicInteger counter = new AtomicInteger(0);
		JobContext initialContext = JobContext.create("initial");
		asyncQueueWorker.job("job1", previousJobContext -> {
//			$(() -> Assertions.assertEquals("initial", previousJobContext.getResult()));
			counter.incrementAndGet();
			System.out.println("job1 " + Thread.currentThread());
			return JobContext.create("Hello world").jumpToId("job3");
		}).job("job2", previousJobContext -> {
			counter.incrementAndGet();
			System.out.println("job2 " + Thread.currentThread());
			$(Assertions::fail);
			return null;
		});
		asyncQueueWorker.start(initialContext, 0, 500);
		asyncQueueWorker.job("job3", jobContext -> {
			counter.incrementAndGet();
			System.out.println("job3 " + Thread.currentThread());
			return null;
		});
		new Thread(() -> {
			try {
				Thread.sleep(700L);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
			$(() -> Assertions.assertEquals(4, counter.get()));
			completed();
		}).start();

		waitCompleted(6);
	}


	@Test
	public void testExternalJob() throws Throwable {
		AsyncMaster asyncMaster = InstanceFactory.instance(AsyncMaster.class);
		AsyncQueueWorker asyncQueueWorker = asyncMaster.createAsyncQueueWorker("Test");
		asyncQueueWorker.setQueueWorkerStateListener((id, fromState, toState) -> {
			System.out.println("id " + id + " from " + fromState + " to " + toState);
		});
		asyncQueueWorker.setAsyncJobErrorListener((id, asyncJob, throwable) -> {

		});
		AtomicInteger counter = new AtomicInteger(0);
		JobContext initialContext = JobContext.create("initial");
		asyncQueueWorker.job("job1", previousJobContext -> {
//			$(() -> Assertions.assertEquals("initial", previousJobContext.getResult()));
			counter.incrementAndGet();
			System.out.println("job1 " + Thread.currentThread());
			return JobContext.create("Hello world");
		}).externalJob("batchRead", previousJobContext -> {
			counter.incrementAndGet();
			System.out.println("job2 " + Thread.currentThread());
			$(() -> Assertions.assertEquals(1, ((TapInsertRecordEvent)((List)previousJobContext.getResult()).get(0)).getAfter().get("a")));
			completed();
			return null;
		});
		asyncQueueWorker.start(initialContext);

		waitCompleted(6);
	}
}
