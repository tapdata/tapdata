package io.tapdata.async.master;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.utils.InstanceFactory;
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
		JobMaster asyncMaster = InstanceFactory.instance(JobMaster.class);
		asyncMaster.destroyAsyncQueueWorker("Test");
	}
	@Test
	public void testAsyncQueueWorkerWithError() {
		JobMaster asyncMaster = InstanceFactory.instance(JobMaster.class);
		QueueWorker asyncQueueWorker = asyncMaster.createAsyncQueueWorker("Test");
		asyncQueueWorker.setQueueWorkerStateListener((id, fromState, toState) -> {
			System.out.println("id " + id + " from " + fromState + " to " + toState);
		});
		AtomicReference<JobBase> errorAsyncJob = new AtomicReference<>();
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
		JobMaster asyncMaster = InstanceFactory.instance(JobMaster.class);
		QueueWorker asyncQueueWorker = asyncMaster.createAsyncQueueWorker("Test");
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
		JobMaster asyncMaster = InstanceFactory.instance(JobMaster.class);
		QueueWorker asyncQueueWorker = asyncMaster.createAsyncQueueWorker("Test");
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
		JobMaster asyncMaster = InstanceFactory.instance(JobMaster.class);
		QueueWorker asyncQueueWorker = asyncMaster.createAsyncQueueWorker("Test");
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
		JobMaster asyncMaster = InstanceFactory.instance(JobMaster.class);
		QueueWorker asyncQueueWorker = asyncMaster.createAsyncQueueWorker("Test");
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
		JobMaster asyncMaster = InstanceFactory.instance(JobMaster.class);
		QueueWorker asyncQueueWorker = asyncMaster.createAsyncQueueWorker("Test");
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
		JobMaster asyncMaster = InstanceFactory.instance(JobMaster.class);
		QueueWorker asyncQueueWorker = asyncMaster.createAsyncQueueWorker("Test");
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
	public void testExternalJob() throws Throwable {
		JobMaster asyncMaster = InstanceFactory.instance(JobMaster.class);
		QueueWorker asyncQueueWorker = asyncMaster.createAsyncQueueWorker("Test");
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

	@Test
	public void testPendingJobWithoutJump() throws Throwable {
		JobMaster asyncMaster = InstanceFactory.instance(JobMaster.class);
		QueueWorker asyncQueueWorker = asyncMaster.createAsyncQueueWorker("Test");
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
		}).job("pendingJob1", jobContext -> {
			$(Assertions::fail);
			return null;
		}, true).job("pendingJob2", jobContext -> {
			$(Assertions::fail);
			return null;
		}, true).externalJob("batchRead", previousJobContext -> {
			counter.incrementAndGet();
			System.out.println("job2 " + Thread.currentThread());
			$(() -> Assertions.assertEquals(1, ((TapInsertRecordEvent)((List)previousJobContext.getResult()).get(0)).getAfter().get("a")));
			completed();
			return null;
		});
		asyncQueueWorker.start(initialContext);

		waitCompleted(6);
	}

	@Test
	public void testPendingJob() throws Throwable {
		JobMaster asyncMaster = InstanceFactory.instance(JobMaster.class);
		QueueWorker asyncQueueWorker = asyncMaster.createAsyncQueueWorker("Test");
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
			return JobContext.create("Hello world").jumpToId("pendingJob1");
		}).job("pendingJob1", jobContext -> {
			counter.incrementAndGet();
			return null;
		}, true).job("pendingJob2", jobContext -> {
			$(Assertions::fail);
			return null;
		}, true).externalJob("batchRead", previousJobContext -> {
			counter.incrementAndGet();
			System.out.println("job2 " + Thread.currentThread());
			$(() -> Assertions.assertEquals(1, ((TapInsertRecordEvent)((List)previousJobContext.getResult()).get(0)).getAfter().get("a")));
			$(() -> Assertions.assertEquals(3, counter.get()));
			completed();
			return null;
		});
		asyncQueueWorker.start(initialContext);

		waitCompleted(6);
	}
	@Test
	public void testAnotherAsyncJob() throws Throwable {
		JobMaster asyncMaster = InstanceFactory.instance(JobMaster.class);
		QueueWorker asyncQueueWorker = asyncMaster.createAsyncQueueWorker("Test");
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
			return JobContext.create("Hello world").jumpToId("pendingJob1");
		}).asyncJob("pendingJob1", (jobContext, jobCompleted) -> {
			try {
				Thread.sleep(100L);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
			counter.incrementAndGet();
			jobCompleted.completed(null, null);
		}, true).job("pendingJob2", jobContext -> {
			$(Assertions::fail);
			return null;
		}, true).externalJob("batchRead", previousJobContext -> {
			counter.incrementAndGet();
			System.out.println("job2 " + Thread.currentThread());
			$(() -> Assertions.assertEquals(1, ((TapInsertRecordEvent)((List)previousJobContext.getResult()).get(0)).getAfter().get("a")));
			$(() -> Assertions.assertEquals(3, counter.get()));
			completed();
			return null;
		});
		asyncQueueWorker.start(initialContext);

		waitCompleted(6);
	}

	@Test
	public void testAnotherAsyncJobError() throws Throwable {
		JobMaster asyncMaster = InstanceFactory.instance(JobMaster.class);
		QueueWorker asyncQueueWorker = asyncMaster.createAsyncQueueWorker("Test");
		asyncQueueWorker.setQueueWorkerStateListener((id, fromState, toState) -> {
			System.out.println("id " + id + " from " + fromState + " to " + toState);
		});
		asyncQueueWorker.setAsyncJobErrorListener((id, asyncJob, throwable) -> {
			System.out.println("error " + throwable.getMessage());
			$(() -> Assertions.assertEquals("aaaa", throwable.getMessage()));
			completed();
		});
		AtomicInteger counter = new AtomicInteger(0);
		JobContext initialContext = JobContext.create("initial");
		asyncQueueWorker.job("job1", previousJobContext -> {
//			$(() -> Assertions.assertEquals("initial", previousJobContext.getResult()));
			counter.incrementAndGet();
			System.out.println("job1 " + Thread.currentThread());
			return JobContext.create("Hello world").jumpToId("pendingJob1");
		}).asyncJob("pendingJob1", (jobContext, jobCompleted) -> {
			try {
				Thread.sleep(100L);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
			counter.incrementAndGet();
			jobCompleted.completed(null, new CoreException("aaaa"));
		}, true).job("pendingJob2", jobContext -> {
			$(Assertions::fail);
			return null;
		}, true).externalJob("batchRead", previousJobContext -> {
			$(Assertions::fail);
			return null;
		});
		asyncQueueWorker.start(initialContext);

		waitCompleted(6);
	}

	@Test
	public void testLastJob() throws Throwable {
		JobMaster asyncMaster = InstanceFactory.instance(JobMaster.class);
		QueueWorker asyncQueueWorker = asyncMaster.createAsyncQueueWorker("Test");
		asyncQueueWorker.setQueueWorkerStateListener((id, fromState, toState) -> {
			System.out.println("id " + id + " from " + fromState + " to " + toState);
		});
		asyncQueueWorker.setAsyncJobErrorListener((id, asyncJob, throwable) -> {
			System.out.println("error " + throwable.getMessage());

		});
		AtomicInteger counter = new AtomicInteger(0);
		JobContext initialContext = JobContext.create("initial");
		asyncQueueWorker.job("job1", previousJobContext -> {
//			$(() -> Assertions.assertEquals("initial", previousJobContext.getResult()));
			counter.incrementAndGet();
			System.out.println("job1 " + Thread.currentThread());
			return JobContext.create("Hello world").jumpToId("pendingJob1");
		}).asyncJob("pendingJob1", (jobContext, jobCompleted) -> {
			try {
				Thread.sleep(100L);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
			counter.incrementAndGet();
			jobCompleted.completed(null, null);
		}, true).finished(jobContext -> {
			System.out.println("job1 " + Thread.currentThread());
			new Thread(() -> {
				try {
					Thread.sleep(100L);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
				Assertions.assertEquals(QueueWorkerStateListener.STATE_FINISHED, asyncQueueWorker.getState());
				completed();
			}).start();
			return null;
		}).job("pendingJob2", jobContext -> {
			$(Assertions::fail);
			return null;
		}, true).externalJob("batchRead", previousJobContext -> {
			$(Assertions::fail);
			return null;
		});
		asyncQueueWorker.start(initialContext);

		waitCompleted(6);
	}
}
