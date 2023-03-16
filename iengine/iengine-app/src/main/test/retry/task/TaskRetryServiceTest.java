package retry.task;

import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.flow.engine.V2.task.retry.task.TaskRetryFactory;
import io.tapdata.flow.engine.V2.task.retry.task.TaskRetryService;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

/**
 * @author samuel
 * @Description
 * @create 2023-03-13 10:40
 **/
public class TaskRetryServiceTest {
	private TaskRetryFactory taskRetryFactory;
	private TaskDto taskDto;

	@Before
	public void init() {
		taskRetryFactory = TaskRetryFactory.getInstance();
		ObjectId objectId = new ObjectId();
		taskDto = new TaskDto();
		taskDto.setId(objectId);
	}

	@Test
	public void testTaskRetryFactorySingleton() {
		AtomicReference<TaskRetryFactory> taskRetryFactory1 = new AtomicReference<>();
		AtomicReference<TaskRetryFactory> taskRetryFactory2 = new AtomicReference<>();
		IntStream.range(0, 10).forEach(i -> {
			CountDownLatch countDownLatch = new CountDownLatch(2);
			new Thread(() -> {
				try {
					taskRetryFactory1.set(TaskRetryFactory.getInstance());
				} finally {
					countDownLatch.countDown();
				}
			}).start();
			new Thread(() -> {
				try {
					taskRetryFactory2.set(TaskRetryFactory.getInstance());
				} finally {
					countDownLatch.countDown();
				}
			}).start();
			try {
				countDownLatch.await(10, TimeUnit.SECONDS);
			} catch (InterruptedException ignored) {
			}
			Assert.assertNotNull(taskRetryFactory1.get());
			Assert.assertNotNull(taskRetryFactory2.get());
			Assert.assertEquals(taskRetryFactory1.get(), taskRetryFactory2.get());
		});
	}

	@Test
	public void testGetAndRemoveTaskRetryService() {
		TaskRetryService taskRetryService = taskRetryFactory.getTaskRetryService(taskDto, TimeUnit.SECONDS.toMillis(3L));
		Assert.assertNotNull(taskRetryFactory.getTaskRetryService(taskDto.getId().toHexString()).orElse(null));
		Assert.assertEquals(taskRetryFactory.getTaskRetryService(taskDto.getId().toHexString()).orElse(null), taskRetryService);
		taskRetryFactory.removeTaskRetryService(taskDto.getId().toHexString());
		Assert.assertNull(taskRetryFactory.getTaskRetryService(taskDto.getId().toHexString()).orElse(null));
	}

	@Test
	public void testTaskRetryStartEndTime() {
		long duration = 10 * 1000L;
		TaskRetryService taskRetryService = taskRetryFactory.getTaskRetryService(taskDto, duration);
		taskRetryService.start();
		long startTs = System.currentTimeMillis();
		long endTs = startTs + duration;
		Assert.assertNotNull(taskRetryService.getStartRetryTimeMs());
		Assert.assertTrue(startTs >= taskRetryService.getStartRetryTimeMs());
		Assert.assertNotNull(taskRetryService.getEndRetryTimeMs());
		Assert.assertTrue(endTs >= taskRetryService.getEndRetryTimeMs());
	}

	@Test
	public void testMethodRetryDuration() {
		long duration = TimeUnit.SECONDS.toMillis(4L);
		long methodRetryIntervalMs = TimeUnit.SECONDS.toMillis(1L);
		long methodRetryTime = 3L;
		TaskRetryService taskRetryService = taskRetryFactory.getTaskRetryService(taskDto, duration, methodRetryTime);
		taskRetryService.start();
		long methodRetryDurationMs = taskRetryService.getMethodRetryDurationMs(methodRetryIntervalMs);
		Assert.assertTrue(methodRetryDurationMs >= methodRetryTime * methodRetryIntervalMs && methodRetryDurationMs <= duration * methodRetryIntervalMs);
		try {
			TimeUnit.SECONDS.sleep(TimeUnit.MILLISECONDS.toSeconds(duration - 1000L));
		} catch (InterruptedException ignored) {
		}
		methodRetryDurationMs = taskRetryService.getMethodRetryDurationMs(methodRetryIntervalMs);
		Assert.assertTrue(methodRetryDurationMs <= 1000L);
	}

	@Test
	public void testMethodZeroRetryDuration() {
		long duration = 0L;
		long methodRetryIntervalMs = TimeUnit.SECONDS.toMillis(1L);
		long methodRetryTime = 3L;
		TaskRetryService taskRetryService = taskRetryFactory.getTaskRetryService(taskDto, duration, methodRetryTime);
		taskRetryService.start();
		Assert.assertEquals(taskRetryService.getMethodRetryDurationMs(methodRetryIntervalMs), 0L);
	}
}
