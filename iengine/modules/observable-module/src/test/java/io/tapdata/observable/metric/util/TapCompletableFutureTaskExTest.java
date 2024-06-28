package io.tapdata.observable.metric.util;

import com.tapdata.tm.commons.task.dto.TaskDto;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author samuel
 * @Description
 * @create 2024-06-28 14:12
 **/
class TapCompletableFutureTaskExTest {

	private TaskDto taskDto;

	@BeforeEach
	void setUp() {
		taskDto = new TaskDto();
		taskDto.setId(new ObjectId());
		taskDto.setName("test");
	}

	@Test
	@DisplayName("Method create test")
	void testCreate() {
		TapCompletableFutureTaskEx tapCompletableFutureTaskEx = TapCompletableFutureTaskEx.create(1000, 1000, taskDto);
		assertSame(taskDto, tapCompletableFutureTaskEx.getTaskDto());

		assertDoesNotThrow(() -> TapCompletableFutureTaskEx.create(1000, 1000, null));
		taskDto.setId(null);
		assertDoesNotThrow(() -> TapCompletableFutureTaskEx.create(1000, 1000, taskDto));
		taskDto.setName(null);
		assertDoesNotThrow(() -> TapCompletableFutureTaskEx.create(1000, 100, taskDto));
	}

	@Test
	@DisplayName("Test runAsync and stop with single thread")
	void testRunAsyncAndStopSingleThread() {
		TapCompletableFutureEx tapCompletableFutureTaskEx = TapCompletableFutureTaskEx.create(1000, 1000, taskDto).start();
		AtomicInteger counter = new AtomicInteger();
		tapCompletableFutureTaskEx.runAsync(() -> {
			try {
				TimeUnit.SECONDS.sleep(1L);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			counter.incrementAndGet();
		});
		tapCompletableFutureTaskEx.stop(1L, TimeUnit.SECONDS);
		assertEquals(1, counter.get());
	}
}