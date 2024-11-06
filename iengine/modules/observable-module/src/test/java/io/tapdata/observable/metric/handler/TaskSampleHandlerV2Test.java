package io.tapdata.observable.metric.handler;

import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.observable.metric.entity.TaskInputOutputRecordCounter;
import io.tapdata.pdk.apis.entity.WriteListResult;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.*;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

/**
 * @author samuel
 * @Description
 * @create 2024-09-12 17:34
 **/
@DisplayName("Class TaskSampleHandlerV2 Test")
class TaskSampleHandlerV2Test {

	private TaskSampleHandlerV2 taskSampleHandlerV2;
	private TaskDto taskDto;

	@BeforeEach
	void setUp() {
		taskDto = new TaskDto();
		taskDto.setId(new ObjectId());
		taskDto.setName("task 1");
		taskDto.setTaskRecordId(new ObjectId().toString());
		taskDto.setStartTime(new Date());
		taskSampleHandlerV2 = new TaskSampleHandlerV2(taskDto);
		taskSampleHandlerV2 = spy(taskSampleHandlerV2);
		doReturn(new HashMap<>()).when(taskSampleHandlerV2).retrieve();
		taskSampleHandlerV2.init();
	}

	@Nested
	@DisplayName("Method doInit test")
	class doInitTest {
		@Test
		@DisplayName("test main process")
		void test1() {
			Object isConsumeInputOutputThreadRunning = ReflectionTestUtils.getField(taskSampleHandlerV2, "isConsumeInputOutputThreadRunning");
			assertInstanceOf(AtomicBoolean.class, isConsumeInputOutputThreadRunning);
			assertTrue(((AtomicBoolean) isConsumeInputOutputThreadRunning).get());
			Object consumeInputOutputThread = ReflectionTestUtils.getField(taskSampleHandlerV2, "consumeInputOutputThread");
			assertNotNull(consumeInputOutputThread);
			assertInstanceOf(Thread.class, consumeInputOutputThread);
			assertTrue(((Thread) consumeInputOutputThread).isAlive());
		}

		@Test
		@Disabled
		@DisplayName("test already running")
		void test2() {
			Thread consumeInputOutputThread = (Thread) ReflectionTestUtils.getField(taskSampleHandlerV2, "consumeInputOutputThread");
			taskSampleHandlerV2.init();
			assertSame(consumeInputOutputThread, ReflectionTestUtils.getField(taskSampleHandlerV2, "consumeInputOutputThread"));
		}
	}

	@Nested
	@DisplayName("Method close test")
	class closeTest {
		@Test
		@DisplayName("test main process")
		void test1() {
			taskSampleHandlerV2.close();

			Object isConsumeInputOutputThreadRunning = ReflectionTestUtils.getField(taskSampleHandlerV2, "isConsumeInputOutputThreadRunning");
			assertInstanceOf(AtomicBoolean.class, isConsumeInputOutputThreadRunning);
			assertFalse(((AtomicBoolean) isConsumeInputOutputThreadRunning).get());
			Object consumeInputOutputThread = ReflectionTestUtils.getField(taskSampleHandlerV2, "consumeInputOutputThread");
			assertInstanceOf(Thread.class, consumeInputOutputThread);
			assertDoesNotThrow(() -> Thread.sleep(100L));
			assertFalse(((Thread) consumeInputOutputThread).isAlive());
		}
	}

	@Nested
	@DisplayName("Method handleBatchReadAccept test")
	class handleBatchReadAcceptTest {
		@BeforeEach
		void setUp() {
			((Thread) ReflectionTestUtils.getField(taskSampleHandlerV2, "consumeInputOutputThread")).interrupt();
		}

		@Test
		@DisplayName("test main process")
		void test1() {
			HandlerUtil.EventTypeRecorder record = new HandlerUtil.EventTypeRecorder();
			record.setInsertTotal(1000L);
			record.setMemorySize(1024L);
			taskSampleHandlerV2.handleBatchReadAccept(record);

			LinkedBlockingQueue<TaskInputOutputRecordCounter> sourceTaskInputOutputCounterQueue = (LinkedBlockingQueue<TaskInputOutputRecordCounter>) ReflectionTestUtils.getField(taskSampleHandlerV2, "sourceTaskInputOutputCounterQueue");
			assertEquals(1, sourceTaskInputOutputCounterQueue.size());
			TaskInputOutputRecordCounter taskInputOutputRecordCounter = sourceTaskInputOutputCounterQueue.poll();
			assertNotNull(taskInputOutputRecordCounter);
			assertEquals(1000L, taskInputOutputRecordCounter.getInsertCounter().value().longValue());
		}

		@Test
		@DisplayName("test null record")
		void test2() {
			taskSampleHandlerV2.handleBatchReadAccept(null);
			LinkedBlockingQueue<TaskInputOutputRecordCounter> sourceTaskInputOutputCounterQueue = (LinkedBlockingQueue<TaskInputOutputRecordCounter>) ReflectionTestUtils.getField(taskSampleHandlerV2, "sourceTaskInputOutputCounterQueue");
			assertEquals(0, sourceTaskInputOutputCounterQueue.size());
		}
	}

	@Nested
	@DisplayName("Method handleStreamReadAccept test")
	class handleStreamReadAcceptTest {
		@BeforeEach
		void setUp() {
			((Thread) ReflectionTestUtils.getField(taskSampleHandlerV2, "consumeInputOutputThread")).interrupt();
		}

		@Test
		@DisplayName("test main process")
		@Disabled
		void test1() {
			HandlerUtil.EventTypeRecorder record = new HandlerUtil.EventTypeRecorder();
			record.setInsertTotal(1000L);
			record.setUpdateTotal(100L);
			record.setDeleteTotal(10L);
			record.setMemorySize(1024L);
			taskSampleHandlerV2.handleStreamReadAccept(record);

			LinkedBlockingQueue<TaskInputOutputRecordCounter> sourceTaskInputOutputCounterQueue = (LinkedBlockingQueue<TaskInputOutputRecordCounter>) ReflectionTestUtils.getField(taskSampleHandlerV2, "sourceTaskInputOutputCounterQueue");
			assertEquals(1, sourceTaskInputOutputCounterQueue.size());
			TaskInputOutputRecordCounter taskInputOutputRecordCounter = sourceTaskInputOutputCounterQueue.poll();
			assertNotNull(taskInputOutputRecordCounter);
			assertEquals(1000L, taskInputOutputRecordCounter.getInsertCounter().value().longValue());
			assertEquals(100L, taskInputOutputRecordCounter.getUpdateCounter().value().longValue());
			assertEquals(10L, taskInputOutputRecordCounter.getDeleteCounter().value().longValue());
		}

		@Test
		@DisplayName("test null record")
		void test2() {
			taskSampleHandlerV2.handleStreamReadAccept(null);
			LinkedBlockingQueue<TaskInputOutputRecordCounter> sourceTaskInputOutputCounterQueue = (LinkedBlockingQueue<TaskInputOutputRecordCounter>) ReflectionTestUtils.getField(taskSampleHandlerV2, "sourceTaskInputOutputCounterQueue");
			assertEquals(0, sourceTaskInputOutputCounterQueue.size());
		}
	}

	@Nested
	@DisplayName("Method handleWriteRecordAccept test")
	class handleWriteRecordAcceptTest {
		@Test
		@DisplayName("test main process")
		void test1() {
			WriteListResult<TapRecordEvent> writeListResult = new WriteListResult<>();
			writeListResult.setInsertedCount(1000L);
			writeListResult.setModifiedCount(100L);
			writeListResult.setRemovedCount(10L);
			HandlerUtil.EventTypeRecorder eventTypeRecorder = new HandlerUtil.EventTypeRecorder();
			eventTypeRecorder.setMemorySize(1024L);

			taskSampleHandlerV2.handleWriteRecordAccept(writeListResult, new ArrayList<TapRecordEvent>() {{
				TapInsertRecordEvent tapInsertRecordEvent = new TapInsertRecordEvent();
				tapInsertRecordEvent.setTime(System.currentTimeMillis());
				add(tapInsertRecordEvent);
			}}, eventTypeRecorder);
			taskSampleHandlerV2.handleWriteRecordAccept(writeListResult, new ArrayList<>(), eventTypeRecorder);

			TaskInputOutputRecordCounter tempInputOutputCounter = (TaskInputOutputRecordCounter) ReflectionTestUtils.getField(taskSampleHandlerV2, "tempInputOutputCounter");
			assertEquals(2000L, tempInputOutputCounter.getInsertCounter().value().longValue());
			assertEquals(200L, tempInputOutputCounter.getUpdateCounter().value().longValue());
			assertEquals(20L, tempInputOutputCounter.getDeleteCounter().value().longValue());
		}

		@Test
		@DisplayName("test null record")
		void test2() {
			taskSampleHandlerV2.handleWriteRecordAccept(null, new ArrayList<>(), null);
			TaskInputOutputRecordCounter tempInputOutputCounter = (TaskInputOutputRecordCounter) ReflectionTestUtils.getField(taskSampleHandlerV2, "tempInputOutputCounter");
			assertEquals(0L, tempInputOutputCounter.getInsertCounter().value().longValue());
			assertEquals(0L, tempInputOutputCounter.getUpdateCounter().value().longValue());
			assertEquals(0L, tempInputOutputCounter.getDeleteCounter().value().longValue());
		}
	}

	@Nested
	@DisplayName("Method handleWriteBatchSplit test")
	class handleWriteBatchSplitTest {
		@BeforeEach
		void setUp() {
			((Thread) ReflectionTestUtils.getField(taskSampleHandlerV2, "consumeInputOutputThread")).interrupt();
		}

		@Test
		@DisplayName("test main process")
		void test1() {
			taskSampleHandlerV2.handleWriteBatchSplit();

			LinkedBlockingQueue<TaskInputOutputRecordCounter> targetTaskInputOutputCounterQueue = (LinkedBlockingQueue<TaskInputOutputRecordCounter>) ReflectionTestUtils.getField(taskSampleHandlerV2, "targetTaskInputOutputCounterQueue");
			assertEquals(1, targetTaskInputOutputCounterQueue.size());
			TaskInputOutputRecordCounter taskInputOutputRecordCounter = targetTaskInputOutputCounterQueue.poll();
			assertNotNull(taskInputOutputRecordCounter);
			assertEquals(0L, taskInputOutputRecordCounter.getInsertCounter().value().longValue());
			assertEquals(0L, taskInputOutputRecordCounter.getUpdateCounter().value().longValue());
			assertEquals(0L, taskInputOutputRecordCounter.getDeleteCounter().value().longValue());
		}
	}

	@Nested
	@DisplayName("Method consumeQueue test")
	class consumeQueueTest {

		private LinkedBlockingQueue<TaskInputOutputRecordCounter> targetTaskInputOutputCounterQueue;
		private LinkedBlockingQueue<TaskInputOutputRecordCounter> sourceTaskInputOutputCounterQueue;

		@BeforeEach
		void setUp() {
			((Thread) ReflectionTestUtils.getField(taskSampleHandlerV2, "consumeInputOutputThread")).interrupt();
			Thread consumeInputOutputThread = (Thread) ReflectionTestUtils.getField(taskSampleHandlerV2, "consumeInputOutputThread");
			while (true) {
				if (!consumeInputOutputThread.isAlive()) {
					break;
				}
			}
			sourceTaskInputOutputCounterQueue = (LinkedBlockingQueue<TaskInputOutputRecordCounter>) ReflectionTestUtils.getField(taskSampleHandlerV2, "sourceTaskInputOutputCounterQueue");
			targetTaskInputOutputCounterQueue = (LinkedBlockingQueue<TaskInputOutputRecordCounter>) ReflectionTestUtils.getField(taskSampleHandlerV2, "targetTaskInputOutputCounterQueue");
		}

		@Test
		@DisplayName("test main process")
		void test1() {
			TaskInputOutputRecordCounter sourceCounter = new TaskInputOutputRecordCounter();
			sourceCounter.getInsertCounter().inc(3L);
			sourceCounter.getUpdateCounter().inc(2L);
			sourceCounter.getDeleteCounter().inc(1L);
			assertDoesNotThrow(() -> sourceTaskInputOutputCounterQueue.put(sourceCounter));
			TaskInputOutputRecordCounter targetCounter = new TaskInputOutputRecordCounter();
			targetCounter.getInsertCounter().inc(1L);
			targetCounter.getUpdateCounter().inc(2L);
			targetCounter.getDeleteCounter().inc(3L);
			assertDoesNotThrow(() -> targetTaskInputOutputCounterQueue.put(targetCounter));

			assertDoesNotThrow(() -> taskSampleHandlerV2.consumeQueue(1000L));

			assertEquals(0, sourceTaskInputOutputCounterQueue.size());
			assertEquals(0, targetTaskInputOutputCounterQueue.size());
			assertEquals(3L, taskSampleHandlerV2.getCollector().getCounterSampler(Constants.INPUT_INSERT_TOTAL).value().longValue());
			assertEquals(2L, taskSampleHandlerV2.getCollector().getCounterSampler(Constants.INPUT_UPDATE_TOTAL).value().longValue());
			assertEquals(1L, taskSampleHandlerV2.getCollector().getCounterSampler(Constants.INPUT_DELETE_TOTAL).value().longValue());
			assertEquals(1L, taskSampleHandlerV2.getCollector().getCounterSampler(Constants.OUTPUT_INSERT_TOTAL).value().longValue());
			assertEquals(2L, taskSampleHandlerV2.getCollector().getCounterSampler(Constants.OUTPUT_UPDATE_TOTAL).value().longValue());
			assertEquals(3L, taskSampleHandlerV2.getCollector().getCounterSampler(Constants.OUTPUT_DELETE_TOTAL).value().longValue());
		}

		@Test
		@DisplayName("test no target counter")
		void test2() {
			TaskInputOutputRecordCounter sourceCounter = new TaskInputOutputRecordCounter();
			sourceCounter.getInsertCounter().inc(3L);
			sourceCounter.getUpdateCounter().inc(2L);
			sourceCounter.getDeleteCounter().inc(1L);
			assertDoesNotThrow(() -> sourceTaskInputOutputCounterQueue.put(sourceCounter));

			assertDoesNotThrow(() -> taskSampleHandlerV2.consumeQueue(0L));

			assertEquals(0, sourceTaskInputOutputCounterQueue.size());
			assertEquals(0, targetTaskInputOutputCounterQueue.size());
			assertEquals(3L, taskSampleHandlerV2.getCollector().getCounterSampler(Constants.INPUT_INSERT_TOTAL).value().longValue());
			assertEquals(2L, taskSampleHandlerV2.getCollector().getCounterSampler(Constants.INPUT_UPDATE_TOTAL).value().longValue());
			assertEquals(1L, taskSampleHandlerV2.getCollector().getCounterSampler(Constants.INPUT_DELETE_TOTAL).value().longValue());
			assertEquals(0L, taskSampleHandlerV2.getCollector().getCounterSampler(Constants.OUTPUT_INSERT_TOTAL).value().longValue());
			assertEquals(0L, taskSampleHandlerV2.getCollector().getCounterSampler(Constants.OUTPUT_UPDATE_TOTAL).value().longValue());
			assertEquals(0L, taskSampleHandlerV2.getCollector().getCounterSampler(Constants.OUTPUT_DELETE_TOTAL).value().longValue());
			assertEquals(1, ReflectionTestUtils.getField(taskSampleHandlerV2, "missingTarget"));
		}

		@Test
		@DisplayName("test missing target > 0")
		void test3() {
			ReflectionTestUtils.setField(taskSampleHandlerV2, "missingTarget", 1);
			TaskInputOutputRecordCounter sourceCounter = new TaskInputOutputRecordCounter();
			sourceCounter.getInsertCounter().inc(3L);
			sourceCounter.getUpdateCounter().inc(2L);
			sourceCounter.getDeleteCounter().inc(1L);
			assertDoesNotThrow(() -> sourceTaskInputOutputCounterQueue.put(sourceCounter));
			TaskInputOutputRecordCounter targetCounter = new TaskInputOutputRecordCounter();
			targetCounter.getInsertCounter().inc(1L);
			targetCounter.getUpdateCounter().inc(2L);
			targetCounter.getDeleteCounter().inc(3L);
			assertDoesNotThrow(() -> targetTaskInputOutputCounterQueue.put(targetCounter));
			assertDoesNotThrow(() -> targetTaskInputOutputCounterQueue.put(targetCounter));

			assertDoesNotThrow(() -> taskSampleHandlerV2.consumeQueue(1000L));

			assertEquals(0, sourceTaskInputOutputCounterQueue.size());
			assertEquals(0, targetTaskInputOutputCounterQueue.size());
			assertEquals(3L, taskSampleHandlerV2.getCollector().getCounterSampler(Constants.INPUT_INSERT_TOTAL).value().longValue());
			assertEquals(2L, taskSampleHandlerV2.getCollector().getCounterSampler(Constants.INPUT_UPDATE_TOTAL).value().longValue());
			assertEquals(1L, taskSampleHandlerV2.getCollector().getCounterSampler(Constants.INPUT_DELETE_TOTAL).value().longValue());
			assertEquals(2L, taskSampleHandlerV2.getCollector().getCounterSampler(Constants.OUTPUT_INSERT_TOTAL).value().longValue());
			assertEquals(4L, taskSampleHandlerV2.getCollector().getCounterSampler(Constants.OUTPUT_UPDATE_TOTAL).value().longValue());
			assertEquals(6L, taskSampleHandlerV2.getCollector().getCounterSampler(Constants.OUTPUT_DELETE_TOTAL).value().longValue());
		}
	}
}