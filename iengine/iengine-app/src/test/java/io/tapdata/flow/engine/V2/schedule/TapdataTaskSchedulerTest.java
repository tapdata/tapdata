package io.tapdata.flow.engine.V2.schedule;

import com.hazelcast.jet.core.JobStatus;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.entity.ResponseBody;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.sdk.available.TmStatusService;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.exception.RestDoNotRetryException;
import io.tapdata.exception.TmUnavailableException;
import io.tapdata.flow.engine.V2.task.OpType;
import io.tapdata.flow.engine.V2.task.TaskClient;
import io.tapdata.flow.engine.V2.task.impl.HazelcastTaskClient;
import io.tapdata.flow.engine.V2.task.operation.StartTaskOperation;
import io.tapdata.flow.engine.V2.task.operation.StopTaskOperation;
import io.tapdata.flow.engine.V2.task.operation.TaskOperation;
import io.tapdata.flow.engine.V2.task.retry.task.TaskRetryFactory;
import io.tapdata.flow.engine.V2.task.retry.task.TaskRetryService;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.observable.logging.ObsLoggerFactory;
import io.tapdata.utils.AppType;
import io.tapdata.utils.UnitTestUtils;
import org.apache.logging.log4j.Logger;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.util.ReflectionTestUtils;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class TapdataTaskSchedulerTest {
	private TapdataTaskScheduler tapdataTaskScheduler;
	private Map<String, Long> taskRetryTimeMap;
	private Map<String, TaskClient<TaskDto>> taskClientMap;

	@BeforeEach
	void buildTapdataTaskScheduler() throws NoSuchFieldException, IllegalAccessException {
		tapdataTaskScheduler = mock(TapdataTaskScheduler.class);
		Class<TapdataTaskScheduler> clazz = TapdataTaskScheduler.class;
		Field taskRetryTimeMapField = clazz.getDeclaredField("taskRetryTimeMap");
		taskRetryTimeMapField.setAccessible(true);
		taskRetryTimeMap = (Map<String, Long>) (taskRetryTimeMapField.get(clazz));
		taskRetryTimeMap.put("111", 1L);
		taskRetryTimeMap.put("222", 2L);
		taskClientMap = mock(Map.class);
		ReflectionTestUtils.setField(tapdataTaskScheduler, "taskClientMap", taskClientMap);
	}

	@Nested
	class ResetTaskRetryServiceIfNeedTest {
		@DisplayName("test reset task retry service if need normal")
		@Test
		void test1() {
			TaskRetryFactory factory = mock(TaskRetryFactory.class);
			try (MockedStatic<TaskRetryFactory> mb = Mockito
					.mockStatic(TaskRetryFactory.class)) {
				mb.when(TaskRetryFactory::getInstance).thenReturn(factory);
				ObsLoggerFactory obsLoggerFactory = mock(ObsLoggerFactory.class);
				try (MockedStatic<ObsLoggerFactory> of = Mockito
						.mockStatic(ObsLoggerFactory.class)) {
					mb.when(ObsLoggerFactory::getInstance).thenReturn(obsLoggerFactory);
					when(obsLoggerFactory.getObsLogger(anyString())).thenReturn(mock(ObsLogger.class));
					TaskRetryService taskRetryService = mock(TaskRetryService.class);
					when(factory.getTaskRetryService(anyString())).thenReturn(Optional.ofNullable(taskRetryService));
					TaskClient taskDtoTaskClient = mock(TaskClient.class);
					when(taskClientMap.get(anyString())).thenReturn(taskDtoTaskClient);
					TaskDto taskDto = mock(TaskDto.class);
					when(taskDtoTaskClient.getTask()).thenReturn(taskDto);
					when(taskDto.getName()).thenReturn("task 1");
					doCallRealMethod().when(tapdataTaskScheduler).resetTaskRetryServiceIfNeed();
					tapdataTaskScheduler.resetTaskRetryServiceIfNeed();
					assertEquals(0, taskRetryTimeMap.size());
				}
			}
		}

		@DisplayName("test reset task retry service if need with null task retry service")
		@Test
		void test2() {
			doCallRealMethod().when(tapdataTaskScheduler).resetTaskRetryServiceIfNeed();
			tapdataTaskScheduler.resetTaskRetryServiceIfNeed();
			assertEquals(2, taskRetryTimeMap.size());
		}

		@Nested
		@DisplayName("memory method test")
		class memoryTest {
			@Test
			@DisplayName("main process test")
			void testMemory() {
				TapdataTaskScheduler taskScheduler = mock(TapdataTaskScheduler.class);
				HazelcastTaskClient hazelcastTaskClient = mock(HazelcastTaskClient.class);
				when(hazelcastTaskClient.getJetStatus()).thenReturn(JobStatus.RUNNING);
				TaskDto taskDto = mock(TaskDto.class);
				when(taskDto.getName()).thenReturn("task 1");
				when(taskDto.getStatus()).thenReturn(TaskDto.STATUS_RUNNING);
				when(hazelcastTaskClient.getTask()).thenReturn(taskDto);
				ReflectionTestUtils.setField(taskScheduler, "taskClientMap", new ConcurrentHashMap<String, TaskClient<TaskDto>>() {{
					put("1", hazelcastTaskClient);
				}});

				when(taskScheduler.memory(null, null)).thenCallRealMethod();
				DataMap actual = taskScheduler.memory(null, null);
				assertNotNull(actual);
				assertEquals(1, actual.size());
				assertTrue(actual.containsKey("task client map"));
				Object taskClientMapObj = actual.get("task client map");
				assertInstanceOf(DataMap.class, taskClientMapObj);
				DataMap taskClientMap = (DataMap) taskClientMapObj;
				assertEquals(1, taskClientMap.size());
				assertTrue(taskClientMap.containsKey("task 1"));
				Object taskMapObj = taskClientMap.get("task 1");
				assertInstanceOf(DataMap.class, taskMapObj);
				DataMap taskMap = (DataMap) taskMapObj;
				assertEquals(2, taskMap.size());
				assertEquals(TaskDto.STATUS_RUNNING, taskMap.getString("task status"));
				assertEquals(JobStatus.RUNNING.toString(), taskMap.getString("jet status"));
			}

			@Test
			@DisplayName("when task client is not hazelcast task client")
			void notHazelcastTaskClient() {
				TapdataTaskScheduler taskScheduler = mock(TapdataTaskScheduler.class);
				TaskClient taskClient = mock(TaskClient.class);
				TaskDto taskDto = mock(TaskDto.class);
				when(taskDto.getName()).thenReturn("task 1");
				when(taskDto.getStatus()).thenReturn(TaskDto.STATUS_RUNNING);
				when(taskClient.getTask()).thenReturn(taskDto);
				ReflectionTestUtils.setField(taskScheduler, "taskClientMap", new ConcurrentHashMap<String, TaskClient<TaskDto>>() {{
					put("1", taskClient);
				}});

				when(taskScheduler.memory(null, null)).thenCallRealMethod();
				DataMap actual = taskScheduler.memory(null, null);
				assertNotNull(actual);
				assertEquals(1, actual.size());
				assertTrue(actual.containsKey("task client map"));
				Object taskClientMapObj = actual.get("task client map");
				assertInstanceOf(DataMap.class, taskClientMapObj);
				DataMap taskClientMap = (DataMap) taskClientMapObj;
				assertEquals(1, taskClientMap.size());
				assertTrue(taskClientMap.containsKey("task 1"));
				Object taskMapObj = taskClientMap.get("task 1");
				assertInstanceOf(DataMap.class, taskMapObj);
				DataMap taskMap = (DataMap) taskMapObj;
				assertEquals(1, taskMap.size());
				assertEquals(TaskDto.STATUS_RUNNING, taskMap.getString("task status"));
			}
		}
	}

	@Nested
	class startTaskTest {

		private TapdataTaskScheduler taskScheduler;

		@BeforeEach
		void setUp() {
			taskScheduler = mock(TapdataTaskScheduler.class);
		}

		@Test
		@DisplayName("start task when task exists")
		void testStartTaskWhenTaskExists() {
			ObsLoggerFactory obsLoggerFactory = mock(ObsLoggerFactory.class);
			try (
					MockedStatic obsLoggerFactoryMockedStatic = mockStatic(ObsLoggerFactory.class)
			) {
				obsLoggerFactoryMockedStatic.when(ObsLoggerFactory::getInstance).thenReturn(obsLoggerFactory);
				ObjectId taskId = new ObjectId();
				TaskDto taskDto = new TaskDto();
				taskDto.setId(taskId);
				Map<String, TaskClient<TaskDto>> taskClientMap = new ConcurrentHashMap<>();
				TaskClient taskClient = mock(TaskClient.class);
				taskClientMap.put(taskId.toString(), taskClient);
				ReflectionTestUtils.setField(taskScheduler, "taskClientMap", taskClientMap);
				Logger logger = mock(Logger.class);
				ReflectionTestUtils.setField(taskScheduler, "logger", logger);
				ClientMongoOperator clientMongoOperator = mock(ClientMongoOperator.class);
				ReflectionTestUtils.setField(taskScheduler, "clientMongoOperator", clientMongoOperator);
				doCallRealMethod().when(taskScheduler).startTask(taskDto);

				when(taskClient.getStatus()).thenReturn(TaskDto.STATUS_RUNNING);
				assertDoesNotThrow(() -> taskScheduler.startTask(taskDto));

				when(taskClient.getStatus()).thenReturn(TaskDto.STATUS_STOPPING);
				assertDoesNotThrow(() -> taskScheduler.startTask(taskDto));
				verify(clientMongoOperator, times(2)).updateById(any(Update.class), eq(ConnectorConstant.TASK_COLLECTION + "/running"), eq(taskId.toString()), eq(TaskDto.class));
			}
		}

		@Test
		@DisplayName("start task when task exists, throw error when call tm rest api")
		void testStartTaskWhenTaskExistsAndThrowError() {
			ObsLoggerFactory obsLoggerFactory = mock(ObsLoggerFactory.class);
			try (
					MockedStatic obsLoggerFactoryMockedStatic = mockStatic(ObsLoggerFactory.class)
			) {
				obsLoggerFactoryMockedStatic.when(ObsLoggerFactory::getInstance).thenReturn(obsLoggerFactory);
				ObjectId taskId = new ObjectId();
				TaskDto taskDto = new TaskDto();
				taskDto.setId(taskId);
				Map<String, TaskClient<TaskDto>> taskClientMap = new ConcurrentHashMap<>();
				TaskClient taskClient = mock(TaskClient.class);
				taskClientMap.put(taskId.toString(), taskClient);
				ReflectionTestUtils.setField(taskScheduler, "taskClientMap", taskClientMap);
				Logger logger = mock(Logger.class);
				ReflectionTestUtils.setField(taskScheduler, "logger", logger);
				ClientMongoOperator clientMongoOperator = mock(ClientMongoOperator.class);
				RestDoNotRetryException restDoNotRetryException = mock(RestDoNotRetryException.class);
				when(restDoNotRetryException.getCode()).thenReturn("Transition.Not.Supported");
				when(clientMongoOperator.updateById(any(Update.class), eq(ConnectorConstant.TASK_COLLECTION + "/running"), eq(taskId.toString()), eq(TaskDto.class)))
						.thenThrow(restDoNotRetryException);
				ReflectionTestUtils.setField(taskScheduler, "clientMongoOperator", clientMongoOperator);
				doCallRealMethod().when(taskScheduler).startTask(taskDto);

				when(taskClient.getStatus()).thenReturn(TaskDto.STATUS_RUNNING);
				assertDoesNotThrow(() -> taskScheduler.startTask(taskDto));
				verify(clientMongoOperator, times(1)).updateById(any(Update.class), eq(ConnectorConstant.TASK_COLLECTION + "/running"), eq(taskId.toString()), eq(TaskDto.class));

				RuntimeException runtimeException = new RuntimeException("test");
				when(clientMongoOperator.updateById(any(Update.class), eq(ConnectorConstant.TASK_COLLECTION + "/running"), eq(taskId.toString()), eq(TaskDto.class)))
						.thenThrow(runtimeException);
				RuntimeException actual = assertThrows(RuntimeException.class, () -> taskScheduler.startTask(taskDto));
				assertEquals(actual, runtimeException);
			}
		}

		@Test
		@DisplayName("start task when task not exists, throw error when call tm rest api")
		void testStartTaskWhenTaskNotExistsAndThrowError() {
			ObsLoggerFactory obsLoggerFactory = mock(ObsLoggerFactory.class);
			try (
					MockedStatic obsLoggerFactoryMockedStatic = mockStatic(ObsLoggerFactory.class)
			) {
				obsLoggerFactoryMockedStatic.when(ObsLoggerFactory::getInstance).thenReturn(obsLoggerFactory);
				ObjectId taskId = new ObjectId();
				TaskDto taskDto = new TaskDto();
				taskDto.setName("test-task");
				taskDto.setId(taskId);

				Logger logger = mock(Logger.class);
				TapdataTaskScheduler instance = new TapdataTaskScheduler();

				RuntimeException enableAndAvailableError = new RuntimeException("enable available exception");
				RuntimeException enableUnavailableError = new TmUnavailableException("unavailable  exception", "post", null, new ResponseBody());
				RuntimeException disableAndAvailableError = new RuntimeException("disable and available exception");
				RuntimeException disableAndUnavailableError = new TmUnavailableException("disable and unavailable  exception", "post", null, new ResponseBody());
				ClientMongoOperator mongoOperator = mock(ClientMongoOperator.class);
				when(mongoOperator.updateById(any(), eq(ConnectorConstant.TASK_COLLECTION + "/running"), any(), any())).thenThrow(
					enableAndAvailableError, enableUnavailableError, disableAndAvailableError, disableAndUnavailableError
				);

				UnitTestUtils.injectField(TapdataTaskScheduler.class, instance, "clientMongoOperator", mongoOperator);
				UnitTestUtils.injectField(TapdataTaskScheduler.class, instance, "logger", logger); // ignore exception log

				// enable cloud logic
				try (MockedStatic<TmStatusService> tmStatusServiceMockedStatic = mockStatic(TmStatusService.class)) {
					tmStatusServiceMockedStatic.when(TmStatusService::isNotEnable).thenReturn(false);

					instance.startTask(taskDto);
					verify(logger, times(1)).error(anyString(), eq(taskDto.getName()), eq(enableAndAvailableError.getMessage()), eq(enableAndAvailableError));
					instance.startTask(taskDto);
					verify(logger, times(1)).warn(anyString(), eq(taskDto.getName()), eq(enableUnavailableError.getMessage()));
				}
				// disable cloud logic
				try (MockedStatic<TmStatusService> tmStatusServiceMockedStatic = mockStatic(TmStatusService.class)) {
					tmStatusServiceMockedStatic.when(TmStatusService::isNotEnable).thenReturn(true);

					instance.startTask(taskDto);
					verify(logger, times(1)).error(anyString(), eq(taskDto.getName()), eq(disableAndAvailableError.getMessage()), eq(disableAndAvailableError));
					instance.startTask(taskDto);
					verify(logger, times(1)).error(anyString(), eq(taskDto.getName()), eq(disableAndUnavailableError.getMessage()), eq(disableAndUnavailableError));
				}

			}
		}
	}

	@Nested
	class ForceStoppingTaskTest {
		@Test
		void testEmptyClient() {
			TapdataTaskScheduler instance = new TapdataTaskScheduler() {
				@Override
				protected List<TaskDto> findStoppingTasks() {
					return new ArrayList<>();
				}
			};

			instance.forceStoppingTask();
		}

		@Test
		void testEmptyClientIsCloud() {
			TapdataTaskScheduler instance = new TapdataTaskScheduler() {
				@Override
				protected List<TaskDto> findStoppingTasks() {
					return new ArrayList<>();
				}
			};
			try (MockedStatic<AppType> appTypeMockedStatic = mockStatic(AppType.class)) {
				AppType appType = mock(AppType.class);
				when(appType.isCloud()).thenReturn(true);
				appTypeMockedStatic.when(AppType::currentType).thenReturn(appType);

				instance.forceStoppingTask();
			}
		}
	}

	@Nested
	class GetHandleTaskOperationRunnableTest {
		@Test
		void testNone() {
			AtomicBoolean isCallRunnable = new AtomicBoolean(false);
			TaskOperation taskOperation = mock(TaskOperation.class);
			TapdataTaskScheduler instance = new TapdataTaskScheduler();
			TapdataTaskScheduler spyInstance = spy(instance);
			when(spyInstance.getHandleTaskOperationRunnable(taskOperation)).thenReturn(()-> isCallRunnable.set(true));

			Runnable handleTaskOperationRunnable = spyInstance.getHandleTaskOperationRunnable(taskOperation);
			assertNotNull(handleTaskOperationRunnable);
			handleTaskOperationRunnable.run();
			assertTrue(isCallRunnable.get());
		}
		@Test
		void testStartTaskRunException() {
			TaskDto taskDto = new TaskDto();
			taskDto.setId(ObjectId.get());
			taskDto.setName("test-task");

			AtomicReference<RuntimeException> errorAtomic = new AtomicReference<>();
			TaskOperation taskOperation = StartTaskOperation.create().taskDto(taskDto);
			TapdataTaskScheduler instance = new TapdataTaskScheduler() {
				@Override
				protected void startTask(TaskDto taskDto) {
					throw errorAtomic.get();
				}
			};

			Logger logger = mock(Logger.class);
			UnitTestUtils.injectField(TapdataTaskScheduler.class, instance, "logger", logger);

			Runnable handleTaskOperationRunnable = instance.getHandleTaskOperationRunnable(taskOperation);
			assertNotNull(handleTaskOperationRunnable);

			// enable TM status
			try (MockedStatic<TmStatusService> tmStatusServiceMockedStatic = mockStatic(TmStatusService.class)) {
				tmStatusServiceMockedStatic.when(TmStatusService::isNotEnable).thenReturn(false);

				errorAtomic.set(new RuntimeException("enable and not status exception"));
				handleTaskOperationRunnable.run();
				verify(logger, times(1)).error(anyString(), eq(errorAtomic.get()));

				errorAtomic.set(new TmUnavailableException("enable and is status exception", "post", null, new ResponseBody()));
				handleTaskOperationRunnable.run();
				verify(logger, times(1)).warn(anyString(), eq(errorAtomic.get().getMessage()));
			}

			// disable TM status
			try (MockedStatic<TmStatusService> tmStatusServiceMockedStatic = mockStatic(TmStatusService.class)) {
				tmStatusServiceMockedStatic.when(TmStatusService::isNotEnable).thenReturn(true);

				errorAtomic.set(new RuntimeException("disable and not status exception"));
				handleTaskOperationRunnable.run();
				verify(logger, times(1)).error(anyString(), eq(errorAtomic.get()));

				errorAtomic.set(new TmUnavailableException("disable and is status exception", "post", null, new ResponseBody()));
				handleTaskOperationRunnable.run();
				verify(logger, times(1)).error(anyString(), eq(errorAtomic.get()));
			}
		}

		@Test
		void testStartTaskOperation() {
			TaskDto taskDto = new TaskDto();
			taskDto.setId(ObjectId.get());
			taskDto.setName("test-task");
			TaskOperation taskOperation = StartTaskOperation.create().taskDto(taskDto);
			AtomicBoolean isCallStartTask = new AtomicBoolean(false);
			TapdataTaskScheduler instance = new TapdataTaskScheduler() {
				@Override
				protected void startTask(TaskDto taskDto) {
					isCallStartTask.set(true);
				}
			};

			Runnable handleTaskOperationRunnable = instance.getHandleTaskOperationRunnable(taskOperation);
			assertNotNull(handleTaskOperationRunnable);
			handleTaskOperationRunnable.run();
			assertTrue(isCallStartTask.get());
		}

		@Test
		void testStopTaskOperation() {
			String taskId = "test-task-id";
			TaskOperation taskOperation = StopTaskOperation.create().taskId(taskId);
			AtomicBoolean isCallStopTask = new AtomicBoolean(false);
			TapdataTaskScheduler instance = new TapdataTaskScheduler() {
				@Override
				protected void startTask(TaskDto taskDto) {
					isCallStopTask.set(true);
				}
			};
			Runnable handleTaskOperationRunnable = instance.getHandleTaskOperationRunnable(taskOperation);
			assertNotNull(handleTaskOperationRunnable);
			handleTaskOperationRunnable.run();
			assertTrue(isCallStopTask.get());
		}
	}

	@Test
	void testHandleTaskOperation() {
		TaskOperation taskOperation = mock(TaskOperation.class);
		TapdataTaskScheduler instance = new TapdataTaskScheduler();
		instance.handleTaskOperation(taskOperation);
	}
}
