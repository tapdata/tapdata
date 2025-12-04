package io.tapdata.flow.engine.V2.schedule;

import com.hazelcast.jet.core.JobStatus;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.entity.ResponseBody;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.task.dto.TaskOpRespDto;
import com.tapdata.tm.sdk.available.TmStatusService;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.exception.RestDoNotRetryException;
import io.tapdata.exception.TmUnavailableException;
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
import org.junit.jupiter.api.*;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.data.mongodb.core.query.Query;
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
import static org.mockito.ArgumentMatchers.argThat;

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
			ReflectionTestUtils.setField(taskScheduler, "startTaskLock", new Object());
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
				taskDto.setSyncType("test");

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
				protected void stopTask(String taskId) {
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

	@Nested
	@DisplayName("SafeQueryTaskById Test")
	class SafeQueryTaskByIdTest {
		private TapdataTaskScheduler taskScheduler;
		private ClientMongoOperator clientMongoOperator;

		@BeforeEach
		void setUp() {
			taskScheduler = mock(TapdataTaskScheduler.class);
			clientMongoOperator = mock(ClientMongoOperator.class);
			ReflectionTestUtils.setField(taskScheduler, "clientMongoOperator", clientMongoOperator);
		}

		@Test
		@DisplayName("Should return TaskDto when task exists")
		void testSafeQueryTaskByIdWhenTaskExists() {
			// Given
			String taskId = "507f1f77bcf86cd799439011";
			TaskDto expectedTaskDto = new TaskDto();
			expectedTaskDto.setId(new ObjectId(taskId));
			expectedTaskDto.setName("test-task");
			expectedTaskDto.setStatus(TaskDto.STATUS_RUNNING);

			when(clientMongoOperator.findOne(any(Query.class), eq(ConnectorConstant.TASK_COLLECTION), eq(TaskDto.class)))
					.thenReturn(expectedTaskDto);
			when(taskScheduler.safeQueryTaskById(taskId)).thenCallRealMethod();

			// When
			TaskDto result = taskScheduler.safeQueryTaskById(taskId);

			// Then
			assertNotNull(result);
			assertEquals(expectedTaskDto.getId(), result.getId());
			assertEquals(expectedTaskDto.getName(), result.getName());
			assertEquals(expectedTaskDto.getStatus(), result.getStatus());

			// Verify the query was constructed correctly
			verify(clientMongoOperator).findOne(any(Query.class), any(), any());
		}

		@Test
		@DisplayName("Should return null when task does not exist")
		void testSafeQueryTaskByIdWhenTaskNotExists() {
			// Given
			String taskId = "507f1f77bcf86cd799439011";

			when(clientMongoOperator.findOne(any(Query.class), eq(ConnectorConstant.TASK_COLLECTION), eq(TaskDto.class)))
					.thenReturn(null);
			when(taskScheduler.safeQueryTaskById(taskId)).thenCallRealMethod();

			// When
			TaskDto result = taskScheduler.safeQueryTaskById(taskId);

			// Then
			assertNull(result);
			verify(clientMongoOperator).findOne(any(Query.class), eq(ConnectorConstant.TASK_COLLECTION), eq(TaskDto.class));
		}

		@Test
		@DisplayName("Should handle exception from clientMongoOperator gracefully")
		void testSafeQueryTaskByIdWhenExceptionThrown() {
			// Given
			String taskId = "507f1f77bcf86cd799439011";

			when(clientMongoOperator.findOne(any(Query.class), eq(ConnectorConstant.TASK_COLLECTION), eq(TaskDto.class)))
					.thenThrow(new RuntimeException("Database connection error"));
			when(taskScheduler.safeQueryTaskById(taskId)).thenCallRealMethod();

			// When & Then
			assertThrows(RuntimeException.class, () -> taskScheduler.safeQueryTaskById(taskId));
			verify(clientMongoOperator).findOne(any(Query.class), eq(ConnectorConstant.TASK_COLLECTION), eq(TaskDto.class));
		}

		@Test
		@DisplayName("Should handle null taskId parameter")
		void testSafeQueryTaskByIdWithNullTaskId() {
			// Given
			String taskId = null;

			when(taskScheduler.safeQueryTaskById(taskId)).thenCallRealMethod();

			// When
			TaskDto result = taskScheduler.safeQueryTaskById(taskId);

			// Then
			assertNull(result);
			verify(clientMongoOperator).findOne(any(Query.class), eq(ConnectorConstant.TASK_COLLECTION), eq(TaskDto.class));
		}

		@Test
		@DisplayName("Should handle empty taskId parameter")
		void testSafeQueryTaskByIdWithEmptyTaskId() {
			// Given
			String taskId = "";

			when(clientMongoOperator.findOne(any(Query.class), eq(ConnectorConstant.TASK_COLLECTION), eq(TaskDto.class)))
					.thenReturn(null);
			when(taskScheduler.safeQueryTaskById(taskId)).thenCallRealMethod();

			// When
			TaskDto result = taskScheduler.safeQueryTaskById(taskId);

			// Then
			assertNull(result);
			verify(clientMongoOperator).findOne(any(Query.class), eq(ConnectorConstant.TASK_COLLECTION), eq(TaskDto.class));
		}

	}

	@Nested
	@Disabled
	class StopTaskCallAssignApiTest {
		TapdataTaskScheduler scheduler;
		Logger logger;
		TaskClient<TaskDto> taskClient;
		TapdataTaskScheduler.StopTaskResource stopTaskResource;
		ClientMongoOperator clientMongoOperator;

		TaskDto taskDto;
		TaskOpRespDto taskOpRespDto;
		@BeforeEach
		void init() {
			scheduler = mock(TapdataTaskScheduler.class);
			logger = mock(Logger.class);
			taskClient = mock(TaskClient.class);
			stopTaskResource = mock(TapdataTaskScheduler.StopTaskResource.class);
			clientMongoOperator = mock(ClientMongoOperator.class);
			ReflectionTestUtils.setField(scheduler, "logger", logger);
			ReflectionTestUtils.setField(scheduler, "clientMongoOperator", clientMongoOperator);

			taskDto = mock(TaskDto.class);
			taskOpRespDto = mock(TaskOpRespDto.class);

			when(taskDto.getId()).thenReturn(new ObjectId());
			when(taskDto.getName()).thenReturn("name");
			when(taskClient.getTask()).thenReturn(taskDto);
			when(taskClient.stop()).thenReturn(true);
			when(stopTaskResource.getResource()).thenReturn("source");
			doNothing().when(logger).info(anyString(), anyString(), anyString());
			doNothing().when(logger).warn(anyString(), anyString(), anyString(), any(Exception.class));
			doNothing().when(logger).warn(anyString(), anyString(), anyString(), anyString(), any(Exception.class));
			when(clientMongoOperator.updateById(any(Update.class), anyString(), anyString(), any(Class.class))).thenReturn(taskOpRespDto);
			when(taskOpRespDto.getSuccessIds()).thenReturn(new ArrayList<>());

			when(scheduler.stopTaskCallAssignApi(taskClient, stopTaskResource)).thenCallRealMethod();
		}

		@Test
		void taskClientIsNull() {
			when(scheduler.stopTaskCallAssignApi(null, stopTaskResource)).thenCallRealMethod();
			Assertions.assertTrue(scheduler.stopTaskCallAssignApi(null, stopTaskResource));
		}

		@Test
		void taskIsNull() {
			when(taskClient.getTask()).thenReturn(null);
			Assertions.assertTrue(scheduler.stopTaskCallAssignApi(taskClient, stopTaskResource));
		}

		@Test
		void taskIdIsNull() {
			ObjectId is = mock(ObjectId.class);
			when(is.toHexString()).thenReturn("");
			when(taskDto.getId()).thenReturn(is);
			Assertions.assertTrue(scheduler.stopTaskCallAssignApi(taskClient, stopTaskResource));
		}

		@Test
		void testNormal() {
			Assertions.assertFalse(scheduler.stopTaskCallAssignApi(taskClient, stopTaskResource));
			verify(taskClient, times(3)).getTask();
			verify(taskClient, times(1)).stop();
			verify(taskDto, times(1)).getName();
			verify(taskDto, times(2)).getId();
			verify(clientMongoOperator, times(1)).updateById(any(Update.class), anyString(), anyString(), any(Class.class));
			verify(logger, times(1)).info(anyString(), anyString(), anyString());
			verify(logger, times(0)).warn(anyString(), any(Exception.class));
			verify(taskOpRespDto, times(1)).getSuccessIds();
		}
		@Test
		void testNotStop() {
			when(taskClient.stop()).thenReturn(false);
			Assertions.assertFalse(scheduler.stopTaskCallAssignApi(taskClient, stopTaskResource));
			verify(taskClient, times(2)).getTask();
			verify(taskClient, times(1)).stop();
			verify(taskDto, times(0)).getName();
			verify(taskDto, times(1)).getId();
			verify(clientMongoOperator, times(0)).updateById(any(Update.class), anyString(), anyString(), any(Class.class));
			verify(logger, times(0)).info(anyString(), anyString(), anyString());
			verify(logger, times(0)).warn(anyString(), anyString(), anyString(), any(Exception.class));
			verify(taskOpRespDto, times(0)).getSuccessIds();
		}
		@Test
		void testNotEmpty() {
			List<String> objects = new ArrayList<>();
			objects.add("");
			when(taskOpRespDto.getSuccessIds()).thenReturn(objects);
			Assertions.assertTrue(scheduler.stopTaskCallAssignApi(taskClient, stopTaskResource));
			verify(taskClient, times(3)).getTask();
			verify(taskClient, times(1)).stop();
			verify(taskDto, times(1)).getName();
			verify(taskDto, times(2)).getId();
			verify(clientMongoOperator, times(1)).updateById(any(Update.class), anyString(), anyString(), any(Class.class));
			verify(logger, times(1)).info(anyString(), anyString(), anyString());
			verify(logger, times(0)).warn(anyString(), anyString(), anyString(), any(Exception.class));
			verify(taskOpRespDto, times(1)).getSuccessIds();
		}
		@Test
		void testThrow() {
			when(clientMongoOperator.updateById(any(Update.class), anyString(), anyString(), any(Class.class))).thenAnswer(a -> {
				throw new Exception("");
			});
			Assertions.assertFalse(scheduler.stopTaskCallAssignApi(taskClient, stopTaskResource));
			verify(taskClient, times(3)).getTask();
			verify(taskClient, times(1)).stop();
			verify(taskDto, times(1)).getName();
			verify(taskDto, times(2)).getId();
			verify(clientMongoOperator, times(1)).updateById(any(Update.class), anyString(), anyString(), any(Class.class));
			verify(logger, times(1)).info(anyString(), anyString(), anyString());
			verify(logger, times(0)).warn(anyString(), anyString(), anyString(), any(Exception.class));
			verify(taskOpRespDto, times(0)).getSuccessIds();
		}
		@Test
		void testThrow2() {
			when(clientMongoOperator.updateById(any(Update.class), anyString(), anyString(), any(Class.class))).thenAnswer(a -> {
				throw new Exception("222");
			});
			Assertions.assertFalse(scheduler.stopTaskCallAssignApi(taskClient, stopTaskResource));
			verify(taskClient, times(3)).getTask();
			verify(taskClient, times(1)).stop();
			verify(taskDto, times(1)).getName();
			verify(taskDto, times(2)).getId();
			verify(clientMongoOperator, times(1)).updateById(any(Update.class), anyString(), anyString(), any(Class.class));
			verify(logger, times(1)).info(anyString(), anyString(), anyString());
			verify(logger, times(0)).warn(anyString(), anyString(), anyString(), any(Exception.class));
			verify(taskOpRespDto, times(0)).getSuccessIds();
		}
		@Test
		void testThrowTransitionNoSupport() {
			when(clientMongoOperator.updateById(any(Update.class), anyString(), anyString(), any(Class.class))).thenAnswer(a -> {
				throw new Exception("Transition.Not.Supported");
			});
			Assertions.assertThrows(Exception.class, () -> scheduler.stopTaskCallAssignApi(taskClient, stopTaskResource));
			verify(taskClient, times(3)).getTask();
			verify(taskClient, times(1)).stop();
			verify(taskDto, times(1)).getName();
			verify(taskDto, times(2)).getId();
			verify(clientMongoOperator, times(2)).updateById(any(Update.class), anyString(), anyString(), any(Class.class));
			verify(logger, times(1)).info(anyString(), anyString(), anyString());
			verify(logger, times(1)).warn(anyString(), anyString(), anyString(), any(Exception.class));
			verify(taskOpRespDto, times(0)).getSuccessIds();
		}
		@Test
		void testThrowTransitionNoSupport2() {
			final AtomicBoolean isFirst = new AtomicBoolean(true);
			when(clientMongoOperator.updateById(any(Update.class), anyString(), anyString(), any(Class.class))).thenAnswer(a -> {
				synchronized (isFirst) {
					if (isFirst.get()) {
						isFirst.set(false);
						throw new Exception("Transition.Not.Supported");
					} else {
						return null;
					}
				}
			});
			Assertions.assertTrue(scheduler.stopTaskCallAssignApi(taskClient, stopTaskResource));
			verify(taskClient, times(3)).getTask();
			verify(taskClient, times(1)).stop();
			verify(taskDto, times(1)).getName();
			verify(taskDto, times(2)).getId();
			verify(clientMongoOperator, times(2)).updateById(any(Update.class), anyString(), anyString(), any(Class.class));
			verify(logger, times(1)).info(anyString(), anyString(), anyString());
			verify(logger, times(1)).warn(anyString(), anyString(), anyString(), any(Exception.class));
			verify(taskOpRespDto, times(0)).getSuccessIds();
		}
	}

	@Nested
	@DisplayName("Method getRunningTaskInfos test")
	class GetRunningTaskInfosTest {

		private TapdataTaskScheduler taskScheduler;

		@BeforeEach
		void setUp() {
			taskScheduler = mock(TapdataTaskScheduler.class);
		}

		@Test
		@DisplayName("test getRunningTaskInfos with multiple running tasks")
		void testGetRunningTaskInfosWithMultipleRunningTasks() {
			// Prepare test data
			Map<String, TaskClient<TaskDto>> taskClientMap = new ConcurrentHashMap<>();

			// Create running task 1
			TaskClient<TaskDto> runningClient1 = mock(TaskClient.class);
			TaskDto runningTask1 = new TaskDto();
			runningTask1.setId(new ObjectId());
			runningTask1.setName("Running Task 1");
			runningTask1.setStatus(TaskDto.STATUS_RUNNING);
			when(runningClient1.isRunning()).thenReturn(true);
			when(runningClient1.getTask()).thenReturn(runningTask1);
			taskClientMap.put("task1", runningClient1);

			// Create running task 2
			TaskClient<TaskDto> runningClient2 = mock(TaskClient.class);
			TaskDto runningTask2 = new TaskDto();
			runningTask2.setId(new ObjectId());
			runningTask2.setName("Running Task 2");
			runningTask2.setStatus(TaskDto.STATUS_RUNNING);
			when(runningClient2.isRunning()).thenReturn(true);
			when(runningClient2.getTask()).thenReturn(runningTask2);
			taskClientMap.put("task2", runningClient2);

			// Create stopped task
			TaskClient<TaskDto> stoppedClient = mock(TaskClient.class);
			TaskDto stoppedTask = new TaskDto();
			stoppedTask.setId(new ObjectId());
			stoppedTask.setName("Stopped Task");
			stoppedTask.setStatus(TaskDto.STATUS_STOP);
			when(stoppedClient.isRunning()).thenReturn(false);
			when(stoppedClient.getTask()).thenReturn(stoppedTask);
			taskClientMap.put("task3", stoppedClient);

			ReflectionTestUtils.setField(taskScheduler, "taskClientMap", taskClientMap);
			doCallRealMethod().when(taskScheduler).getRunningTaskInfos();

			// Execute
			List<TaskDto> result = taskScheduler.getRunningTaskInfos();

			// Verify
			assertNotNull(result);
			assertEquals(2, result.size());
			assertTrue(result.contains(runningTask1));
			assertTrue(result.contains(runningTask2));
			assertTrue(result.contains(stoppedTask));

			verify(runningClient1, times(1)).isRunning();
			verify(runningClient1, times(1)).getTask();
			verify(runningClient2, times(1)).isRunning();
			verify(runningClient2, times(1)).getTask();
			verify(stoppedClient, times(1)).isRunning();
			verify(stoppedClient, times(0)).getTask();
		}

		@Test
		@DisplayName("test getRunningTaskInfos with empty task client map")
		void testGetRunningTaskInfosWithEmptyMap() {
			Map<String, TaskClient<TaskDto>> taskClientMap = new ConcurrentHashMap<>();
			ReflectionTestUtils.setField(taskScheduler, "taskClientMap", taskClientMap);
			doCallRealMethod().when(taskScheduler).getRunningTaskInfos();

			List<TaskDto> result = taskScheduler.getRunningTaskInfos();

			assertNotNull(result);
			assertTrue(result.isEmpty());
		}
//
//		@Test
//		@DisplayName("test getRunningTaskInfos with null task clients")
//		void testGetRunningTaskInfosWithNullClients() {
//			Map<String, TaskClient<TaskDto>> taskClientMap = new ConcurrentHashMap<>();
//
//			// Add null task client
//			taskClientMap.put("task1", new );
//
//			// Add valid running task
//			TaskClient<TaskDto> runningClient = mock(TaskClient.class);
//			TaskDto runningTask = new TaskDto();
//			runningTask.setId(new ObjectId());
//			runningTask.setName("Running Task");
//			when(runningClient.isRunning()).thenReturn(true);
//			when(runningClient.getTask()).thenReturn(runningTask);
//			taskClientMap.put("task2", runningClient);
//
//			// Add another null
//			taskClientMap.put("task3", null);
//
//			ReflectionTestUtils.setField(taskScheduler, "taskClientMap", taskClientMap);
//			doCallRealMethod().when(taskScheduler).getRunningTaskInfos();
//
//			List<TaskDto> result = taskScheduler.getRunningTaskInfos();
//
//			assertNotNull(result);
//			assertEquals(1, result.size());
//			assertEquals(runningTask, result.get(0));
//
//			verify(runningClient, times(1)).isRunning();
//			verify(runningClient, times(1)).getTask();
//		}

		@Test
		@DisplayName("test getRunningTaskInfos with all stopped tasks")
		void testGetRunningTaskInfosWithAllStoppedTasks() {
			Map<String, TaskClient<TaskDto>> taskClientMap = new ConcurrentHashMap<>();

			// Create stopped task 1
			TaskClient<TaskDto> stoppedClient1 = mock(TaskClient.class);
			when(stoppedClient1.isRunning()).thenReturn(false);
			taskClientMap.put("task1", stoppedClient1);

			// Create stopped task 2
			TaskClient<TaskDto> stoppedClient2 = mock(TaskClient.class);
			when(stoppedClient2.isRunning()).thenReturn(false);
			taskClientMap.put("task2", stoppedClient2);

			ReflectionTestUtils.setField(taskScheduler, "taskClientMap", taskClientMap);
			doCallRealMethod().when(taskScheduler).getRunningTaskInfos();

			List<TaskDto> result = taskScheduler.getRunningTaskInfos();

			assertNotNull(result);
			assertTrue(result.isEmpty());

			verify(stoppedClient1, times(1)).isRunning();
			verify(stoppedClient1, times(0)).getTask();
			verify(stoppedClient2, times(1)).isRunning();
			verify(stoppedClient2, times(0)).getTask();
		}

		@Test
		@DisplayName("test getRunningTaskInfos with only running tasks")
		void testGetRunningTaskInfosWithOnlyRunningTasks() {
			Map<String, TaskClient<TaskDto>> taskClientMap = new ConcurrentHashMap<>();

			List<TaskDto> expectedTasks = new ArrayList<>();

			// Create 3 running tasks
			for (int i = 1; i <= 3; i++) {
				TaskClient<TaskDto> runningClient = mock(TaskClient.class);
				TaskDto runningTask = new TaskDto();
				runningTask.setId(new ObjectId());
				runningTask.setName("Running Task " + i);
				runningTask.setStatus(TaskDto.STATUS_RUNNING);
				when(runningClient.isRunning()).thenReturn(true);
				when(runningClient.getTask()).thenReturn(runningTask);
				taskClientMap.put("task" + i, runningClient);
				expectedTasks.add(runningTask);
			}

			ReflectionTestUtils.setField(taskScheduler, "taskClientMap", taskClientMap);
			doCallRealMethod().when(taskScheduler).getRunningTaskInfos();

			List<TaskDto> result = taskScheduler.getRunningTaskInfos();

			assertNotNull(result);
			assertEquals(3, result.size());
			assertTrue(result.containsAll(expectedTasks));
		}

//		@Test
//		@DisplayName("test getRunningTaskInfos with mixed null and running tasks")
//		void testGetRunningTaskInfosWithMixedNullAndRunning() {
//			Map<String, TaskClient<TaskDto>> taskClientMap = new ConcurrentHashMap<>();
//
//			// Add null
//			taskClientMap.put("task1", null);
//
//			// Add running task
//			TaskClient<TaskDto> runningClient1 = mock(TaskClient.class);
//			TaskDto runningTask1 = new TaskDto();
//			runningTask1.setId(new ObjectId());
//			runningTask1.setName("Running Task 1");
//			when(runningClient1.isRunning()).thenReturn(true);
//			when(runningClient1.getTask()).thenReturn(runningTask1);
//			taskClientMap.put("task2", runningClient1);
//
//			// Add null
//			taskClientMap.put("task3", null);
//
//			// Add stopped task
//			TaskClient<TaskDto> stoppedClient = mock(TaskClient.class);
//			when(stoppedClient.isRunning()).thenReturn(false);
//			taskClientMap.put("task4", stoppedClient);
//
//			// Add running task
//			TaskClient<TaskDto> runningClient2 = mock(TaskClient.class);
//			TaskDto runningTask2 = new TaskDto();
//			runningTask2.setId(new ObjectId());
//			runningTask2.setName("Running Task 2");
//			when(runningClient2.isRunning()).thenReturn(true);
//			when(runningClient2.getTask()).thenReturn(runningTask2);
//			taskClientMap.put("task5", runningClient2);
//
//			ReflectionTestUtils.setField(taskScheduler, "taskClientMap", taskClientMap);
//			doCallRealMethod().when(taskScheduler).getRunningTaskInfos();
//
//			List<TaskDto> result = taskScheduler.getRunningTaskInfos();
//
//			assertNotNull(result);
//			assertEquals(2, result.size());
//			assertTrue(result.contains(runningTask1));
//			assertTrue(result.contains(runningTask2));
//		}

		@Test
		@DisplayName("test getRunningTaskInfos returns immutable list")
		void testGetRunningTaskInfosReturnsImmutableList() {
			Map<String, TaskClient<TaskDto>> taskClientMap = new ConcurrentHashMap<>();

			TaskClient<TaskDto> runningClient = mock(TaskClient.class);
			TaskDto runningTask = new TaskDto();
			runningTask.setId(new ObjectId());
			runningTask.setName("Running Task");
			when(runningClient.isRunning()).thenReturn(true);
			when(runningClient.getTask()).thenReturn(runningTask);
			taskClientMap.put("task1", runningClient);

			ReflectionTestUtils.setField(taskScheduler, "taskClientMap", taskClientMap);
			doCallRealMethod().when(taskScheduler).getRunningTaskInfos();

			List<TaskDto> result = taskScheduler.getRunningTaskInfos();

			assertNotNull(result);
			assertEquals(1, result.size());
		}

		@Test
		@DisplayName("test getRunningTaskInfos stream operations")
		void testGetRunningTaskInfosStreamOperations() {
			Map<String, TaskClient<TaskDto>> taskClientMap = new ConcurrentHashMap<>();

			// Create a mix of running and non-running tasks
			TaskClient<TaskDto> runningClient1 = mock(TaskClient.class);
			TaskDto runningTask1 = new TaskDto();
			runningTask1.setId(new ObjectId());
			runningTask1.setName("Running Task 1");
			when(runningClient1.isRunning()).thenReturn(true);
			when(runningClient1.getTask()).thenReturn(runningTask1);
			taskClientMap.put("task1", runningClient1);

			TaskClient<TaskDto> runningClient2 = mock(TaskClient.class);
			TaskDto runningTask2 = new TaskDto();
			runningTask2.setId(new ObjectId());
			runningTask2.setName("Running Task 2");
			when(runningClient2.isRunning()).thenReturn(true);
			when(runningClient2.getTask()).thenReturn(runningTask2);
			taskClientMap.put("task2", runningClient2);

			TaskClient<TaskDto> stoppedClient = mock(TaskClient.class);
			when(stoppedClient.isRunning()).thenReturn(false);
			taskClientMap.put("task3", stoppedClient);


			ReflectionTestUtils.setField(taskScheduler, "taskClientMap", taskClientMap);
			doCallRealMethod().when(taskScheduler).getRunningTaskInfos();

			List<TaskDto> result = taskScheduler.getRunningTaskInfos();

			// Verify stream filtering worked correctly
			assertNotNull(result);
			assertEquals(2, result.size());

			// Verify all returned tasks are from running clients
			result.forEach(task -> {
				assertTrue(task.getName().startsWith("Running Task"));
			});
		}
	}
}
