package com.tapdata.tm.schedule;

import com.tapdata.tm.apiCalls.service.ApiCallService;
import com.tapdata.tm.apiCalls.service.SupplementApiCallServer;
import com.tapdata.tm.apiCalls.service.WorkerCallServiceImpl;
import com.tapdata.tm.apicallminutestats.dto.ApiCallMinuteStatsDto;
import com.tapdata.tm.apicallminutestats.service.ApiCallMinuteStatsService;
import com.tapdata.tm.module.dto.ModulesDto;
import com.tapdata.tm.modules.service.ModulesService;
import com.tapdata.tm.worker.dto.WorkerDto;
import com.tapdata.tm.worker.service.WorkerService;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.scheduling.annotation.Scheduled;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * @author samuel
 * @Description
 * @create 2024-09-02 19:25
 **/
@DisplayName("Class ApiCallMinuteStatsScheduler Test")
class ApiCallMinuteStatsSchedulerTest {

	private ModulesService modulesService;
	private ApiCallMinuteStatsService apiCallMinuteStatsService;
	private ApiCallService apiCallService;
	private ApiCallMinuteStatsScheduler apiCallMinuteStatsScheduler;
	private WorkerCallServiceImpl workerCallServiceImpl;
	WorkerService workerService;
	SupplementApiCallServer supplementApiCallServer;

	@BeforeEach
	void setUp() {
		supplementApiCallServer = mock(SupplementApiCallServer.class);
		doNothing().when(supplementApiCallServer).supplementOnce();
		modulesService = mock(ModulesService.class);
		apiCallMinuteStatsService = mock(ApiCallMinuteStatsService.class);
		apiCallService = mock(ApiCallService.class);
		workerCallServiceImpl = mock(WorkerCallServiceImpl.class);
		workerService = mock(WorkerService.class);
		apiCallMinuteStatsScheduler = new ApiCallMinuteStatsScheduler(modulesService, apiCallMinuteStatsService, apiCallService, workerCallServiceImpl, workerService, supplementApiCallServer);
	}

//	@Test
//	@DisplayName("test scheduler config")
//	void test1() {
//		apiCallMinuteStatsScheduler.schedule();
//		assertEquals("ApiCallMinuteStatsScheduler-scheduler", Thread.currentThread().getName());
//		Method schedule = assertDoesNotThrow(() -> apiCallMinuteStatsScheduler.getClass().getDeclaredMethod("schedule"));
//		Scheduled scheduleAnnotation = schedule.getAnnotation(Scheduled.class);
//		assertEquals("0 0/1 * * * ?", scheduleAnnotation.cron());
//		SchedulerLock schedulerLockAnnotation = schedule.getAnnotation(SchedulerLock.class);
//		assertEquals("api_call_minute_stats_scheduler", schedulerLockAnnotation.name());
//		assertEquals("30m", schedulerLockAnnotation.lockAtMostFor());
//		assertEquals("5s", schedulerLockAnnotation.lockAtLeastFor());
//	}

	@Nested
	@DisplayName("test schedule method")
	class TestScheduleMethod {

		private List<ModulesDto> modulesList;
		private ApiCallMinuteStatsDto lastApiCallMinuteStatsDto;
		private List<ApiCallMinuteStatsDto> apiCallMinuteStatsDtoList;

		@BeforeEach
		void setUp() {
			modulesList = new ArrayList<ModulesDto>() {{
				add(new ModulesDto() {{
					setId(new ObjectId());
					setUserId("test");
				}});
			}};
			lastApiCallMinuteStatsDto = new ApiCallMinuteStatsDto() {{
				setLastApiCallId(new ObjectId().toString());
			}};
			apiCallMinuteStatsDtoList = new ArrayList<ApiCallMinuteStatsDto>() {{
				add(new ApiCallMinuteStatsDto());
			}};
		}

		@Test
		@DisplayName("test schedule method")
		void test1() {
			when(modulesService.findAll(any(Query.class))).thenAnswer(invocationOnMock -> {
				Query query = invocationOnMock.getArgument(0);
				assertEquals("{\"is_deleted\": {\"$ne\": true}}", query.getQueryObject().toJson());
				assertEquals("{\"user_id\": 1, \"id\": 1}", query.getFieldsObject().toJson());
				return modulesList;
			});
			when(apiCallMinuteStatsService.findOne(any(Query.class))).thenAnswer(invocationOnMock -> {
				Query query = invocationOnMock.getArgument(0);
				assertEquals("{\"moduleId\": \"" + modulesList.get(0).getId().toString() + "\"}", query.getQueryObject().toJson());
				assertEquals("{\"_id\": -1}", query.getSortObject().toJson());
				assertEquals(1, query.getLimit());
				return lastApiCallMinuteStatsDto;
			});
			when(apiCallService.aggregateMinuteByAllPathId(anyString(), anyString(), any(Date.class))).thenAnswer(invocationOnMock -> {
				assertEquals(modulesList.get(0).getId().toString(), invocationOnMock.getArgument(0));
				assertEquals(lastApiCallMinuteStatsDto.getLastApiCallId(), invocationOnMock.getArgument(1));
				assertNotNull(invocationOnMock.getArgument(2));
				assertTrue(new Date().getTime() > ((Date) invocationOnMock.getArgument(2)).getTime());
				return apiCallMinuteStatsDtoList;
			});
			doAnswer(invocationOnMock -> {
				List<ApiCallMinuteStatsDto> list = invocationOnMock.getArgument(0);
				assertEquals(1, list.size());
				assertSame(apiCallMinuteStatsDtoList.get(0), list.get(0));
				assertEquals(modulesList.get(0).getUserId(), list.get(0).getUserId());
				assertNotNull(list.get(0).getCreateAt());
				return null;
			}).when(apiCallMinuteStatsService).bulkWrite(any(List.class), any(Class.class), any());

			apiCallMinuteStatsScheduler.schedule();
		}

		@Test
		@DisplayName("test modules oid is null")
		void test2() {
			modulesList = new ArrayList<ModulesDto>() {{
				add(new ModulesDto());
			}};
			when(modulesService.findAll(any(Query.class))).thenReturn(modulesList);

			apiCallMinuteStatsScheduler.schedule();

			verify(apiCallService, never()).aggregateByAllPathId(any(), any());
		}

		@Test
		@DisplayName("test last api call id is blank")
		void test3() {
			when(modulesService.findAll(any(Query.class))).thenReturn(modulesList);
			lastApiCallMinuteStatsDto.setLastApiCallId("");
			when(apiCallMinuteStatsService.findOne(any(Query.class))).thenReturn(lastApiCallMinuteStatsDto);
			when(apiCallService.aggregateMinuteByAllPathId(anyString(), any(), any(Date.class))).thenAnswer(invocationOnMock -> {
				assertNull(invocationOnMock.getArgument(1));
				return apiCallMinuteStatsDtoList;
			});
			apiCallMinuteStatsScheduler.schedule();

			when(modulesService.findAll(any(Query.class))).thenReturn(modulesList);
			lastApiCallMinuteStatsDto.setLastApiCallId("    ");
			when(apiCallService.aggregateMinuteByAllPathId(anyString(), any(), any(Date.class))).thenAnswer(invocationOnMock -> {
				assertNull(invocationOnMock.getArgument(1));
				return apiCallMinuteStatsDtoList;
			});
			apiCallMinuteStatsScheduler.schedule();

			when(modulesService.findAll(any(Query.class))).thenReturn(modulesList);
			lastApiCallMinuteStatsDto.setLastApiCallId(null);
			when(apiCallService.aggregateMinuteByAllPathId(anyString(), any(), any(Date.class))).thenAnswer(invocationOnMock -> {
				assertNull(invocationOnMock.getArgument(1));
				return apiCallMinuteStatsDtoList;
			});
			apiCallMinuteStatsScheduler.schedule();
		}

		@Test
		@DisplayName("test last api call minute stats not exists")
		void test4() {
			when(modulesService.findAll(any(Query.class))).thenReturn(modulesList);
			when(apiCallMinuteStatsService.findOne(any(Query.class))).thenReturn(null);
			when(apiCallService.aggregateMinuteByAllPathId(anyString(), any(), any(Date.class))).thenAnswer(invocationOnMock -> {
				assertNull(invocationOnMock.getArgument(1));
				return apiCallMinuteStatsDtoList;
			});
			apiCallMinuteStatsScheduler.schedule();
		}
	}

	@Nested
	class scheduleWorkerCallTest {
		@Test
		@DisplayName("test schedule worker call")
		void test1() {
			doNothing().when(workerCallServiceImpl).metric();
			when(workerService.findAll(any(Query.class))).thenReturn(new ArrayList<>());
			doNothing().when(workerCallServiceImpl).collectApiCallCountGroupByWorker(anyString());
			Assertions.assertDoesNotThrow(apiCallMinuteStatsScheduler::scheduleWorkerCall);
			verify(workerCallServiceImpl, times(0)).collectApiCallCountGroupByWorker(anyString());
		}
		@Test
		@DisplayName("test schedule worker call")
		void testNull() {
			doNothing().when(workerCallServiceImpl).metric();
			when(workerService.findAll(any(Query.class))).thenReturn(null);
			doNothing().when(workerCallServiceImpl).collectApiCallCountGroupByWorker(anyString());
			Assertions.assertDoesNotThrow(apiCallMinuteStatsScheduler::scheduleWorkerCall);
			verify(workerCallServiceImpl, times(0)).collectApiCallCountGroupByWorker(anyString());
		}

		@Test
		void testException() {
			doAnswer(a -> {throw new RuntimeException("test");}).when(workerCallServiceImpl).metric();
			when(workerService.findAll(any(Query.class))).thenReturn(new ArrayList<>());
			doAnswer(a -> {throw new RuntimeException("test");}).when(workerCallServiceImpl).collectApiCallCountGroupByWorker(anyString());
			Assertions.assertDoesNotThrow(apiCallMinuteStatsScheduler::scheduleWorkerCall);
			verify(workerCallServiceImpl, times(0)).collectApiCallCountGroupByWorker(anyString());
		}

		@Test
		void testException1() {
			doAnswer(a -> {throw new RuntimeException("test");}).when(workerCallServiceImpl).metric();
			when(workerService.findAll(any(Query.class))).thenAnswer(a -> {throw new RuntimeException("test");});
			doAnswer(a -> {throw new RuntimeException("test");}).when(workerCallServiceImpl).collectApiCallCountGroupByWorker(anyString());
			Assertions.assertDoesNotThrow(apiCallMinuteStatsScheduler::scheduleWorkerCall);
			verify(workerCallServiceImpl, times(0)).collectApiCallCountGroupByWorker(anyString());
		}

		@Test
		void testException2() {
			ArrayList<WorkerDto> objects = new ArrayList<>();
			WorkerDto dto = new WorkerDto();
			dto.setProcessId("1");
			objects.add(dto);
			doThrow(new RuntimeException("test")).when(workerCallServiceImpl).metric();
			when(workerService.findAll(any(Query.class))).thenReturn(objects);
			doThrow(new RuntimeException("test")).when(workerCallServiceImpl).collectApiCallCountGroupByWorker(anyString());
			Assertions.assertDoesNotThrow(apiCallMinuteStatsScheduler::scheduleWorkerCall);
			verify(workerCallServiceImpl, times(objects.size())).collectApiCallCountGroupByWorker(anyString());
		}
	}
}