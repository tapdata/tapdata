package com.tapdata.tm.schedule;

import ch.qos.logback.classic.Logger;
import com.tapdata.tm.apiCalls.service.ApiCallService;
import com.tapdata.tm.apicallstats.dto.ApiCallStatsDto;
import com.tapdata.tm.apicallstats.service.ApiCallStatsService;
import com.tapdata.tm.modules.dto.ModulesDto;
import com.tapdata.tm.modules.service.ModulesService;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.test.util.ReflectionTestUtils;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author samuel
 * @Description
 * @create 2024-08-30 18:28
 **/
@DisplayName("Class ApiCallStatsScheduler Test")
class ApiCallStatsSchedulerTest {

	private ApiCallStatsScheduler apiCallStatsScheduler;
	private ModulesService modulesService;
	private ApiCallService apiCallService;
	private ApiCallStatsService apiCallStatsService;

	@BeforeEach
	void setUp() {
		modulesService = mock(ModulesService.class);
		apiCallService = mock(ApiCallService.class);
		apiCallStatsService = mock(ApiCallStatsService.class);
		apiCallStatsScheduler = new ApiCallStatsScheduler(modulesService, apiCallStatsService, apiCallService);
	}

	@Test
	@DisplayName("test scheduler config")
	void test1() {
		apiCallStatsScheduler.schedule();
		assertEquals("ApiCallStatsScheduler-scheduler", Thread.currentThread().getName());
		Method schedule = assertDoesNotThrow(() -> apiCallStatsScheduler.getClass().getDeclaredMethod("schedule"));
		Scheduled scheduleAnnotation = schedule.getAnnotation(Scheduled.class);
		assertEquals("0 0/5 * * * ?", scheduleAnnotation.cron());
		SchedulerLock schedulerLockAnnotation = schedule.getAnnotation(SchedulerLock.class);
		assertEquals("api_call_stats_scheduler", schedulerLockAnnotation.name());
		assertEquals("30m", schedulerLockAnnotation.lockAtMostFor());
		assertEquals("5s", schedulerLockAnnotation.lockAtLeastFor());
	}

	@Nested
	@DisplayName("Method schedule test")
	class ScheduleTest {

		private ArrayList<ModulesDto> modulesList;
		private ApiCallStatsDto existsApiCallStatsDto;
		private ApiCallStatsDto newApiCallStatsDto;
		private Logger log;

		@BeforeEach
		void setUp() {
			modulesList = new ArrayList<ModulesDto>() {{
				add(new ModulesDto() {{
					setId(new ObjectId());
					setUserId("test");
					setIsDeleted(false);
				}});
				add(new ModulesDto() {{
					setId(new ObjectId());
					setUserId("test");
					setIsDeleted(true);
				}});
			}};
			existsApiCallStatsDto = new ApiCallStatsDto() {{
				setId(new ObjectId());
				setCallTotalCount(100L);
				setTransferDataTotalBytes(100L);
				setCallAlarmTotalCount(5L);
				setResponseDataRowTotalCount(150L);
				setTotalResponseTime(30000L);
				setMaxResponseTime(2000L);
				setClientIds(new HashSet<String>() {{
					add("client1");
				}});
				setLastApiCallId(new ObjectId().toString());
				setModuleId(modulesList.get(0).getId().toString());
				setCreateAt(new Date());
			}};
			newApiCallStatsDto = new ApiCallStatsDto() {{
				setCallTotalCount(200L);
				setTransferDataTotalBytes(200L);
				setCallAlarmTotalCount(8L);
				setResponseDataRowTotalCount(250L);
				setTotalResponseTime(60000L);
				setMaxResponseTime(2000L);
				setClientIds(new HashSet<String>() {{
					add("client2");
					add("client3");
				}});
				setLastApiCallId(new ObjectId().toString());
				setModuleId(modulesList.get(0).getId().toString());
			}};
			log = (Logger) ReflectionTestUtils.getField(apiCallStatsScheduler, "log");
			log = spy(log);
			doReturn(true).when(log).isDebugEnabled();
		}

		@Test
		@DisplayName("test schedule main process")
		void test1() {
			when(modulesService.findAll(any(Query.class))).thenAnswer(invocationOnMock -> {
				Query query = invocationOnMock.getArgument(0);
				Document fieldsObject = query.getFieldsObject();
				assertEquals("{\"is_deleted\": 1, \"user_id\": 1, \"id\": 1}", fieldsObject.toJson());
				assertTrue(query.getQueryObject().isEmpty());
				return modulesList;
			});
			when(apiCallStatsService.findOne(any(Query.class))).thenAnswer(invocationOnMock -> {
				Query query = invocationOnMock.getArgument(0);
				assertEquals("{\"moduleId\": \"" + modulesList.get(0).getId().toString() + "\"}", query.getQueryObject().toJson());
				assertEquals(1, query.getLimit());
				assertTrue(query.getFieldsObject().isEmpty());
				return existsApiCallStatsDto;
			});
			when(apiCallService.aggregateByAllPathId(anyString(), anyString())).thenAnswer(invocationOnMock -> {
				assertEquals(modulesList.get(0).getId().toString(), invocationOnMock.getArgument(0));
				assertEquals(existsApiCallStatsDto.getLastApiCallId(), invocationOnMock.getArgument(1));
				return newApiCallStatsDto;
			});
			doCallRealMethod().when(apiCallStatsService).merge(any(ApiCallStatsDto.class), any(ApiCallStatsDto.class));
			when(apiCallStatsService.upsert(any(Query.class), any(ApiCallStatsDto.class))).thenAnswer(invocationOnMock -> {
				Query query = invocationOnMock.getArgument(0);
				assertEquals("{\"moduleId\": \"" + modulesList.get(0).getId().toString() + "\"}", query.getQueryObject().toJson());
				ApiCallStatsDto actual = invocationOnMock.getArgument(1);
				assertEquals(modulesList.get(0).getId().toString(), actual.getModuleId());
				assertEquals(300L, actual.getCallTotalCount());
				assertEquals(300L, actual.getTransferDataTotalBytes());
				assertEquals(13L, actual.getCallAlarmTotalCount());
				assertEquals(400L, actual.getResponseDataRowTotalCount());
				assertEquals(90000L, actual.getTotalResponseTime());
				assertEquals(3, actual.getClientIds().size());
				assertTrue(actual.getClientIds().contains("client1"));
				assertTrue(actual.getClientIds().contains("client2"));
				assertTrue(actual.getClientIds().contains("client3"));
				assertEquals(0.0433D, actual.getAccessFailureRate());
				assertNotNull(actual.getLastApiCallId());
				assertEquals(existsApiCallStatsDto.getCreateAt(), actual.getCreateAt());
				assertEquals(modulesList.get(0).getUserId(), actual.getUserId());
				return null;
			});

			apiCallStatsScheduler.schedule();

			verify(apiCallStatsService).deleteAllByModuleId(modulesList.get(1).getId().toString());
		}

		@Test
		@DisplayName("test no exists api call stats")
		void test2() {
			when(modulesService.findAll(any(Query.class))).thenAnswer(invocationOnMock -> {
				Query query = invocationOnMock.getArgument(0);
				Document fieldsObject = query.getFieldsObject();
				assertEquals("{\"is_deleted\": 1, \"user_id\": 1, \"id\": 1}", fieldsObject.toJson());
				assertTrue(query.getQueryObject().isEmpty());
				return modulesList;
			});
			when(apiCallStatsService.findOne(any(Query.class))).thenAnswer(invocationOnMock -> {
				Query query = invocationOnMock.getArgument(0);
				assertEquals("{\"moduleId\": \"" + modulesList.get(0).getId().toString() + "\"}", query.getQueryObject().toJson());
				assertEquals(1, query.getLimit());
				assertTrue(query.getFieldsObject().isEmpty());
				return null;
			});
			when(apiCallService.aggregateByAllPathId(anyString(), any())).thenAnswer(invocationOnMock -> {
				assertEquals(modulesList.get(0).getId().toString(), invocationOnMock.getArgument(0));
				assertNull(invocationOnMock.getArgument(1));
				return newApiCallStatsDto;
			});
			doCallRealMethod().when(apiCallStatsService).merge(any(), any(ApiCallStatsDto.class));
			when(apiCallStatsService.upsert(any(Query.class), any(ApiCallStatsDto.class))).thenAnswer(invocationOnMock -> {
				Query query = invocationOnMock.getArgument(0);
				assertEquals("{\"moduleId\": \"" + modulesList.get(0).getId().toString() + "\"}", query.getQueryObject().toJson());
				ApiCallStatsDto actual = invocationOnMock.getArgument(1);
				assertEquals(modulesList.get(0).getId().toString(), actual.getModuleId());
				assertEquals(200L, actual.getCallTotalCount());
				assertEquals(200L, actual.getTransferDataTotalBytes());
				assertEquals(8L, actual.getCallAlarmTotalCount());
				assertEquals(250L, actual.getResponseDataRowTotalCount());
				assertEquals(60000L, actual.getTotalResponseTime());
				assertEquals(2, actual.getClientIds().size());
				assertTrue(actual.getClientIds().contains("client2"));
				assertTrue(actual.getClientIds().contains("client3"));
				assertEquals(0.04D, actual.getAccessFailureRate());
				assertNotNull(actual.getLastApiCallId());
				assertNotNull(actual.getCreateAt());
				assertEquals(modulesList.get(0).getUserId(), actual.getUserId());
				return null;
			});
			when(apiCallStatsService.isEmpty()).thenReturn(true);

			apiCallStatsScheduler.schedule();

			verify(apiCallStatsService).deleteAllByModuleId(modulesList.get(1).getId().toString());
		}

		@Test
		@DisplayName("test no modules")
		void test3() {
			when(modulesService.findAll(any(Query.class))).thenAnswer(invocationOnMock -> {
				Query query = invocationOnMock.getArgument(0);
				Document fieldsObject = query.getFieldsObject();
				assertEquals("{\"is_deleted\": 1, \"user_id\": 1, \"id\": 1}", fieldsObject.toJson());
				assertTrue(query.getQueryObject().isEmpty());
				return null;
			});

			apiCallStatsScheduler.schedule();

			verify(apiCallStatsService, never()).findOne(any(Query.class));
		}

		@Test
		@DisplayName("test aggregateByAllPathId error")
		void test4() {
			when(modulesService.findAll(any(Query.class))).thenReturn(modulesList);
			when(apiCallStatsService.findOne(any(Query.class))).thenReturn(existsApiCallStatsDto);
			RuntimeException runtimeException = new RuntimeException("test");
			when(apiCallService.aggregateByAllPathId(anyString(), anyString())).thenThrow(runtimeException);

			assertDoesNotThrow(() -> apiCallStatsScheduler.schedule());

			verify(apiCallStatsService, never()).merge(any(), any(ApiCallStatsDto.class));
		}

		@Test
		@DisplayName("test merge error")
		void test5() {
			when(modulesService.findAll(any(Query.class))).thenReturn(modulesList);
			when(apiCallStatsService.findOne(any(Query.class))).thenReturn(existsApiCallStatsDto);
			when(apiCallService.aggregateByAllPathId(anyString(), anyString())).thenReturn(newApiCallStatsDto);
			RuntimeException runtimeException = new RuntimeException("test");
			doThrow(runtimeException).when(apiCallStatsService).merge(any(), any(ApiCallStatsDto.class));

			assertDoesNotThrow(() -> apiCallStatsScheduler.schedule());

			verify(apiCallStatsService, never()).upsert(any(Query.class), any(ApiCallStatsDto.class));
		}

		@Test
		@DisplayName("test upsert error")
		void test6() {
			when(modulesService.findAll(any(Query.class))).thenReturn(modulesList);
			when(apiCallStatsService.findOne(any(Query.class))).thenReturn(existsApiCallStatsDto);
			when(apiCallService.aggregateByAllPathId(anyString(), anyString())).thenReturn(newApiCallStatsDto);
			doCallRealMethod().when(apiCallStatsService).merge(any(ApiCallStatsDto.class), any(ApiCallStatsDto.class));
			RuntimeException runtimeException = new RuntimeException("test");
			when(apiCallStatsService.upsert(any(Query.class), any(ApiCallStatsDto.class))).thenThrow(runtimeException);

			assertDoesNotThrow(() -> apiCallStatsScheduler.schedule());
		}
	}
}