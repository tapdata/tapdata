package com.tapdata.tm.apicallminutestats.service;

import com.tapdata.tm.apicallminutestats.dto.ApiCallMinuteStatsDto;
import com.tapdata.tm.apicallminutestats.entity.ApiCallMinuteStatsEntity;
import com.tapdata.tm.apicallminutestats.repository.ApiCallMinuteStatsRepository;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * @author samuel
 * @Description
 * @create 2024-09-03 18:59
 **/
@DisplayName("Class ApiCallMinuteStatsService Test")
class ApiCallMinuteStatsServiceTest {

	private ApiCallMinuteStatsRepository repository;
	private ApiCallMinuteStatsService apiCallMinuteStatsService;

	@BeforeEach
	void setUp() {
		repository = mock(ApiCallMinuteStatsRepository.class);
		apiCallMinuteStatsService = new ApiCallMinuteStatsService(repository);
		apiCallMinuteStatsService = spy(apiCallMinuteStatsService);
	}

	@Nested
	@DisplayName("Method merge test")
	class mergeTest {
		@Test
		@DisplayName("test main process")
		void test1() {
			Instant apiCallTime = ZonedDateTime.now().withSecond(0).withNano(0).toInstant();
			List<ApiCallMinuteStatsDto> apiCallMinuteStatsDtoList = new ArrayList<>();
			apiCallMinuteStatsDtoList.add(new ApiCallMinuteStatsDto() {{
				setModuleId("1");
				setApiCallTime(Date.from(apiCallTime));
				setTotalResponseTime(1800L);
				setResponseDataRowTotalCount(11L);
				setTransferDataTotalBytes(134L);
			}});
			apiCallMinuteStatsDtoList.add(new ApiCallMinuteStatsDto() {{
				setModuleId("2");
				setApiCallTime(Date.from(apiCallTime.plusSeconds(60L)));
				setTotalResponseTime(1000L);
				setResponseDataRowTotalCount(10L);
				setTransferDataTotalBytes(100L);
			}});
			ApiCallMinuteStatsDto existsApiCallMinuteStats = new ApiCallMinuteStatsDto() {{
				setId(new ObjectId());
				setCreateAt(new Date());
				setModuleId("1");
				setApiCallTime(Date.from(apiCallTime));
				setTotalResponseTime(1500L);
				setResponseDataRowTotalCount(16L);
				setTransferDataTotalBytes(1234L);
			}};
			doAnswer(invocationOnMock -> {
				Query query = invocationOnMock.getArgument(0);
				assertEquals("{\"moduleId\": \"1\", \"apiCallTime\": {\"$date\": \"" + apiCallTime + "\"}}", query.getQueryObject().toJson());
				return existsApiCallMinuteStats;
			}).when(apiCallMinuteStatsService).findOne(any(Query.class));

			apiCallMinuteStatsService.merge(apiCallMinuteStatsDtoList);

			assertEquals("1", apiCallMinuteStatsDtoList.get(0).getModuleId());
			assertEquals(27L, apiCallMinuteStatsDtoList.get(0).getResponseDataRowTotalCount());
			assertEquals(3300L, apiCallMinuteStatsDtoList.get(0).getTotalResponseTime());
			assertEquals(1368L, apiCallMinuteStatsDtoList.get(0).getTransferDataTotalBytes());
			assertEquals(122.22D, apiCallMinuteStatsDtoList.get(0).getResponseTimePerRow());
			assertEquals(410D, apiCallMinuteStatsDtoList.get(0).getTransferBytePerSecond());
			assertEquals(existsApiCallMinuteStats.getId(), apiCallMinuteStatsDtoList.get(0).getId());
			assertEquals(existsApiCallMinuteStats.getCreateAt(), apiCallMinuteStatsDtoList.get(0).getCreateAt());
			assertEquals("2", apiCallMinuteStatsDtoList.get(1).getModuleId());
			assertEquals(10L, apiCallMinuteStatsDtoList.get(1).getResponseDataRowTotalCount());
			assertEquals(1000L, apiCallMinuteStatsDtoList.get(1).getTotalResponseTime());
			assertEquals(100L, apiCallMinuteStatsDtoList.get(1).getTransferDataTotalBytes());
			assertEquals(0D, apiCallMinuteStatsDtoList.get(1).getResponseTimePerRow());
			assertEquals(0D, apiCallMinuteStatsDtoList.get(1).getTransferBytePerSecond());
		}

		@Test
		@DisplayName("test when apiCallMinuteStatsDtoList is null or empty list")
		void test2() {
			assertDoesNotThrow(() -> apiCallMinuteStatsService.merge(null));
			assertDoesNotThrow(() -> apiCallMinuteStatsService.merge(new ArrayList<>()));
		}
	}

	@Nested
	@DisplayName("Method bulkWrite test")
	class bulkWriteTest {
		@Test
		@DisplayName("test main process")
		void test1() {
			List<ApiCallMinuteStatsDto> apiCallMinuteStatsDtoList = new ArrayList<>();
			apiCallMinuteStatsDtoList.add(new ApiCallMinuteStatsDto() {{
				setId(new ObjectId());
			}});
			BulkOperations bulkOperations = mock(BulkOperations.class);
			when(repository.bulkOperations(any(BulkOperations.BulkMode.class))).thenAnswer(invocationOnMock -> {
				BulkOperations.BulkMode bulkMode = invocationOnMock.getArgument(0);
				assertEquals(BulkOperations.BulkMode.UNORDERED, bulkMode);
				return bulkOperations;
			});
			when(bulkOperations.upsert(any(Query.class), any(Update.class))).thenAnswer(invocationOnMock -> {
				Query query = invocationOnMock.getArgument(0);
				Update update = invocationOnMock.getArgument(1);
				assertEquals("{\"id\": {\"$oid\": \"" + apiCallMinuteStatsDtoList.get(0).getId() + "\"}}", query.getQueryObject().toJson());
				assertEquals("{\"$set\": {\"test\": \"test\"}}", update.getUpdateObject().toJson());
				return null;
			});
			when(repository.buildUpdateSet(any(ApiCallMinuteStatsEntity.class))).thenReturn(Update.update("test", "test"));

			apiCallMinuteStatsService.bulkWrite(apiCallMinuteStatsDtoList);

			verify(bulkOperations).execute();
		}

		@Test
		@DisplayName("test when apiCallMinuteStatsDtoList is null or empty list")
		void test2() {
			BulkOperations bulkOperations = mock(BulkOperations.class);
			when(repository.bulkOperations(any())).thenReturn(bulkOperations);
			assertDoesNotThrow(() -> apiCallMinuteStatsService.bulkWrite(null));
			assertDoesNotThrow(() -> apiCallMinuteStatsService.bulkWrite(new ArrayList<>()));
			verify(bulkOperations, never()).upsert(any(Query.class), any(Update.class));
			verify(bulkOperations, never()).execute();
		}
	}

	@Nested
	@DisplayName("Method isEmpty test")
	class isEmptyTest {
		@Test
		@DisplayName("test main process")
		void test1() {
			when(repository.findOne(any(Query.class))).thenAnswer(invocationOnMock -> {
				Query query = invocationOnMock.getArgument(0);
				assertEquals("{\"_id\": 1}", query.getFieldsObject().toJson());
				assertEquals(1, query.getLimit());
				return Optional.of(new ApiCallMinuteStatsEntity());
			});
			assertFalse(apiCallMinuteStatsService.isEmpty());
		}

		@Test
		@DisplayName("test when findOne return null")
		void test2() {
			when(repository.findOne(any(Query.class))).thenAnswer(invocationOnMock -> {
				Query query = invocationOnMock.getArgument(0);
				assertEquals("{\"_id\": 1}", query.getFieldsObject().toJson());
				assertEquals(1, query.getLimit());
				return Optional.empty();
			});
			assertTrue(apiCallMinuteStatsService.isEmpty());
		}
	}
}