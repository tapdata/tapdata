package com.tapdata.tm.apicallstats.service;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.tapdata.tm.apicallminutestats.entity.ApiCallMinuteStatsEntity;
import com.tapdata.tm.apicallstats.dto.ApiCallStatsDto;
import com.tapdata.tm.apicallstats.entity.ApiCallStatsEntity;
import com.tapdata.tm.apicallstats.repository.ApiCallStatsRepository;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * @author samuel
 * @Description
 * @create 2024-09-04 10:44
 **/
@DisplayName("Class ApiCallStatsService Test")
class ApiCallStatsServiceTest {

	private ApiCallStatsRepository apiCallStatsRepository;
	private ApiCallStatsService apiCallStatsService;

	@BeforeEach
	void setUp() {
		apiCallStatsRepository = mock(ApiCallStatsRepository.class);
		apiCallStatsService = new ApiCallStatsService(apiCallStatsRepository);
	}

	@Nested
	@DisplayName("Method merge test")
	class mergeTest {

		private ApiCallStatsDto oldStats;
		private ApiCallStatsDto newStats;
		private ObjectId oldId;

		@BeforeEach
		void setUp() {
			oldId = new ObjectId();
			oldStats = new ApiCallStatsDto() {{
				setId(oldId);
				setCallTotalCount(100L);
				setTransferDataTotalBytes(100L);
				setCallAlarmTotalCount(10L);
				setResponseDataRowTotalCount(10L);
				setTotalResponseTime(1000L);
				setClientIds(new HashSet<String>() {{
					add("client1");
				}});
				setCreateAt(new Date());
			}};
			newStats = new ApiCallStatsDto() {{
				setCallTotalCount(100L);
				setTransferDataTotalBytes(100L);
				setCallAlarmTotalCount(10L);
				setResponseDataRowTotalCount(10L);
				setTotalResponseTime(1000L);
				setClientIds(new HashSet<String>() {{
					add("client1");
					add("client2");
				}});
				setCreateAt(new Date());
			}};
		}

		@Test
		@DisplayName("test new stats merge old stats")
		void test1() {
			apiCallStatsService.merge(oldStats, newStats);

			assertEquals(oldId, newStats.getId());
			assertEquals(200L, newStats.getCallTotalCount());
			assertEquals(200L, newStats.getTransferDataTotalBytes());
			assertEquals(20L, newStats.getCallAlarmTotalCount());
			assertEquals(20L, newStats.getResponseDataRowTotalCount());
			assertEquals(2000L, newStats.getTotalResponseTime());
			assertEquals(2, newStats.getClientIds().size());
			assertTrue(newStats.getClientIds().contains("client1"));
			assertTrue(newStats.getClientIds().contains("client2"));
			assertEquals(oldStats.getCreateAt(), newStats.getCreateAt());
		}

		@Test
		@DisplayName("test old stats is null")
		void test2() {
			apiCallStatsService.merge(null, newStats);

			assertNotNull(newStats.getId());
			assertEquals(100L, newStats.getCallTotalCount());
			assertEquals(100L, newStats.getTransferDataTotalBytes());
			assertEquals(10L, newStats.getCallAlarmTotalCount());
			assertEquals(10L, newStats.getResponseDataRowTotalCount());
			assertEquals(1000L, newStats.getTotalResponseTime());
			assertEquals(2, newStats.getClientIds().size());
			assertTrue(newStats.getClientIds().contains("client1"));
			assertTrue(newStats.getClientIds().contains("client2"));
			assertNotNull(newStats.getCreateAt());
		}

		@Test
		@DisplayName("test input two parameters all null")
		void test3() {
			assertDoesNotThrow(() -> apiCallStatsService.merge(null, null));
		}
	}

	@Nested
	@DisplayName("Method aggregateByUserId test")
	class aggregateByUserIdTest {
		@BeforeEach
		void setUp() {

		}

		@Test
		@DisplayName("test main process")
		void test1() {
			String userId = new ObjectId().toString();
			MongoCollection<Document> mongoCollection = mock(MongoCollection.class);
			MongoTemplate mongoTemplate = mock(MongoTemplate.class);
			when(mongoTemplate.getCollection(anyString())).thenAnswer(invocationOnMock -> {
				assertEquals(ApiCallStatsEntity.class.getAnnotation(org.springframework.data.mongodb.core.mapping.Document.class).value(), invocationOnMock.getArgument(0));
				return mongoCollection;
			});
			when(apiCallStatsRepository.getMongoOperations()).thenReturn(mongoTemplate);
			MongoCursor<Document> mongoCursor = mock(MongoCursor.class);
			AggregateIterable<Document> aggregateIterable = mock(AggregateIterable.class);
			when(aggregateIterable.iterator()).thenReturn(mongoCursor);
			when(mongoCollection.aggregate(anyList())).thenAnswer(invocationOnMock -> {
				List<Document> pipeline = invocationOnMock.getArgument(0);
				assertEquals("{\"$match\": {\"user_id\": \"" + userId + "\"}}", pipeline.get(0).toJson());
				assertEquals("{\"$facet\": {\"callTotalCount\": [{\"$group\": {\"_id\": null, \"data\": {\"$sum\": \"$callTotalCount\"}}}], \"transferDataTotalBytes\": [{\"$group\": {\"_id\": null, \"data\": {\"$sum\": \"$transferDataTotalBytes\"}}}], \"callAlarmTotalCount\": [{\"$group\": {\"_id\": null, \"data\": {\"$sum\": \"$callAlarmTotalCount\"}}}], \"responseDataRowTotalCount\": [{\"$group\": {\"_id\": \"$allPathId\", \"data\": {\"$sum\": \"$responseDataRowTotalCount\"}}}], \"totalResponseTime\": [{\"$group\": {\"_id\": \"$allPathId\", \"data\": {\"$sum\": \"$totalResponseTime\"}}}], \"alarmApiTotalCount\": [{\"$match\": {\"accessFailureRate\": {\"$gt\": 0}}}, {\"$group\": {\"_id\": null, \"data\": {\"$sum\": 1}}}]}}", pipeline.get(1).toJson());
				return aggregateIterable;
			});
			when(aggregateIterable.allowDiskUse(anyBoolean())).thenAnswer(invocationOnMock -> {
				assertEquals(true, invocationOnMock.getArgument(0));
				return aggregateIterable;
			});
			when(mongoCursor.hasNext()).thenReturn(true);
			Document mockResult = new Document("callTotalCount", Arrays.asList(new Document("_id", null).append("data", 4226845L)))
					.append("transferDataTotalBytes", Arrays.asList(new Document("_id", null).append("data", 309468035240L)))
					.append("callAlarmTotalCount", Arrays.asList(new Document("_id", null).append("data", 1926396L)))
					.append("responseDataRowTotalCount", Arrays.asList(new Document("_id", null).append("data", 1887940806L)))
					.append("totalResponseTime", Arrays.asList(new Document("_id", null).append("data", 12986886132L)))
					.append("alarmApiTotalCount", Arrays.asList(new Document("_id", null).append("data", 32)));
			when(mongoCursor.next()).thenReturn(mockResult);

			ApiCallStatsDto apiCallStatsDto = apiCallStatsService.aggregateByUserId(userId);

			assertEquals(4226845L, apiCallStatsDto.getCallTotalCount());
			assertEquals(309468035240L, apiCallStatsDto.getTransferDataTotalBytes());
			assertEquals(1926396L, apiCallStatsDto.getCallAlarmTotalCount());
			assertEquals(1887940806L, apiCallStatsDto.getResponseDataRowTotalCount());
			assertEquals(12986886132L, apiCallStatsDto.getTotalResponseTime());
			assertEquals(32L, apiCallStatsDto.getAlarmApiTotalCount());
		}
	}

	@Nested
	@DisplayName("Method isEmpty test")
	class isEmptyTest {
		@Test
		@DisplayName("test main process")
		void test1() {
			when(apiCallStatsRepository.findOne(any(Query.class))).thenAnswer(invocationOnMock -> {
				Query query = invocationOnMock.getArgument(0);
				assertEquals("{\"_id\": 1}", query.getFieldsObject().toJson());
				assertEquals(1, query.getLimit());
				return Optional.of(new ApiCallMinuteStatsEntity());
			});
			assertFalse(apiCallStatsService.isEmpty());
		}

		@Test
		@DisplayName("test when findOne return null")
		void test2() {
			when(apiCallStatsRepository.findOne(any(Query.class))).thenAnswer(invocationOnMock -> {
				Query query = invocationOnMock.getArgument(0);
				assertEquals("{\"_id\": 1}", query.getFieldsObject().toJson());
				assertEquals(1, query.getLimit());
				return Optional.empty();
			});
			assertTrue(apiCallStatsService.isEmpty());
		}
	}

	@Nested
	@DisplayName("Method deleteAllByModuleId test")
	class deleteAllByModuleIdTest {
		@Test
		@DisplayName("test main process")
		void test1() {
			String moduleId = new ObjectId().toString();
			apiCallStatsService = spy(apiCallStatsService);
			doAnswer(invocationOnMock -> {
				Query query = invocationOnMock.getArgument(0);
				System.out.println(query.getQueryObject().toJson());
				return 1L;
			}).when(apiCallStatsService).deleteAll(any(Query.class));

			apiCallStatsService.deleteAllByModuleId(moduleId);
		}
	}
}