package com.tapdata.tm.apiCalls.service;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.tapdata.tm.apiCalls.entity.ApiCallEntity;
import com.tapdata.tm.apicallminutestats.dto.ApiCallMinuteStatsDto;
import com.tapdata.tm.apicallminutestats.service.ApiCallMinuteStatsService;
import com.tapdata.tm.apicallstats.dto.ApiCallStatsDto;
import com.tapdata.tm.apicallstats.service.ApiCallStatsService;
import com.tapdata.tm.application.dto.ApplicationDto;
import com.tapdata.tm.application.service.ApplicationService;
import com.tapdata.tm.modules.service.ModulesService;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * @author samuel
 * @Description
 * @create 2024-09-03 14:36
 **/
@DisplayName("Class ApiCallService Test")
class ApiCallServiceTest {

	private ApiCallService apiCallService;
	private ApiCallMinuteStatsService apiCallMinuteStatsService;
	private MongoTemplate mongoTemplate;
	private ModulesService modulesService;
	private ApplicationService applicationService;
	private ApiCallStatsService apiCallStatsService;

	@BeforeEach
	void setUp() {
		apiCallService = new ApiCallService();
		apiCallMinuteStatsService = mock(ApiCallMinuteStatsService.class);
		ReflectionTestUtils.setField(apiCallService, "apiCallMinuteStatsService", apiCallMinuteStatsService);
		mongoTemplate = mock(MongoTemplate.class);
		ReflectionTestUtils.setField(apiCallService, "mongoOperations", mongoTemplate);
		modulesService = mock(ModulesService.class);
		ReflectionTestUtils.setField(apiCallService, "modulesService", modulesService);
		applicationService = mock(ApplicationService.class);
		ReflectionTestUtils.setField(apiCallService, "applicationService", applicationService);
		apiCallStatsService = mock(ApiCallStatsService.class);
		ReflectionTestUtils.setField(apiCallService, "apiCallStatsService", apiCallStatsService);
	}

	@Nested
	@DisplayName("Method aggregateByAllPathId test")
	class aggregateByAllPathIdTest {

		private String allPathId;
		private String lastApiCallId;

		@BeforeEach
		void setUp() {
			allPathId = new ObjectId().toString();
			lastApiCallId = new ObjectId().toString();
		}

		@Test
		@DisplayName("test main process")
		void test1() {
			MongoCollection<Document> mongoCollection = mock(MongoCollection.class);
			when(mongoTemplate.getCollection(anyString())).thenAnswer(invocationOnMock -> {
				assertEquals(ApiCallEntity.class.getAnnotation(org.springframework.data.mongodb.core.mapping.Document.class).value(), invocationOnMock.getArgument(0));
				return mongoCollection;
			});
			AggregateIterable<Document> aggregateIterable = mock(AggregateIterable.class);
			when(mongoCollection.aggregate(anyList(), any(Class.class))).thenAnswer(invocationOnMock -> {
				List<Document> pipeline = invocationOnMock.getArgument(0);
				assertEquals("{\"$match\": {\"allPathId\": \"" + allPathId + "\", \"_id\": {\"$gt\": {\"$oid\": \"" + lastApiCallId + "\"}}}}", pipeline.get(0).toJson());
				assertEquals("{\"$facet\": {\"callTotalCount\": [{\"$group\": {\"_id\": \"$allPathId\", \"data\": {\"$sum\": 1}}}], \"transferDataTotalBytes\": [{\"$group\": {\"_id\": \"$allPathId\", \"data\": {\"$sum\": \"$req_bytes\"}}}], \"callAlarmTotalCount\": [{\"$match\": {\"code\": {\"$ne\": \"200\"}}}, {\"$group\": {\"_id\": \"$allPathId\", \"data\": {\"$sum\": 1}}}], \"responseDataRowTotalCount\": [{\"$group\": {\"_id\": \"$allPathId\", \"data\": {\"$sum\": \"$res_rows\"}}}], \"totalResponseTime\": [{\"$group\": {\"_id\": \"$allPathId\", \"data\": {\"$sum\": \"$latency\"}}}], \"lastApiCallId\": [{\"$project\": {\"_id\": 1}}, {\"$sort\": {\"_id\": -1}}, {\"$limit\": 1}], \"maxResponseTime\": [{\"$group\": {\"_id\": \"$allPathId\", \"data\": {\"$max\": \"$latency\"}}}], \"clientIds\": [{\"$group\": {\"_id\": \"$user_info.clientId\"}}]}}", pipeline.get(1).toJson());
				assertEquals(Document.class, invocationOnMock.getArgument(1));
				return aggregateIterable;
			});
			when(aggregateIterable.allowDiskUse(anyBoolean())).thenAnswer(invocationOnMock -> {
				assertEquals(true, invocationOnMock.getArgument(0));
				return aggregateIterable;
			});
			MongoCursor<Document> mongoCursor = mock(MongoCursor.class);
			when(aggregateIterable.iterator()).thenReturn(mongoCursor);
			when(mongoCursor.hasNext()).thenReturn(true);
			Document aggregateResult = new Document()
					.append("callTotalCount", Arrays.asList(new Document("_id", allPathId).append("data", 132968)))
					.append("transferDataTotalBytes", Arrays.asList(new Document("_id", allPathId).append("data", 666876690L)))
					.append("callAlarmTotalCount", Arrays.asList(new Document("_id", allPathId).append("data", 61178)))
					.append("responseDataRowTotalCount", Arrays.asList(new Document("_id", allPathId).append("data", 56052629L)))
					.append("totalResponseTime", Arrays.asList(new Document("_id", allPathId).append("data", 53680329L)))
					.append("lastApiCallId", Arrays.asList(new Document("_id", new ObjectId("66d6b3b69b37425548a8017a"))))
					.append("maxResponseTime", Arrays.asList(new Document("_id", allPathId).append("data", 5119)))
					.append("clientIds", Arrays.asList(new Document("_id", "client1")));
			when(mongoCursor.next()).thenReturn(aggregateResult);

			ApiCallStatsDto apiCallStatsDto = apiCallService.aggregateByAllPathId(allPathId, lastApiCallId);
			assertEquals(allPathId, apiCallStatsDto.getModuleId());
			assertEquals(132968L, apiCallStatsDto.getCallTotalCount());
			assertEquals(666876690L, apiCallStatsDto.getTransferDataTotalBytes());
			assertEquals(61178L, apiCallStatsDto.getCallAlarmTotalCount());
			assertEquals(56052629L, apiCallStatsDto.getResponseDataRowTotalCount());
			assertEquals(53680329L, apiCallStatsDto.getTotalResponseTime());
			assertEquals(1, apiCallStatsDto.getClientIds().size());
			assertEquals("client1", apiCallStatsDto.getClientIds().iterator().next());
			assertEquals("66d6b3b69b37425548a8017a", apiCallStatsDto.getLastApiCallId());
			assertEquals(5119L, apiCallStatsDto.getMaxResponseTime());
		}
	}

	@Nested
	@DisplayName("Method aggregateMinuteByAllPathId test")
	class aggregateMinuteByAllPathIdTest {
		private String allPathId;
		private String lastApiCallId;
		private Date startTime;

		@BeforeEach
		void setUp() {
			allPathId = new ObjectId().toString();
			lastApiCallId = new ObjectId().toString();
			startTime = Date.from(ZonedDateTime.of(LocalDateTime.now().withNano(0), ZoneId.systemDefault()).toInstant());
		}

		@Test
		@DisplayName("test main process")
		void test1() {
			MongoCollection<Document> mongoCollection = mock(MongoCollection.class);
			when(mongoTemplate.getCollection(anyString())).thenAnswer(invocationOnMock -> {
				assertEquals(ApiCallEntity.class.getAnnotation(org.springframework.data.mongodb.core.mapping.Document.class).value(), invocationOnMock.getArgument(0));
				return mongoCollection;
			});
			AggregateIterable<Document> aggregateIterable = mock(AggregateIterable.class);
			when(mongoCollection.aggregate(anyList(), any(Class.class))).thenAnswer(invocationOnMock -> {
				List<Document> pipeline = invocationOnMock.getArgument(0);
				System.out.println(pipeline.get(1).toJson());
				assertEquals("{\"$match\": {\"allPathId\": \"" + allPathId + "\", \"_id\": {\"$gt\": {\"$oid\": \"" + lastApiCallId + "\"}}, \"createTime\": {\"$gte\": {\"$date\": \"" + startTime.toInstant().toString() + "\"}}}}", pipeline.get(0).toJson());
				assertEquals("{\"$project\": {\"year\": {\"$year\": \"$createTime\"}, \"month\": {\"$month\": \"$createTime\"}, \"day\": {\"$dayOfMonth\": \"$createTime\"}, \"hour\": {\"$hour\": \"$createTime\"}, \"minute\": {\"$minute\": \"$createTime\"}, \"res_rows\": 1, \"latency\": 1, \"req_bytes\": 1}}", pipeline.get(1).toJson());
				assertEquals("{\"$group\": {\"_id\": {\"year\": \"$year\", \"month\": \"$month\", \"day\": \"$day\", \"hour\": \"$hour\", \"minute\": \"$minute\"}, \"responseDataRowTotalCount\": {\"$sum\": \"$res_rows\"}, \"totalResponseTime\": {\"$sum\": \"$latency\"}, \"transferDataTotalBytes\": {\"$sum\": \"$req_bytes\"}, \"lastApiCallId\": {\"$last\": \"$_id\"}}}", pipeline.get(2).toJson());
				assertEquals(Document.class, invocationOnMock.getArgument(1));
				return aggregateIterable;
			});
			when(aggregateIterable.allowDiskUse(anyBoolean())).thenAnswer(invocationOnMock -> {
				assertEquals(true, invocationOnMock.getArgument(0));
				return aggregateIterable;
			});
			MongoCursor<Document> mongoCursor = mock(MongoCursor.class);
			when(aggregateIterable.iterator()).thenReturn(mongoCursor);
			List<Document> documents = new ArrayList<>();
			documents.add(new Document("_id", new Document("year", 2024).append("month", 9).append("day", 3).append("hour", 6).append("minute", 58))
					.append("responseDataRowTotalCount", 111078)
					.append("totalResponseTime", 828882)
					.append("transferDataTotalBytes", 159714)
					.append("lastApiCallId", new ObjectId("66d6b3b39b37425548a7fb55")));
			documents.add(new Document("_id", new Document("year", 2024).append("month", 9).append("day", 3).append("hour", 6).append("minute", 59))
					.append("responseDataRowTotalCount", 127664)
					.append("totalResponseTime", 636568)
					.append("transferDataTotalBytes", 225802)
					.append("lastApiCallId", new ObjectId("66d6b3b69b37425548a8017a")));
			AtomicInteger countDown = new AtomicInteger();
			AtomicInteger countUp = new AtomicInteger();
			when(mongoCursor.hasNext()).thenAnswer(invocationOnMock -> countDown.getAndIncrement() <= 1);
			when(mongoCursor.next()).thenAnswer(invocationOnMock -> documents.get(countUp.getAndIncrement()));

			List<ApiCallMinuteStatsDto> apiCallMinuteStatsDtoList = apiCallService.aggregateMinuteByAllPathId(allPathId, lastApiCallId, startTime);

			assertEquals(allPathId, apiCallMinuteStatsDtoList.get(0).getModuleId());
			assertEquals(111078L, apiCallMinuteStatsDtoList.get(0).getResponseDataRowTotalCount());
			assertEquals(828882L, apiCallMinuteStatsDtoList.get(0).getTotalResponseTime());
			assertEquals(159714L, apiCallMinuteStatsDtoList.get(0).getTransferDataTotalBytes());
			assertEquals("66d6b3b39b37425548a7fb55", apiCallMinuteStatsDtoList.get(0).getLastApiCallId());
			Date apiCallTime = apiCallMinuteStatsDtoList.get(0).getApiCallTime();
			LocalDateTime localDateTime = LocalDateTime.ofInstant(apiCallTime.toInstant(), ZoneId.of("UTC"));
			assertEquals(2024, localDateTime.getYear());
			assertEquals(9, localDateTime.getMonthValue());
			assertEquals(3, localDateTime.getDayOfMonth());
			assertEquals(6, localDateTime.getHour());
			assertEquals(58, localDateTime.getMinute());
			assertEquals(allPathId, apiCallMinuteStatsDtoList.get(1).getModuleId());
			assertEquals(127664L, apiCallMinuteStatsDtoList.get(1).getResponseDataRowTotalCount());
			assertEquals(636568L, apiCallMinuteStatsDtoList.get(1).getTotalResponseTime());
			assertEquals(225802L, apiCallMinuteStatsDtoList.get(1).getTransferDataTotalBytes());
			assertEquals("66d6b3b69b37425548a8017a", apiCallMinuteStatsDtoList.get(1).getLastApiCallId());
			apiCallTime = apiCallMinuteStatsDtoList.get(1).getApiCallTime();
			localDateTime = LocalDateTime.ofInstant(apiCallTime.toInstant(), ZoneId.of("UTC"));
			assertEquals(2024, localDateTime.getYear());
			assertEquals(9, localDateTime.getMonthValue());
			assertEquals(3, localDateTime.getDayOfMonth());
			assertEquals(6, localDateTime.getHour());
			assertEquals(59, localDateTime.getMinute());
		}
	}

	@Nested
	@DisplayName("Method findClients test")
	class findClientsTest {
		@Test
		@DisplayName("test main process")
		void test1() {
			List<String> moduleIdList = new ArrayList<String>() {{
				add(new ObjectId().toString());
				add(new ObjectId().toString());
			}};
			List<ApiCallStatsDto> apiCallStatsDtoList = new ArrayList<>();
			apiCallStatsDtoList.add(new ApiCallStatsDto() {{
				setClientIds(new HashSet<String>() {{
					add(new ObjectId().toString());
				}});
			}});
			apiCallStatsDtoList.add(new ApiCallStatsDto() {{
				setClientIds(new HashSet<String>() {{
					add(new ObjectId().toString());
				}});
			}});
			when(apiCallStatsService.findAll(any(Query.class))).thenAnswer(invocationOnMock -> {
				Query query = invocationOnMock.getArgument(0);
				assertEquals("{\"moduleId\": {\"$in\": [\"" + moduleIdList.get(0) + "\", \"" + moduleIdList.get(1) + "\"]}}", query.getQueryObject().toJson());
				assertEquals("{\"clientIds\": 1}", query.getFieldsObject().toJson());
				return apiCallStatsDtoList;
			});
			List<ApplicationDto> applicationDtoList = new ArrayList<>();
			applicationDtoList.add(new ApplicationDto() {{
				setId(new ObjectId(apiCallStatsDtoList.get(0).getClientIds().iterator().next()));
				setName("app1");
			}});
			applicationDtoList.add(new ApplicationDto() {{
				setId(new ObjectId(apiCallStatsDtoList.get(1).getClientIds().iterator().next()));
				setName("app2");
			}});
			when(applicationService.findByIds(anyList())).thenAnswer(invocationOnMock -> {
				List<String> ids = invocationOnMock.getArgument(0);
				assertEquals(2, ids.size());
				assertTrue(ids.contains(apiCallStatsDtoList.get(0).getClientIds().iterator().next()));
				assertTrue(ids.contains(apiCallStatsDtoList.get(1).getClientIds().iterator().next()));
				return applicationDtoList;
			});

			List<Map<String, String>> clients = apiCallService.findClients(moduleIdList);
			Map<String, String> client = clients.get(0);
			assertEquals("app1", client.get("name"));
			assertEquals(applicationDtoList.get(0).getId().toString(), client.get("id"));
			client = clients.get(1);
			assertEquals("app2", client.get("name"));
			assertEquals(applicationDtoList.get(1).getId().toString(), client.get("id"));
		}

		@Test
		@DisplayName("test input module id list is empty")
		void test2() {
			List<String> moduleIdList = new ArrayList<>();
			List<ApiCallStatsDto> apiCallStatsDtoList = new ArrayList<>();
			apiCallStatsDtoList.add(new ApiCallStatsDto() {{
				setClientIds(new HashSet<String>() {{
					add(new ObjectId().toString());
				}});
			}});
			apiCallStatsDtoList.add(new ApiCallStatsDto() {{
				setClientIds(new HashSet<String>() {{
					add(new ObjectId().toString());
				}});
			}});
			when(apiCallStatsService.findAll(any(Query.class))).thenAnswer(invocationOnMock -> {
				Query query = invocationOnMock.getArgument(0);
				assertTrue(query.getQueryObject().isEmpty());
				assertEquals("{\"clientIds\": 1}", query.getFieldsObject().toJson());
				return apiCallStatsDtoList;
			});
			List<ApplicationDto> applicationDtoList = new ArrayList<>();
			applicationDtoList.add(new ApplicationDto() {{
				setId(new ObjectId(apiCallStatsDtoList.get(0).getClientIds().iterator().next()));
				setName("app1");
			}});
			applicationDtoList.add(new ApplicationDto() {{
				setId(new ObjectId(apiCallStatsDtoList.get(1).getClientIds().iterator().next()));
				setName("app2");
			}});
			when(applicationService.findByIds(anyList())).thenAnswer(invocationOnMock -> {
				List<String> ids = invocationOnMock.getArgument(0);
				assertEquals(2, ids.size());
				assertTrue(ids.contains(apiCallStatsDtoList.get(0).getClientIds().iterator().next()));
				assertTrue(ids.contains(apiCallStatsDtoList.get(1).getClientIds().iterator().next()));
				return applicationDtoList;
			});

			List<Map<String, String>> clients = apiCallService.findClients(moduleIdList);
			Map<String, String> client = clients.get(0);
			assertEquals("app1", client.get("name"));
			assertEquals(applicationDtoList.get(0).getId().toString(), client.get("id"));
			client = clients.get(1);
			assertEquals("app2", client.get("name"));
			assertEquals(applicationDtoList.get(1).getId().toString(), client.get("id"));
		}
	}

	@Test
	@DisplayName("Method findOne(Query) test")
	void testFindOne() {
		Query inputQuery = Query.query(Criteria.where("_id").is(1));
		ApiCallEntity apiCallEntity = new ApiCallEntity();
		when(mongoTemplate.findOne(any(Query.class), any(Class.class))).thenAnswer(invocationOnMock -> {
			Query query = invocationOnMock.getArgument(0);
			assertSame(query, inputQuery);
			assertEquals(ApiCallEntity.class, invocationOnMock.getArgument(1));
			return apiCallEntity;
		});

		assertSame(apiCallEntity, apiCallService.findOne(inputQuery));
	}
}