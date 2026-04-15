package com.tapdata.tm.apiCalls.service;

import com.alibaba.fastjson.JSON;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.tapdata.tm.apiCalls.dto.ApiCallDto;
import com.tapdata.tm.apiCalls.entity.ApiCallEntity;
import com.tapdata.tm.apiCalls.vo.ApiCallDataVo;
import com.tapdata.tm.apiCalls.vo.ApiCallDetailVo;
import com.tapdata.tm.apiCalls.vo.ApiPercentile;
import com.tapdata.tm.apiServer.service.check.RealTimeOfApiResponseSizeAlter;
import com.tapdata.tm.apicallminutestats.dto.ApiCallMinuteStatsDto;
import com.tapdata.tm.apicallminutestats.service.ApiCallMinuteStatsService;
import com.tapdata.tm.apicallstats.dto.ApiCallStatsDto;
import com.tapdata.tm.apicallstats.service.ApiCallStatsService;
import com.tapdata.tm.application.dto.ApplicationDto;
import com.tapdata.tm.application.service.ApplicationService;
import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.module.dto.ModulesDto;
import com.tapdata.tm.module.dto.Param;
import com.tapdata.tm.module.entity.Path;
import com.tapdata.tm.modules.entity.ModulesEntity;
import com.tapdata.tm.modules.service.ModulesService;
import com.tapdata.tm.system.api.dto.TextEncryptionRuleDto;
import com.tapdata.tm.system.api.service.TextEncryptionRuleService;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.never;

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
    TextEncryptionRuleService ruleService;
    private RealTimeOfApiResponseSizeAlter realTimeOfApiResponseSizeAlter;

    @BeforeEach
    void setUp() {
        ruleService = mock(TextEncryptionRuleService.class);
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
        ReflectionTestUtils.setField(apiCallService, "ruleService", ruleService);
        realTimeOfApiResponseSizeAlter = mock(RealTimeOfApiResponseSizeAlter.class);
        ReflectionTestUtils.setField(apiCallService, "realTimeOfApiResponseSizeAlter", realTimeOfApiResponseSizeAlter);
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
                assertEquals("{\"$match\": {\"allPathId\": \"" + allPathId + "\", \"supplement\": {\"$ne\": true}, \"_id\": {\"$gt\": {\"$oid\": \"" + lastApiCallId + "\"}}, \"createTime\": {\"$gte\": {\"$date\": \"" + startTime.toInstant().toString() + "\"}}}}", pipeline.get(0).toJson());
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
            assertNull(client.get("name"));
            assertEquals(applicationDtoList.get(0).getId().toString(), client.get("id"));
            client = clients.get(1);
            assertNull(client.get("name"));
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
            assertNull(client.get("name"));
            assertEquals(applicationDtoList.get(0).getId().toString(), client.get("id"));
            client = clients.get(1);
            assertNull(client.get("name"));
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

        assertEquals(apiCallEntity, apiCallService.findOne(inputQuery));
    }

    @Nested
    class startFilterApiNameOrIdTest {
        @Test
        void testNormal() {
            Filter filter = parseFilter("{\"order\":\"createTime DESC\",\"limit\":20,\"skip\":0,\"where\":{\"or\":[{\"name\": {\"$regex\":\"xxx\"}},{\"id\": {\"$regex\":\"xxx\"}}]}}");
            Criteria criteria = new Criteria();
            apiCallService.startFilterApiNameOrId(filter, criteria);
            Assertions.assertEquals("{\"criteriaObject\":{\"$or\":[{\"req_path\":\"xxx\"}]}}", JSON.toJSONString(criteria));
        }
        @Test
        void testQueryById() {
            Filter filter = parseFilter("{\"order\":\"createTime DESC\",\"limit\":20,\"skip\":0,\"where\":{\"or\":[{\"name\": {\"$regex\":\"xxx\"}},{\"id\": {\"$regex\":\"68a7e8decd50c74ff40731f4\"}}]}}");
            Criteria criteria = new Criteria();
            apiCallService.startFilterApiNameOrId(filter, criteria);
            Assertions.assertEquals("{\"criteriaObject\":{\"allPathId\":\"68a7e8decd50c74ff40731f4\"}}", JSON.toJSONString(criteria));
        }
        @Test
        void testEmpty() {
            Filter filter = parseFilter("{\"order\":\"createTime DESC\",\"limit\":20,\"skip\":0,\"where\":{\"or\":[]}}");
            Criteria criteria = new Criteria();
            apiCallService.startFilterApiNameOrId(filter, criteria);
            Assertions.assertEquals("{\"criteriaObject\":{}}", JSON.toJSONString(criteria));
        }
        @Test
        void testNull() {
            Filter filter = parseFilter("{\"order\":\"createTime DESC\",\"limit\":20,\"skip\":0,\"where\":{\"or\":[{\"name\": {\"$regex\":null}},{\"id\": {\"$regex\":null}}]}}");
            Criteria criteria = new Criteria();
            apiCallService.startFilterApiNameOrId(filter, criteria);
            Assertions.assertEquals("{\"criteriaObject\":{}}", JSON.toJSONString(criteria));
        }
        @Test
        void testApiNameIsNull() {
            Filter filter = parseFilter("{\"order\":\"createTime DESC\",\"limit\":20,\"skip\":0,\"where\":{\"or\":[{\"name\": {\"$regex\":null}},{\"id\": {\"$regex\":null}}]}}");
            Criteria criteria = new Criteria();
            apiCallService.startFilterApiNameOrId(filter, criteria);
            Assertions.assertEquals("{\"criteriaObject\":{}}", JSON.toJSONString(criteria));
        }
        @Test
        void testApiNameIsEmpty() {
            Filter filter = parseFilter("{\"order\":\"createTime DESC\",\"limit\":20,\"skip\":0,\"where\":{\"or\":[{\"name\": {\"$regex\":\"\"}},{\"id\": {\"$regex\":\"xxx\"}}]}}");
            Criteria criteria = new Criteria();
            apiCallService.startFilterApiNameOrId(filter, criteria);
            Assertions.assertEquals("{\"criteriaObject\":{}}", JSON.toJSONString(criteria));
        }
        @Test
        void testApiNameIsLikeButNotAnyApi() {
            when(modulesService.findAll(any(Query.class))).thenReturn(new ArrayList<>());
            Filter filter = parseFilter("{\"order\":\"createTime DESC\",\"limit\":20,\"skip\":0,\"where\":{\"or\":[{\"name\": {\"$regex\":\"xxx\"}},{\"id\": {\"$regex\":\"xxx\"}}]}}");
            Criteria criteria = new Criteria();
            apiCallService.startFilterApiNameOrId(filter, criteria);
            Assertions.assertEquals("{\"criteriaObject\":{\"$or\":[{\"req_path\":\"xxx\"}]}}", JSON.toJSONString(criteria));
        }
        @Test
        void testApiNameIsLikeWithOption() {
            ObjectId objectId = new ObjectId("68a7e8decd50c74ff40731f4");
            List<ModulesDto> all = new ArrayList<>();
            ModulesDto dto = new ModulesDto();
            dto.setId(objectId);
            all.add(dto);
            all.add(null);
            when(modulesService.findAll(any(Query.class))).thenReturn(all);
            Filter filter = parseFilter("{\"order\":\"createTime DESC\",\"limit\":20,\"skip\":0,\"where\":{\"or\":[{\"name\": {\"$regex\":\"xxx\"}},{\"id\": {\"$regex\":\"xxx\"}}]}}");
            Criteria criteria = new Criteria();
            apiCallService.startFilterApiNameOrId(filter, criteria);
            Assertions.assertEquals("{\"criteriaObject\":{\"$or\":[{\"allPathId\":{\"$in\":[\"68a7e8decd50c74ff40731f4\"]}}]}}", JSON.toJSONString(criteria));
        }
    }

    @Nested
    @DisplayName("Method Find(Query) test")
    class FindTest {
        @Test
        void testNormal() {
            UserDetail userDetail = mock(UserDetail.class);
            when(userDetail.getEmail()).thenReturn("admin@admin.com");
            when(userDetail.getUserId()).thenReturn("admin@admin.com");
            Filter filter = parseFilter("{\"order\":\"createTime DESC\",\"limit\":20,\"skip\":0,\"where\":{}}");

            List<ApiCallDataVo> result = new ArrayList<>();

            List<ApplicationDto> applications = new ArrayList<>();
            when(applicationService.findByIds(anyList())).thenReturn(applications);

            List<ModulesDto> modules = new ArrayList<>();
            when(modulesService.findAllModulesByIds(anyList())).thenReturn(modules);

            when(mongoTemplate.aggregate(any(Aggregation.class), any(String.class), any(Class.class))).thenAnswer((answer) -> {
                Class<?> className = answer.getArgument(2);
                if (Objects.equals(className.getSimpleName(), ApiCallDataVo.class.getSimpleName())) {
                    return new AggregationResults<>(result, new Document());
                } else if (Objects.equals(className.getSimpleName(), Map.class.getSimpleName())) {
                    Map<String, Number> res = new HashMap<>();
                    res.put("total", 0L);
                    return new AggregationResults<Map<String, Number>>(List.of(res), new Document());
                } else {
                    return null;
                }
            });
            Page<ApiCallDetailVo> page = apiCallService.findApiCallPage(filter);
            Assertions.assertEquals(page.getItems().size(), 0);
            Assertions.assertEquals(page.getTotal(), 0L);
        }
        @Test
        void testCountIsEmpty() {
            List<ApiCallDataVo> result = new ArrayList<>();
            UserDetail userDetail = mock(UserDetail.class);
            when(userDetail.getEmail()).thenReturn("admin@admin.com");
            when(userDetail.getUserId()).thenReturn("admin@admin.com");
            Filter filter = parseFilter("{\"order\":\"createTime DESC\",\"limit\":20,\"skip\":0,\"where\":{}}");
            List<ApplicationDto> applications = new ArrayList<>();
            when(applicationService.findByIds(anyList())).thenReturn(applications);

            List<ModulesDto> modules = new ArrayList<>();
            when(modulesService.findAllModulesByIds(anyList())).thenReturn(modules);
            when(mongoTemplate.aggregate(any(Aggregation.class), any(String.class), any(Class.class))).thenAnswer((answer) -> {
                Class<?> className = answer.getArgument(2);
                if (Objects.equals(className.getSimpleName(), ApiCallDataVo.class.getSimpleName())) {
                    return new AggregationResults<>(result, new Document());
                } else if (Objects.equals(className.getSimpleName(), Map.class.getSimpleName())) {
                    return new AggregationResults<Map<String, Number>>(List.of(), new Document());
                } else {
                    return null;
                }
            });
            Page<ApiCallDetailVo> page = apiCallService.findApiCallPage(filter);
            Assertions.assertEquals(page.getItems().size(), 0);
            Assertions.assertEquals(page.getTotal(), 0L);
        }

        @Test
        void testOrderIsNull() {
            List<ApiCallDataVo> result = new ArrayList<>();
            UserDetail userDetail = mock(UserDetail.class);
            when(userDetail.getEmail()).thenReturn("admin@admin.com");
            when(userDetail.getUserId()).thenReturn("admin@admin.com");
            Filter filter = parseFilter("{\"order\": null,\"limit\":20,\"skip\":0,\"where\":{}}");
            List<ApplicationDto> applications = new ArrayList<>();
            when(applicationService.findByIds(anyList())).thenReturn(applications);

            List<ModulesDto> modules = new ArrayList<>();
            when(modulesService.findAllModulesByIds(anyList())).thenReturn(modules);
            when(mongoTemplate.aggregate(any(Aggregation.class), any(String.class), any(Class.class))).thenAnswer((answer) -> {
                Class<?> className = answer.getArgument(2);
                if (Objects.equals(className.getSimpleName(), ApiCallDataVo.class.getSimpleName())) {
                    return new AggregationResults<>(result, new Document());
                } else if (Objects.equals(className.getSimpleName(), Map.class.getSimpleName())) {
                    Map<String, Number> res = new HashMap<>();
                    res.put("total", 0L);
                    return new AggregationResults<Map<String, Number>>(List.of(res), new Document());
                } else {
                    return null;
                }
            });
            Page<ApiCallDetailVo> page = apiCallService.findApiCallPage(filter);
            Assertions.assertEquals(page.getItems().size(), 0);
            Assertions.assertEquals(page.getTotal(), 0L);
        }

        @Test
        void testClientName() {
            List<ApiCallDataVo> result = new ArrayList<>();
            UserDetail userDetail = mock(UserDetail.class);
            when(userDetail.getEmail()).thenReturn("admin@admin.com");
            when(userDetail.getUserId()).thenReturn("admin@admin.com");
            Filter filter = parseFilter("{\"order\": null,\"limit\":20,\"skip\":0,\"where\":{\"clientId\": \"name\"}}");
            List<ApplicationDto> applications = new ArrayList<>();
            when(applicationService.findByIds(anyList())).thenReturn(applications);

            List<ModulesDto> modules = new ArrayList<>();
            when(modulesService.findAllModulesByIds(anyList())).thenReturn(modules);
            when(mongoTemplate.aggregate(any(Aggregation.class), any(String.class), any(Class.class))).thenAnswer((answer) -> {
                Class<?> className = answer.getArgument(2);
                if (Objects.equals(className.getSimpleName(), ApiCallDataVo.class.getSimpleName())) {
                    return new AggregationResults<>(result, new Document());
                } else if (Objects.equals(className.getSimpleName(), Map.class.getSimpleName())) {
                    Map<String, Number> res = new HashMap<>();
                    res.put("total", 0L);
                    return new AggregationResults<Map<String, Number>>(List.of(res), new Document());
                } else {
                    return null;
                }
            });
            Page<ApiCallDetailVo> page = apiCallService.findApiCallPage(filter);
            Assertions.assertEquals(page.getItems().size(), 0);
            Assertions.assertEquals(page.getTotal(), 0L);
        }

        @Test
        void testClientNameIsEmpty() {
            List<ApiCallDataVo> result = new ArrayList<>();
            UserDetail userDetail = mock(UserDetail.class);
            when(userDetail.getEmail()).thenReturn("admin@admin.com");
            when(userDetail.getUserId()).thenReturn("admin@admin.com");
            Filter filter = parseFilter("{\"order\": null,\"limit\":20,\"skip\":0,\"where\":{\"clientId\": \" \"}}");
            List<ApplicationDto> applications = new ArrayList<>();
            when(applicationService.findByIds(anyList())).thenReturn(applications);

            List<ModulesDto> modules = new ArrayList<>();
            when(modulesService.findAllModulesByIds(anyList())).thenReturn(modules);
            when(mongoTemplate.aggregate(any(Aggregation.class), any(String.class), any(Class.class))).thenAnswer((answer) -> {
                Class<?> className = answer.getArgument(2);
                if (Objects.equals(className.getSimpleName(), ApiCallDataVo.class.getSimpleName())) {
                    return new AggregationResults<>(result, new Document());
                } else if (Objects.equals(className.getSimpleName(), Map.class.getSimpleName())) {
                    Map<String, Number> res = new HashMap<>();
                    res.put("total", 0L);
                    return new AggregationResults<Map<String, Number>>(List.of(res), new Document());
                } else {
                    return null;
                }
            });
            Page<ApiCallDetailVo> page = apiCallService.findApiCallPage(filter);
            Assertions.assertEquals(page.getItems().size(), 0);
            Assertions.assertEquals(page.getTotal(), 0L);
        }

        @Test
        void testQueryResultNotEmpty() {
            List<ApiCallDataVo> result = new ArrayList<>();
            UserDetail userDetail = mock(UserDetail.class);
            when(userDetail.getEmail()).thenReturn("admin@admin.com");
            when(userDetail.getUserId()).thenReturn("admin@admin.com");
            Filter filter = parseFilter("{\"order\": null,\"limit\":20,\"skip\":0,\"where\":{\"clientId\": \" \"}}");
            List<ApplicationDto> applications = new ArrayList<>();
            when(applicationService.findByIds(anyList())).thenReturn(applications);

            List<ModulesDto> modules = new ArrayList<>();
            when(modulesService.findAllModulesByIds(anyList())).thenReturn(modules);
            when(mongoTemplate.aggregate(any(Aggregation.class), any(String.class), any(Class.class))).thenAnswer((answer) -> {
                Class<?> className = answer.getArgument(2);
                if (Objects.equals(className.getSimpleName(), ApiCallDataVo.class.getSimpleName())) {
                    return new AggregationResults<>(result, new Document());
                } else if (Objects.equals(className.getSimpleName(), Map.class.getSimpleName())) {
                    Map<String, Number> res = new HashMap<>();
                    res.put("total", 1L);
                    return new AggregationResults<Map<String, Number>>(List.of(res), new Document());
                } else {
                    return null;
                }
            });
            Page<ApiCallDetailVo> page = apiCallService.findApiCallPage(filter);
            Assertions.assertEquals(page.getItems().size(), 0);
            Assertions.assertEquals(page.getTotal(), 0L);
        }
    }

    @Nested
    class findApiParamTypeMapTest {
        @Test
        void testEmpty() {
            List<ModulesDto> all = new ArrayList<>();
            when(modulesService.findAll(any(Query.class))).thenReturn(all);
            Map<String, Map<String, Param>> result = apiCallService.findApiParamTypeMap(new ObjectId());
            Assertions.assertNotNull(result);
            Assertions.assertEquals(0, result.size());
        }
        @Test
        void testNotApiId() {
            List<ModulesDto> all = new ArrayList<>();
            when(modulesService.findAll(any(Query.class))).thenReturn(all);
            Map<String, Map<String, Param>> result = apiCallService.findApiParamTypeMap(new ObjectId[0]);
            Assertions.assertNotNull(result);
            Assertions.assertEquals(0, result.size());
        }

        @Test
        void testNormal() {
            List<ModulesDto> all = new ArrayList<>();
            ModulesDto modulesDto = new ModulesDto();
            all.add(modulesDto);
            ModulesDto modulesDto1 = new ModulesDto();
            all.add(modulesDto1);

            ModulesDto modulesDto2 = new ModulesDto();
            modulesDto2.setId(new ObjectId());
            all.add(modulesDto2);
            all.add(null);

            ModulesDto modulesDto3 = new ModulesDto();
            modulesDto3.setId(new ObjectId());
            modulesDto3.setPaths(List.of(new Path()));
            all.add(modulesDto3);

            ModulesDto modulesDto4 = new ModulesDto();
            modulesDto4.setId(new ObjectId());
            Path path = new Path();
            path.setName("name");
            Param param = new Param();
            param.setName("number");
            path.setParams(List.of(param));
            modulesDto4.setPaths(List.of(path));
            all.add(modulesDto4);

            ModulesDto modulesDto5 = new ModulesDto();
            modulesDto5.setId(new ObjectId());
            Path path1 = new Path();
            path1.setName("name");
            path1.setType("number");
            Param param1 = new Param();
            param1.setName("number");
            param1.setType("number");
            path1.setParams(List.of(param1));
            modulesDto5.setPaths(List.of(path1));
            all.add(modulesDto5);

            when(modulesService.findAll(any(Query.class))).thenReturn(all);
            Map<String, Map<String, Param>> result = apiCallService.findApiParamTypeMap(new ObjectId());
            Assertions.assertNotNull(result);
            Assertions.assertEquals(2, result.size());
        }
    }

    @Nested
    class getApiPercentileTest {
        ApiCallService call;
        private MongoTemplate mt;
        @BeforeEach
        void setUp() {
            call = mock(ApiCallService.class);
            mt = mock(MongoTemplate.class);
            ReflectionTestUtils.setField(call, "mongoOperations", mt);
            when(call.getApiPercentile(anyString(), anyLong(), anyLong())).thenCallRealMethod();
        }

        @Test
        void testNormal() {
            ModulesEntity entity = new ModulesEntity();
            entity.setName("module");
            when(mt.findOne(any(Query.class), any(Class.class))).thenReturn(entity);
            List<ApiCallEntity> apiCalls = new ArrayList<>();
            ApiCallEntity e1 = new ApiCallEntity();
            e1.setLatency(1000L);
            apiCalls.add(e1);
            apiCalls.add(null);
            ApiCallEntity e2 = new ApiCallEntity();
            apiCalls.add(e2);
            when(mt.find(any(Query.class), any(Class.class), anyString())).thenReturn(apiCalls);
            ApiPercentile apiPercentile = call.getApiPercentile(new ObjectId().toHexString(), 1L, 1L);
            Assertions.assertNotNull(apiPercentile);
            Assertions.assertNotNull(apiPercentile.getP50());
            Assertions.assertNotNull(apiPercentile.getP95());
            Assertions.assertNotNull(apiPercentile.getP99());
        }

        @Test
        void testResultIsEmpty() {
            ModulesEntity entity = new ModulesEntity();
            entity.setName("module");
            when(mt.findOne(any(Query.class), any(Class.class))).thenReturn(entity);
            when(mt.find(any(Query.class), any(Class.class), anyString())).thenReturn(new ArrayList<>());
            ApiPercentile apiPercentile = call.getApiPercentile(new ObjectId().toHexString(), 1L, 1L);
            Assertions.assertNotNull(apiPercentile);
            Assertions.assertNull(apiPercentile.getP50());
            Assertions.assertNull(apiPercentile.getP95());
            Assertions.assertNull(apiPercentile.getP99());
        }

        @Test
        void testApiIdIsBlank() {
            ModulesEntity entity = new ModulesEntity();
            entity.setName("module");
            when(mt.findOne(any(Query.class), any(Class.class))).thenReturn(entity);
            when(mt.find(any(Query.class), any(Class.class), anyString())).thenReturn(new ArrayList<>());
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    call.getApiPercentile("", 1L, 1L);
                } catch (BizException e) {
                    Assertions.assertEquals("api.call.api.id.required", e.getErrorCode());
                    throw e;
                }
            });
        }

        @Test
        void testQueryRangeTooLarge() {
            ModulesEntity entity = new ModulesEntity();
            entity.setName("module");
            when(mt.findOne(any(Query.class), any(Class.class))).thenReturn(entity);
            when(mt.find(any(Query.class), any(Class.class), anyString())).thenReturn(new ArrayList<>());
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    call.getApiPercentile(new ObjectId().toHexString(), 1L, 14 * 24 * 60 * 60 * 1000L);
                } catch (BizException e) {
                    Assertions.assertEquals("api.call.percentile.time.range.too.large", e.getErrorCode());
                    throw e;
                }
            });
        }

        @Test
        void testModuleNotExists() {
            when(mt.findOne(any(Query.class), any(Class.class))).thenReturn(null);
            when(mt.find(any(Query.class), any(Class.class), anyString())).thenReturn(new ArrayList<>());
            ApiPercentile apiPercentile = call.getApiPercentile(new ObjectId().toHexString(), 1L, 1L);
            Assertions.assertNotNull(apiPercentile);
            Assertions.assertNull(apiPercentile.getP50());
            Assertions.assertNull(apiPercentile.getP95());
            Assertions.assertNull(apiPercentile.getP99());
        }
    }

    @Nested
    class saveTest {
        @Test
        void testNormal() {
            List<ApiCallDto> saveApiCallParamList = new ArrayList<>();
            ApiCallDto dto = new ApiCallDto();
            saveApiCallParamList.add(dto);
            UserDetail userDetail = mock(UserDetail.class);
            when(userDetail.getUserId()).thenReturn("userId");
            when(mongoTemplate.insert(anyList(), anyString())).thenReturn(new ArrayList<>());
            doNothing().when(realTimeOfApiResponseSizeAlter).check(anyString(), anyList());
            apiCallService.save(saveApiCallParamList, userDetail);
        }
    }

    @Nested
    class findByIdTest {
        ApiCallService service;
        @BeforeEach
        void init() {
            service = mock(ApiCallService.class);
            when(service.findById(anyString(), any(UserDetail.class))).thenCallRealMethod();
            ReflectionTestUtils.setField(service, "mongoOperations", mongoTemplate);
            ReflectionTestUtils.setField(service, "modulesService", modulesService);
        }
        @Test
        void testNormal() {
            ModulesDto modulesDto = new ModulesDto();
            ApiCallEntity entity = new ApiCallEntity();
            UserDetail mock = mock(UserDetail.class);
            String allPathId = new ObjectId().toHexString();
            entity.setAllPathId(allPathId);
            doNothing().when(service).encryptionIfNeed(any(ApiCallDetailVo.class), any(ModulesDto.class));
            when(mongoTemplate.findById("id", ApiCallEntity.class)).thenReturn(entity);
            when(modulesService.findById(any(ObjectId.class), any(UserDetail.class))).thenReturn(modulesDto);
            ApiCallDetailVo apiCallDetailVo = service.findById("id", mock);
            Assertions.assertNotNull(apiCallDetailVo);
        }
        @Test
        void testLatency() {
            ModulesDto modulesDto = new ModulesDto();
            ApiCallEntity entity = new ApiCallEntity();
            entity.setLatency(1000L);
            UserDetail mock = mock(UserDetail.class);
            String allPathId = new ObjectId().toHexString();
            entity.setAllPathId(allPathId);
            doNothing().when(service).encryptionIfNeed(any(ApiCallDetailVo.class), any(ModulesDto.class));
            when(mongoTemplate.findById("id", ApiCallEntity.class)).thenReturn(entity);
            when(modulesService.findById(any(ObjectId.class), any(UserDetail.class))).thenReturn(modulesDto);
            ApiCallDetailVo apiCallDetailVo = apiCallService.findById("id", mock);
            Assertions.assertNotNull(apiCallDetailVo);
        }
    }

    @Nested
    class mapToApiCallDetailVoTest {
        ApiCallService service;
        @BeforeEach
        void init() {
            service = mock(ApiCallService.class);
            when(service.mapToApiCallDetailVo(any(ApiCallDataVo.class))).thenCallRealMethod();
        }
        @Test
        void testNormal() {
            ApiCallDataVo apiCallDataVo = new ApiCallDataVo();
            apiCallDataVo.setId(new ObjectId());
            apiCallDataVo.setReqTime(System.currentTimeMillis());
            apiCallDataVo.setSpeed(1000L);
            ApiCallDetailVo apiCallDetailVo = service.mapToApiCallDetailVo(apiCallDataVo);
            Assertions.assertNotNull(apiCallDetailVo);
        }
    }

    @Nested
    class encryptionIfNeedTest {
        @Test
        void testConfigFromModuleAndEncryptByRule() {
            ApiCallService service = mock(ApiCallService.class);
            doCallRealMethod().when(service).encryptionIfNeed(anyBoolean(), any(ApiCallDetailVo.class), any(ModulesDto.class));
            ReflectionTestUtils.setField(service, "ruleService", ruleService);
            ReflectionTestUtils.setField(service, "modulesService", modulesService);

            when(service.parseCustomParam(anyBoolean(), anyString(), any(Map.class)))
                    .thenAnswer(invocation -> "ENC:" + invocation.getArgument(1));
            when(service.parse(anyString(), anyBoolean(), any(Map.class)))
                    .thenAnswer(invocation -> "PARSED:" + invocation.getArgument(0));

            ObjectId ruleObjectId = new ObjectId();
            String ruleId = ruleObjectId.toHexString();
            TextEncryptionRuleDto ruleDto = new TextEncryptionRuleDto();
            ruleDto.setId(ruleObjectId);
            when(ruleService.getById(anySet())).thenReturn(List.of(ruleDto));

            Param param = new Param();
            param.setName("name");
            param.setTextEncryptionRuleIds(List.of(ruleId));
            Path path = new Path();
            path.setParams(List.of(param));
            ModulesDto moduleDto = new ModulesDto();
            moduleDto.setPaths(List.of(path));

            ApiCallDetailVo apiCallDetailVo = new ApiCallDetailVo();
            apiCallDetailVo.setApiId("");
            apiCallDetailVo.setFieldEncryptionRule(null);
            apiCallDetailVo.setQuery("{\"name\":\"gavin\"}");
            apiCallDetailVo.setBody("{\"name\":\"gavin\"}");
            apiCallDetailVo.setReqParams("{\"k\":\"v\"}");

            service.encryptionIfNeed(true, apiCallDetailVo, moduleDto);

            Assertions.assertEquals("PARSED:{\"name\":\"gavin\"}", apiCallDetailVo.getQuery());
            Assertions.assertEquals("PARSED:{\"name\":\"gavin\"}", apiCallDetailVo.getBody());
            Assertions.assertNull(apiCallDetailVo.getReqParams());
            verify(ruleService, times(0)).getById(anySet());
        }

        @Test
        void testRuleIdsEmptyFallbackToParse() {
            ApiCallService service = mock(ApiCallService.class);
            doCallRealMethod().when(service).encryptionIfNeed(anyBoolean(), any(ApiCallDetailVo.class), any(ModulesDto.class));
            ReflectionTestUtils.setField(service, "ruleService", ruleService);
            ReflectionTestUtils.setField(service, "modulesService", modulesService);

            when(service.parse(anyString(), anyBoolean(), any(Map.class)))
                    .thenAnswer(invocation -> "PARSED:" + invocation.getArgument(0));

            ApiCallDetailVo apiCallDetailVo = new ApiCallDetailVo();
            apiCallDetailVo.setApiId("");
            apiCallDetailVo.setFieldEncryptionRule(Map.of("name", new ArrayList<>()));
            apiCallDetailVo.setQuery("{\"name\":\"gavin\"}");
            apiCallDetailVo.setBody("{\"name\":\"gavin\"}");
            apiCallDetailVo.setReqParams("{\"k\":\"v\"}");

            service.encryptionIfNeed(true, apiCallDetailVo, null);

            Assertions.assertEquals("{\"name\":\"gavin\"}", apiCallDetailVo.getQuery());
            Assertions.assertEquals("{\"name\":\"gavin\"}", apiCallDetailVo.getBody());
            verify(ruleService, never()).getById(anySet());
        }

        @Test
        void testRuleNotFoundFallbackToParseAndLoadParamMap() {
            ApiCallService service = mock(ApiCallService.class);
            doCallRealMethod().when(service).encryptionIfNeed(anyBoolean(), any(ApiCallDetailVo.class), any(ModulesDto.class));
            ReflectionTestUtils.setField(service, "ruleService", ruleService);
            ReflectionTestUtils.setField(service, "modulesService", modulesService);

            ObjectId apiObjectId = new ObjectId();
            String apiId = apiObjectId.toHexString();
            Map<String, Param> apiParamMap = new HashMap<>();
            Param param = new Param();
            param.setName("name");
            param.setType("string");
            apiParamMap.put("name", param);
            when(service.findApiParamTypeMap(any(ObjectId.class))).thenReturn(Map.of(apiId, apiParamMap));
            when(service.parse(anyString(), anyBoolean(), any(Map.class)))
                    .thenAnswer(invocation -> {
                        Map<String, Param> passedParamMap = invocation.getArgument(2);
                        Assertions.assertSame(apiParamMap, passedParamMap);
                        return "PARSED:" + invocation.getArgument(0);
                    });

            when(ruleService.getById(anySet())).thenReturn(List.of());

            ApiCallDetailVo apiCallDetailVo = new ApiCallDetailVo();
            apiCallDetailVo.setApiId(apiId);
            apiCallDetailVo.setFieldEncryptionRule(Map.of("name", List.of(new ObjectId().toHexString())));
            apiCallDetailVo.setQuery("{\"name\":\"gavin\"}");
            apiCallDetailVo.setBody("{\"name\":\"gavin\"}");
            apiCallDetailVo.setReqParams("{\"k\":\"v\"}");

            service.encryptionIfNeed(true, apiCallDetailVo, null);

            Assertions.assertEquals("{\"name\":\"gavin\"}", apiCallDetailVo.getQuery());
            Assertions.assertEquals("{\"name\":\"gavin\"}", apiCallDetailVo.getBody());
            verify(ruleService,times(0)).getById(anySet());
        }

        @Test
        void testModuleHasNoRuleConfigFallbackToParse() {
            ApiCallService service = mock(ApiCallService.class);
            doCallRealMethod().when(service).encryptionIfNeed(anyBoolean(), any(ApiCallDetailVo.class), any(ModulesDto.class));
            ReflectionTestUtils.setField(service, "ruleService", ruleService);
            ReflectionTestUtils.setField(service, "modulesService", modulesService);

            when(service.parse(anyString(), anyBoolean(), any(Map.class)))
                    .thenAnswer(invocation -> "PARSED:" + invocation.getArgument(0));

            Param param = new Param();
            param.setName("name");
            param.setTextEncryptionRuleIds(List.of());
            Path path = new Path();
            path.setParams(List.of(param));
            ModulesDto moduleDto = new ModulesDto();
            moduleDto.setPaths(List.of(path));

            ApiCallDetailVo apiCallDetailVo = new ApiCallDetailVo();
            apiCallDetailVo.setApiId("");
            apiCallDetailVo.setFieldEncryptionRule(null);
            apiCallDetailVo.setQuery("{\"name\":\"gavin\"}");
            apiCallDetailVo.setBody("{\"name\":\"gavin\"}");
            apiCallDetailVo.setReqParams("{\"k\":\"v\"}");

            service.encryptionIfNeed(true, apiCallDetailVo, moduleDto);

            Assertions.assertEquals("PARSED:{\"name\":\"gavin\"}", apiCallDetailVo.getQuery());
            Assertions.assertEquals("PARSED:{\"name\":\"gavin\"}", apiCallDetailVo.getBody());
            Assertions.assertNull(apiCallDetailVo.getReqParams());
            verify(ruleService, never()).getById(anySet());
        }

        @Test
        void testFieldEncryptionRuleIncludesUnknownRuleId() {
            ApiCallService service = mock(ApiCallService.class);
            doCallRealMethod().when(service).encryptionIfNeed(anyBoolean(), any(ApiCallDetailVo.class), any(ModulesDto.class));
            ReflectionTestUtils.setField(service, "ruleService", ruleService);
            ReflectionTestUtils.setField(service, "modulesService", modulesService);

            when(service.parseCustomParam(anyBoolean(), anyString(), any(Map.class)))
                    .thenAnswer(invocation -> "ENC:" + invocation.getArgument(1));

            ObjectId ruleObjectId = new ObjectId();
            String ruleId = ruleObjectId.toHexString();
            TextEncryptionRuleDto ruleDto = new TextEncryptionRuleDto();
            ruleDto.setId(ruleObjectId);
            List<TextEncryptionRuleDto> list = new ArrayList<>();
            list.add(ruleDto);
            list.add(null);
            when(ruleService.getById(anySet())).thenReturn(list);

            Map<String, List<String>> fieldEncryptionRule = new HashMap<>();
            fieldEncryptionRule.put("name", List.of(ruleId, new ObjectId().toHexString()));

            ApiCallDetailVo apiCallDetailVo = new ApiCallDetailVo();
            apiCallDetailVo.setApiId("");
            apiCallDetailVo.setFieldEncryptionRule(fieldEncryptionRule);
            apiCallDetailVo.setQuery("{\"name\":\"gavin\"}");
            apiCallDetailVo.setBody("{\"name\":\"gavin\"}");
            apiCallDetailVo.setReqParams("{\"k\":\"v\"}");

            service.encryptionIfNeed(true, apiCallDetailVo, null);

            Assertions.assertEquals("{\"name\":\"gavin\"}", apiCallDetailVo.getQuery());
            Assertions.assertEquals("{\"name\":\"gavin\"}", apiCallDetailVo.getBody());
            verify(ruleService, times(0)).getById(anySet());
        }
    }

    @Nested
    @DisplayName("Method addMoreParam test")
    class addMoreParamTest {

        @Test
        @DisplayName("should set dbRate and httpTime when reqBytes > 0 and dataQueryEndTime not null")
        void testSetDbRateAndHttpTimeWhenDataQueryExists() {
            ApiCallEntity apiCallEntity = new ApiCallEntity();
            apiCallEntity.setReqTime(10L);
            apiCallEntity.setResTime(110L);
            apiCallEntity.setReqBytes(1000L);
            apiCallEntity.setDataQueryEndTime(100L);
            apiCallEntity.setDataQueryTotalTime(100L);

            ApiCallDetailVo apiCallDetailVo = new ApiCallDetailVo();
            apiCallDetailVo.setLatency(300D);
            apiCallDetailVo.setDataQueryTotalTime(100L);

            apiCallService.addMoreParam(apiCallEntity, apiCallDetailVo);

            Assertions.assertEquals(10L, apiCallDetailVo.getCallStart());
            Assertions.assertEquals(110L, apiCallDetailVo.getCallEnd());
            Assertions.assertEquals(1000L, apiCallDetailVo.getResponseBytes());
            Assertions.assertEquals(10000D, apiCallDetailVo.getDbRate(), 0.0001);
            Assertions.assertEquals(200D, apiCallDetailVo.getHttpTime(), 0.0001);
        }

        @Test
        @DisplayName("should be compatible with boundary scenarios when dataQueryTotalTime is 0")
        void testCompatibleWhenDataQueryTotalTimeIsZero() {
            ApiCallEntity apiCallEntity = new ApiCallEntity();
            apiCallEntity.setReqTime(10L);
            apiCallEntity.setResTime(110L);
            apiCallEntity.setReqBytes(1000L);
            apiCallEntity.setDataQueryEndTime(100L);
            apiCallEntity.setDataQueryTotalTime(0L);

            ApiCallDetailVo apiCallDetailVo = new ApiCallDetailVo();
            apiCallDetailVo.setLatency(300D);
            apiCallDetailVo.setDataQueryTotalTime(0L);

            apiCallService.addMoreParam(apiCallEntity, apiCallDetailVo);

            Assertions.assertEquals(10000000D, apiCallDetailVo.getDbRate(), 0.0001);
            Assertions.assertEquals(300D, apiCallDetailVo.getHttpTime(), 0.0001);
        }

        @Test
        @DisplayName("should set dbRate to 0 and calculate httpTime by resTime - reqTime when dataQueryEndTime is null")
        void testSetHttpTimeByReqAndResTimeWhenNoDataQueryEndTime() {
            ApiCallEntity apiCallEntity = new ApiCallEntity();
            apiCallEntity.setReqTime(10L);
            apiCallEntity.setResTime(60L);
            apiCallEntity.setReqBytes(1000L);
            apiCallEntity.setDataQueryEndTime(null);

            ApiCallDetailVo apiCallDetailVo = new ApiCallDetailVo();

            apiCallService.addMoreParam(apiCallEntity, apiCallDetailVo);

            Assertions.assertEquals(0D, apiCallDetailVo.getDbRate(), 0.0001);
            Assertions.assertEquals(50D, apiCallDetailVo.getHttpTime(), 0.0001);
        }

        @Test
        @DisplayName("should set httpTime to 0 when reqTime or resTime is null")
        void testSetHttpTimeToZeroWhenReqOrResTimeIsNull() {
            ApiCallEntity apiCallEntity = new ApiCallEntity();
            apiCallEntity.setReqTime(10L);
            apiCallEntity.setResTime(null);
            apiCallEntity.setReqBytes(0L);

            ApiCallDetailVo apiCallDetailVo = new ApiCallDetailVo();

            apiCallService.addMoreParam(apiCallEntity, apiCallDetailVo);

            Assertions.assertEquals(0D, apiCallDetailVo.getDbRate(), 0.0001);
            Assertions.assertEquals(0D, apiCallDetailVo.getHttpTime(), 0.0001);
        }
    }

    public Filter parseFilter(String filterJson) {
        filterJson = BaseController.replaceLoopBack(filterJson);
        Filter filter = JsonUtil.parseJson(filterJson, Filter.class);
        if (filter == null) {
            return new Filter();
        }
        Where where = filter.getWhere();
        if (where != null) {
            where.remove("user_id");
        }
        return filter;
    }
}
