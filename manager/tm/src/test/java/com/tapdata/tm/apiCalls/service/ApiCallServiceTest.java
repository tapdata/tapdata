package com.tapdata.tm.apiCalls.service;

import com.alibaba.fastjson.JSON;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.tapdata.tm.apiCalls.entity.ApiCallEntity;
import com.tapdata.tm.apiCalls.vo.ApiCallDataVo;
import com.tapdata.tm.apiCalls.vo.ApiCallDetailVo;
import com.tapdata.tm.apiCalls.vo.ApiPercentile;
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
import com.tapdata.tm.config.ApplicationConfig;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.modules.dto.ModulesDto;
import com.tapdata.tm.modules.dto.Param;
import com.tapdata.tm.modules.entity.ModulesEntity;
import com.tapdata.tm.modules.entity.Path;
import com.tapdata.tm.modules.service.ModulesService;
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
    ApplicationConfig applicationConfig;
    TextEncryptionRuleService ruleService;

    @BeforeEach
    void setUp() {
        ruleService = mock(TextEncryptionRuleService.class);
        apiCallService = new ApiCallService();
        apiCallMinuteStatsService = mock(ApiCallMinuteStatsService.class);
        applicationConfig = mock(ApplicationConfig.class);
        when(applicationConfig.getAdminAccount()).thenReturn("admin@admin.com");
        ReflectionTestUtils.setField(apiCallService, "applicationConfig", applicationConfig);
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
    @DisplayName("Method GenericFilterCriteria(Query) test")
    class GenericFilterCriteriaTest {

        @Test
        void testGenericFilterCriteria() {
            Filter filter = parseFilter("{\"order\":\"createTime DESC\",\"limit\":20,\"skip\":0,\"where\":{}}");
            Criteria criteria = apiCallService.genericFilterCriteria(filter);
            Assertions.assertEquals("{\"criteriaObject\":{\"$and\":[{},{}]}}", JSON.toJSONString(criteria));
        }
        @Test
        void testGenericFilterCriteriaMethod() {
            Filter filter = parseFilter("{\"order\":\"createTime DESC\",\"limit\":20,\"skip\":0,\"where\":{\"method\":\"get\"}}");
            Criteria criteria = apiCallService.genericFilterCriteria(filter);
            Assertions.assertEquals("{\"criteriaObject\":{\"method\":\"get\",\"$and\":[{},{}]}}", JSON.toJSONString(criteria));
        }
        @Test
        void testGenericFilterCriteriaCode() {
            Filter filter = parseFilter("{\"order\":\"createTime DESC\",\"limit\":20,\"skip\":0,\"where\":{\"code\":200}}");
            Criteria criteria = apiCallService.genericFilterCriteria(filter);
            Assertions.assertEquals("{\"criteriaObject\":{\"code\":\"200.0\",\"$and\":[{},{}]}}", JSON.toJSONString(criteria));
        }
        @Test
        void testGenericFilterCriteriaCodeV2() {
            Filter filter = parseFilter("{\"order\":\"createTime DESC\",\"limit\":20,\"skip\":0,\"where\":{\"code\":\" \"}}");
            Criteria criteria = apiCallService.genericFilterCriteria(filter);
            Assertions.assertEquals("{\"criteriaObject\":{\"code\":{\"$ne\":\"200\"},\"$and\":[{},{}]}}", JSON.toJSONString(criteria));
        }
        @Test
        void testGenericFilterCriteriaTime() {
            Filter filter = parseFilter("{\"order\":\"createTime DESC\",\"limit\":20,\"skip\":0,\"where\":{\"start\":1753804800000,\"end\":1753891200000}}");
            Criteria criteria = apiCallService.genericFilterCriteria(filter);
            Assertions.assertEquals("{\"criteriaObject\":{\"$and\":[{\"createTime\":{\"$gte\":1753804800000}},{\"createTime\":{\"$lte\":1753891200000}}]}}", JSON.toJSONString(criteria));
        }
    }

    @Nested
    class startFilterApiNameOrIdTest {
        @Test
        void testNormal() {
            Filter filter = parseFilter("{\"order\":\"createTime DESC\",\"limit\":20,\"skip\":0,\"where\":{\"or\":[{\"name\": {\"$regex\":\"xxx\"}},{\"id\": {\"$regex\":\"xxx\"}}]}}");
            Criteria criteria = new Criteria();
            apiCallService.startFilterApiNameOrId(filter, criteria);
            Assertions.assertEquals("{\"criteriaObject\":{\"$or\":[{\"allPathId\":\"xxx\"}]}}", JSON.toJSONString(criteria));
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
            Assertions.assertEquals("{\"criteriaObject\":{\"allPathId\":{\"$nin\":[\"\",null]}}}", JSON.toJSONString(criteria));
        }
        @Test
        void testNull() {
            Filter filter = parseFilter("{\"order\":\"createTime DESC\",\"limit\":20,\"skip\":0,\"where\":{\"or\":[{\"name\": {\"$regex\":null}},{\"id\": {\"$regex\":null}}]}}");
            Criteria criteria = new Criteria();
            apiCallService.startFilterApiNameOrId(filter, criteria);
            Assertions.assertEquals("{\"criteriaObject\":{\"allPathId\":{\"$nin\":[\"\",null]}}}", JSON.toJSONString(criteria));
        }
        @Test
        void testApiNameIsNull() {
            Filter filter = parseFilter("{\"order\":\"createTime DESC\",\"limit\":20,\"skip\":0,\"where\":{\"or\":[{\"name\": {\"$regex\":null}},{\"id\": {\"$regex\":null}}]}}");
            Criteria criteria = new Criteria();
            apiCallService.startFilterApiNameOrId(filter, criteria);
            Assertions.assertEquals("{\"criteriaObject\":{\"allPathId\":{\"$nin\":[\"\",null]}}}", JSON.toJSONString(criteria));
        }
        @Test
        void testApiNameIsEmpty() {
            Filter filter = parseFilter("{\"order\":\"createTime DESC\",\"limit\":20,\"skip\":0,\"where\":{\"or\":[{\"name\": {\"$regex\":\"\"}},{\"id\": {\"$regex\":\"xxx\"}}]}}");
            Criteria criteria = new Criteria();
            apiCallService.startFilterApiNameOrId(filter, criteria);
            Assertions.assertEquals("{\"criteriaObject\":{\"allPathId\":{\"$nin\":[\"\",null]}}}", JSON.toJSONString(criteria));
        }
        @Test
        void testApiNameIsLikeButNotAnyApi() {
            when(modulesService.findAll(any(Query.class))).thenReturn(new ArrayList<>());
            Filter filter = parseFilter("{\"order\":\"createTime DESC\",\"limit\":20,\"skip\":0,\"where\":{\"or\":[{\"name\": {\"$regex\":\"xxx\"}},{\"id\": {\"$regex\":\"xxx\"}}]}}");
            Criteria criteria = new Criteria();
            apiCallService.startFilterApiNameOrId(filter, criteria);
            Assertions.assertEquals("{\"criteriaObject\":{\"$or\":[{\"allPathId\":\"xxx\"}]}}", JSON.toJSONString(criteria));
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
            Assertions.assertEquals("{\"criteriaObject\":{\"$or\":[{\"allPathId\":{\"$in\":[\"68a7e8decd50c74ff40731f4\"]}},{\"allPathId\":\"xxx\"}]}}", JSON.toJSONString(criteria));
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
            Page<ApiCallDetailVo> page = apiCallService.find(filter);
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
            Page<ApiCallDetailVo> page = apiCallService.find(filter);
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
            Page<ApiCallDetailVo> page = apiCallService.find(filter);
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
            Page<ApiCallDetailVo> page = apiCallService.find(filter);
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
            Page<ApiCallDetailVo> page = apiCallService.find(filter);
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
            Page<ApiCallDetailVo> page = apiCallService.find(filter);
            Assertions.assertEquals(page.getItems().size(), 0);
            Assertions.assertEquals(page.getTotal(), 1L);
        }

        @Test
        void testQueryResultNotEmpty2() {
            List<ApiCallDataVo> result = new ArrayList<>();

            String apiId = new ObjectId().toHexString();
            result.add(new ApiCallDataVo());
            result.get(0).setId(new ObjectId());
            result.get(0).setApiId(apiId);
            result.get(0).setUserInfo(new HashMap<>());
            result.get(0).getUserInfo().put(ApiCallService.Tag.CLIENT_ID, new ObjectId().toHexString());

            result.add(null);

            result.add(new ApiCallDataVo());
            result.get(2).setId(new ObjectId());

            UserDetail userDetail = mock(UserDetail.class);
            when(userDetail.getEmail()).thenReturn("admin@admin.com");
            when(userDetail.getUserId()).thenReturn("admin@admin.com");
            Filter filter = parseFilter("{\"order\": \"createTime ASC\",\"limit\":20,\"skip\":0,\"where\":{\"clientId\": \" \"}}");
            List<ApplicationDto> applications = new ArrayList<>();
            ApplicationDto app = new ApplicationDto();
            app.setClientId(result.get(0).getUserInfo().get(ApiCallService.Tag.CLIENT_ID).toString());
            app.setClientName("name");
            when(applicationService.findByIds(anyList())).thenReturn(applications);

            List<ModulesDto> modules = new ArrayList<>();
            ModulesDto api = new ModulesDto();
            api.setId(new ObjectId(apiId));
            api.setName("apiName");
            modules.add(api);
            modules.add(null);
            when(modulesService.findAllModulesByIds(anyList())).thenReturn(modules);

            when(mongoTemplate.aggregate(any(Aggregation.class), any(String.class), any(Class.class))).thenAnswer((answer) -> {
                Class<?> className = answer.getArgument(2);
                if (Objects.equals(className.getSimpleName(), ApiCallDataVo.class.getSimpleName())) {
                    return new AggregationResults<>(result, new Document());
                } else if (Objects.equals(className.getSimpleName(), Map.class.getSimpleName())) {
                    Map<String, Number> res = new HashMap<>();
                    res.put("total", 3L);
                    return new AggregationResults<Map<String, Number>>(List.of(res), new Document());
                } else {
                    return null;
                }
            });
            Page<ApiCallDetailVo> page = apiCallService.find(filter);
            Assertions.assertEquals(page.getItems().size(), 2);
            Assertions.assertEquals(page.getTotal(), 3L);
        }
    }

    @Nested
    class mapToApiCallDetailVoTest {
        @Test
        void testNormal() {
            ApiCallDataVo entity = new ApiCallDataVo();
            entity.setId(new ObjectId());
            ApiCallDetailVo apiCallDetailVo = apiCallService.mapToApiCallDetailVo(entity);
            Assertions.assertNotNull(apiCallDetailVo.getId());
        }
    }

    @Nested
    class afterFindEntityTest {
        @Test
        void testNormal() {
            when(ruleService.checkAudioSwitchStatus()).thenReturn(true);
            ApiCallEntity entity = new ApiCallEntity();
            ApiCallEntity apiCallEntity = apiCallService.afterFindEntity(entity);
            Assertions.assertEquals(entity, apiCallEntity);
        }

        @Test
        void testBatchNormal() {
            List<ApiCallEntity> entities = new ArrayList<>();
            when(ruleService.checkAudioSwitchStatus()).thenReturn(true);
            ApiCallEntity entity = new ApiCallEntity();
            entities.add(entity);
            entity.setAllPathId(new ObjectId().toHexString());
            List<ApiCallEntity> apiCallEntities = apiCallService.afterFindEntity(entities);
            Assertions.assertEquals(entities, apiCallEntities);
        }

        @Test
        void testBatchNormal2() {
            List<ApiCallEntity> entities = new ArrayList<>();
            when(ruleService.checkAudioSwitchStatus()).thenReturn(true);
            List<ApiCallEntity> apiCallEntities = apiCallService.afterFindEntity(entities);
            Assertions.assertEquals(entities, apiCallEntities);
        }

        @Test
        void testBatchNormalafterFindDto() {
            List<ApiCallDataVo> entities = new ArrayList<>();
            when(ruleService.checkAudioSwitchStatus()).thenReturn(true);
            ApiCallDataVo entity = new ApiCallDataVo();
            entities.add(entity);
            entity.setApiId(new ObjectId().toHexString());
            List<ApiCallDataVo> apiCallEntities = apiCallService.afterFindDto(entities);
            Assertions.assertEquals(entities, apiCallEntities);
        }

        @Test
        void testBatchNormalafterFindDtoFalse() {
            List<ApiCallDataVo> entities = new ArrayList<>();
            when(ruleService.checkAudioSwitchStatus()).thenReturn(false);
            ApiCallDataVo entity = new ApiCallDataVo();
            entities.add(entity);
            entity.setApiId(new ObjectId().toHexString());
            List<ApiCallDataVo> apiCallEntities = apiCallService.afterFindDto(entities);
            Assertions.assertEquals(entities, apiCallEntities);
        }

        @Test
        void testBatchNormalafterFindDtoJsonNotEmpty() {
            List<ApiCallDataVo> entities = new ArrayList<>();
            when(ruleService.checkAudioSwitchStatus()).thenReturn(true);
            ApiCallDataVo entity = new ApiCallDataVo();
            entities.add(entity);
            entity.setQuery("{\"id\": \"xxkdf\"}");
            entity.setApiId(new ObjectId().toHexString());
            List<ApiCallDataVo> apiCallEntities = apiCallService.afterFindDto(entities);
            Assertions.assertEquals(entities, apiCallEntities);
        }

        @Test
        void testBatchNormalafterFindDtoJsonInvalide() {
            List<ApiCallDataVo> entities = new ArrayList<>();
            when(ruleService.checkAudioSwitchStatus()).thenReturn(true);
            ApiCallDataVo entity = new ApiCallDataVo();
            entities.add(entity);
            entity.setQuery("{");
            entity.setApiId(new ObjectId().toHexString());
            List<ApiCallDataVo> apiCallEntities = apiCallService.afterFindDto(entities);
            Assertions.assertEquals(entities, apiCallEntities);
        }

        @Test
        void testBatchNormalafterFindDtoJsonInvalide2() {
            List<ApiCallDataVo> entities = new ArrayList<>();
            when(ruleService.checkAudioSwitchStatus()).thenReturn(false);
            ApiCallDataVo entity = new ApiCallDataVo();
            entities.add(entity);
            entity.setQuery("{");
            entity.setApiId(new ObjectId().toHexString());
            List<ApiCallDataVo> apiCallEntities = apiCallService.afterFindDto(entities);
            Assertions.assertEquals(entities, apiCallEntities);
        }

        @Test
        void testBatchNormal2afterFindDto() {
            List<ApiCallDataVo> entities = new ArrayList<>();
            when(ruleService.checkAudioSwitchStatus()).thenReturn(true);
            List<ApiCallDataVo> apiCallEntities = apiCallService.afterFindDto(entities);
            Assertions.assertEquals(entities, apiCallEntities);
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