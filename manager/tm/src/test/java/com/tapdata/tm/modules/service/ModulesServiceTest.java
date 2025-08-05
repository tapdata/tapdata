package com.tapdata.tm.modules.service;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.tapdata.tm.apiCalls.entity.ApiCallEntity;
import com.tapdata.tm.apiCalls.service.ApiCallService;
import com.tapdata.tm.apicallminutestats.dto.ApiCallMinuteStatsDto;
import com.tapdata.tm.apicallminutestats.service.ApiCallMinuteStatsService;
import com.tapdata.tm.apicallstats.dto.ApiCallStatsDto;
import com.tapdata.tm.apicallstats.service.ApiCallStatsService;
import com.tapdata.tm.application.dto.ApplicationDto;
import com.tapdata.tm.application.service.ApplicationService;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.Tag;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceDefinitionService;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.modules.constant.ModuleStatusEnum;
import com.tapdata.tm.modules.dto.ModulesDto;
import com.tapdata.tm.modules.dto.ModulesPermissionsDto;
import com.tapdata.tm.modules.dto.ModulesTagsDto;
import com.tapdata.tm.modules.entity.ModulesEntity;
import com.tapdata.tm.modules.entity.Path;
import com.tapdata.tm.modules.param.ApiDetailParam;
import com.tapdata.tm.modules.repository.ModulesRepository;
import com.tapdata.tm.modules.vo.*;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.*;
import org.mockito.internal.verification.Times;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@DisplayName("Class ModulesService Test")
class ModulesServiceTest {
    ModulesService modulesService;
    ModulesRepository modulesRepository;

    DataSourceService dataSourceService;
    DataSourceDefinitionService dataSourceDefinitionService;

    @BeforeEach
    void init(){
        modulesRepository = mock(ModulesRepository.class);
        modulesService = new ModulesService(modulesRepository);
        dataSourceService = mock(DataSourceService.class);
        dataSourceDefinitionService = mock(DataSourceDefinitionService.class);
        ReflectionTestUtils.setField(modulesService, "dataSourceService", dataSourceService);
        ReflectionTestUtils.setField(modulesService, "dataSourceDefinitionService", dataSourceDefinitionService);
    }

    @Nested
    class UpdatePermissionsTest{
        @Test
        void test_main(){
            ModulesPermissionsDto modulesPermissionsDto = new ModulesPermissionsDto();
            modulesPermissionsDto.setModuleId("test");
            modulesPermissionsDto.setAcl(Arrays.asList("admin"));
            modulesService.updatePermissions(modulesPermissionsDto,mock(UserDetail.class));
            verify(modulesRepository,times(1)).updateFirst(any(),any(),any());
        }

        @Test
        void test_aclIsNull(){
            ModulesPermissionsDto modulesPermissionsDto = new ModulesPermissionsDto();
            modulesPermissionsDto.setModuleId("test");
            assertThrows(BizException.class,()->modulesService.updatePermissions(modulesPermissionsDto,mock(UserDetail.class)));
        }

    }

    @Nested
    class BatchUpdatePermissionsTest{
        @Test
        void test_batchUpdatePermissionsExclusive(){
            ModulesPermissionsDto permissionsDto = new ModulesPermissionsDto();
            permissionsDto.setModuleIds(Arrays.asList("module1", "module2"));
            permissionsDto.setAclName("admin");

            modulesService.updatePermissions(permissionsDto, mock(UserDetail.class));
            // 验证调用了两次 update：一次移除所有，一次添加指定
            verify(modulesRepository, times(2)).update(any(), any(), any());
        }

        @Test
        void test_singleModuleUpdate(){
            ModulesPermissionsDto permissionsDto = new ModulesPermissionsDto();
            permissionsDto.setModuleId("module1");
            permissionsDto.setAcl(Arrays.asList("admin", "user"));

            modulesService.updatePermissions(permissionsDto, mock(UserDetail.class));
            verify(modulesRepository, times(1)).updateFirst(any(), any(), any());
        }

        @Test
        void test_invalidParams(){
            ModulesPermissionsDto permissionsDto = new ModulesPermissionsDto();
            // 既没有设置单个模块参数，也没有设置批量参数

            assertThrows(BizException.class, () ->
                modulesService.updatePermissions(permissionsDto, mock(UserDetail.class)));
        }
    }

    @Nested
    class UpdateTagsTest{
        @Test
        void test_main(){
            ModulesTagsDto modulesTagsDto = new ModulesTagsDto();
            modulesTagsDto.setModuleId("test");
            modulesTagsDto.setListtags(Arrays.asList(new Tag("id","app")));
            modulesService.updateTags(modulesTagsDto,mock(UserDetail.class));
            verify(modulesRepository,times(1)).updateFirst(any(),any(),any());
        }

        @Test
        void test_tagsIsNull(){
            ModulesTagsDto modulesTagsDto = new ModulesTagsDto();
            modulesTagsDto.setModuleId("test");
            assertThrows(BizException.class,()->modulesService.updateTags(modulesTagsDto,mock(UserDetail.class)));
        }

    }

    @Nested
    class BeforeSaveTest{
        @Test
        void test_main(){
            ModulesDto modules = new ModulesDto();
            List<Path> paths = new ArrayList<>();
            Path path = new Path();
            List<Field> fields = new ArrayList<>();
            fields.add(new Field());
            fields.add(null);
            fields.add(null);
            path.setFields(fields);
            paths.add(path);
            modules.setPaths(paths);
            modulesService.beforeSave(modules,mock(UserDetail.class));
            Assertions.assertEquals(1,modules.getPaths().get(0).getFields().size());
        }

        @Test
        void test_paths_isNull(){
            ModulesDto modules = new ModulesDto();
            List<Path> paths = new ArrayList<>();
            modules.setPaths(paths);
            modulesService.beforeSave(modules,mock(UserDetail.class));
            Assertions.assertEquals(0,modules.getPaths().size());
        }

        @Test
        void test_paths_fields_isNull(){
            ModulesDto modules = new ModulesDto();
            List<Path> paths = new ArrayList<>();
            Path path = new Path();
            List<Field> fields = new ArrayList<>();
            path.setFields(fields);
            paths.add(path);
            modules.setPaths(paths);
            modulesService.beforeSave(modules,mock(UserDetail.class));
            Assertions.assertEquals(0,modules.getPaths().get(0).getFields().size());
        }
    }

    @Nested
    class saveTest{
        private ModulesDto modulesDto;
        private UserDetail userDetail;

        @BeforeEach
        void beforeEach(){
            modulesService = spy(modulesService);
            modulesDto = mock(ModulesDto.class);
            userDetail = mock(UserDetail.class);
        }

        @Test
        @DisplayName("test save method when name existed")
        void test1(){
            String name = "test";
            when(modulesDto.getName()).thenReturn(name);
            List<ModulesDto> modules = new ArrayList<>();
            modules.add(mock(ModulesDto.class));
            modules.add(mock(ModulesDto.class));
            doReturn(modules).when(modulesService).findByName(name);
            assertThrows(BizException.class, ()->modulesService.save(modulesDto, userDetail));
        }

        @Test
        @DisplayName("test save method normal")
        void test2(){
            String name = "test";
            when(modulesDto.getName()).thenReturn(name);
            List<ModulesDto> modules = new ArrayList<>();
            doReturn(modules).when(modulesService).findByName(name);
            doCallRealMethod().when(modulesService).save(modulesDto, userDetail);
            when(modulesRepository.save(any(),any())).thenReturn(mock(ModulesEntity.class));
            modulesService.save(modulesDto, userDetail);
            verify(modulesRepository).save(any(),any());
        }
    }

    @Nested
    class batchUpdateModuleByListTest{
        private List<ModulesDto> modulesDtos;
        private UserDetail userDetail;

        @Test
        void testBatchUpdateModuleByListNormal(){
            modulesService = spy(modulesService);
            modulesDtos = new ArrayList<>();
            ModulesDto modulesDto = mock(ModulesDto.class);
            modulesDtos.add(modulesDto);
            userDetail = mock(UserDetail.class);
            doReturn(modulesDto).when(modulesService).updateModuleById(modulesDto, userDetail);
            modulesService.batchUpdateModuleByList(modulesDtos, userDetail);
            verify(modulesService,new Times(1)).updateModuleById(modulesDto, userDetail);
        }
    }

    @Nested
    @DisplayName("Method preview test")
    class previewTest {
        @Test
        @DisplayName("test main process")
        void test1() {
            ApiCallStatsDto apiCallStatsDto = new ApiCallStatsDto() {{
                setCallTotalCount(200L);
                setTransferDataTotalBytes(200L);
                setCallAlarmTotalCount(8L);
                setResponseDataRowTotalCount(250L);
                setTotalResponseTime(60000L);
                setMaxResponseTime(2000L);
                setAlarmApiTotalCount(2L);
            }};
            ApiCallStatsService apiCallStatsService = mock(ApiCallStatsService.class);
            ReflectionTestUtils.setField(modulesService, "apiCallStatsService", apiCallStatsService);
            when(apiCallStatsService.aggregateByUserId(anyString())).thenAnswer(invocationOnMock -> {
                assertEquals("user1", invocationOnMock.getArgument(0));
                return apiCallStatsDto;
            });
            UserDetail userDetail = mock(UserDetail.class);
            when(userDetail.getUserId()).thenReturn("user1");
            when(userDetail.isRoot()).thenReturn(false);
            when(modulesService.count(any(Query.class))).thenAnswer(invocationOnMock -> {
                Query query = invocationOnMock.getArgument(0);
                assertEquals("{\"is_deleted\": {\"$ne\": true}, \"user_id\": \"user1\"}", query.getQueryObject().toJson());
                return 2L;
            });

            PreviewVo preview = modulesService.preview(userDetail);

            assertEquals(2L, preview.getTotalCount());
            assertEquals(200L, preview.getVisitTotalCount());
            assertEquals(8L, preview.getWarningVisitTotalCount());
            assertEquals(250L, preview.getVisitTotalLine());
            assertEquals(200L, preview.getTransmitTotal());
            assertEquals(2L, preview.getWarningApiCount());
        }

        @Test
        @DisplayName("test user detail is null")
        void test2() {
            PreviewVo preview = assertDoesNotThrow(() -> modulesService.preview(null));

            assertEquals(0L, preview.getTotalCount());
            assertEquals(0L, preview.getVisitTotalCount());
            assertEquals(0L, preview.getWarningVisitTotalCount());
            assertEquals(0L, preview.getVisitTotalLine());
            assertEquals(0L, preview.getTransmitTotal());
            assertEquals(0L, preview.getWarningApiCount());
        }

        @Test
        @DisplayName("test user id is null")
        void test3() {
            UserDetail userDetail = mock(UserDetail.class);
            PreviewVo preview = assertDoesNotThrow(() -> modulesService.preview(userDetail));

            assertEquals(0L, preview.getTotalCount());
            assertEquals(0L, preview.getVisitTotalCount());
            assertEquals(0L, preview.getWarningVisitTotalCount());
            assertEquals(0L, preview.getVisitTotalLine());
            assertEquals(0L, preview.getTransmitTotal());
            assertEquals(0L, preview.getWarningApiCount());
        }
    }

    @Nested
    @DisplayName("Method rankLists test")
    class rankListsTest {

        private ApiCallStatsService apiCallStatsService;

        @BeforeEach
        void setUp() {
            apiCallStatsService = mock(ApiCallStatsService.class);
            ReflectionTestUtils.setField(modulesService, "apiCallStatsService", apiCallStatsService);
            modulesService = spy(modulesService);
        }

        @Test
        @DisplayName("test main process with default where filter")
        void test1() {
            Filter filter = new Filter();
            filter.setLimit(2);
            filter.setSkip(5);
            UserDetail userDetail = mock(UserDetail.class);
            when(userDetail.getUserId()).thenReturn("user1");
            when(apiCallStatsService.count(any(Query.class))).thenAnswer(invocationOnMock -> {
                Query query = invocationOnMock.getArgument(0);
                assertEquals("{\"user_id\": \"user1\"}", query.getQueryObject().toJson());
                return 19L;
            });
            List<ModulesDto> modulesDtoList = new ArrayList<ModulesDto>(){{
                add(new ModulesDto() {{
                    setId(new ObjectId());
                    setName("module 1");
                }});
                add(new ModulesDto() {{
                    setId(new ObjectId());
                    setName("module 2");
                }});
            }};
            List<ApiCallStatsDto> apiCallStatsDtoList = new ArrayList<ApiCallStatsDto>(){{
                add(new ApiCallStatsDto() {{
                    setModuleId(modulesDtoList.get(0).getId().toString());
                    setAccessFailureRate(0.01D);
                }});
                add(new ApiCallStatsDto() {{
                    setModuleId(modulesDtoList.get(1).getId().toString());
                    setAccessFailureRate(0D);
                }});
            }};
            when(apiCallStatsService.findAll(any(Query.class))).thenAnswer(invocationOnMock -> {
                Query query = invocationOnMock.getArgument(0);
                assertEquals("{\"user_id\": \"user1\"}", query.getQueryObject().toJson());
                assertEquals("{\"accessFailureRate\": -1, \"createTime\": -1}", query.getSortObject().toJson());
                assertEquals("{\"accessFailureRate\": 1, \"moduleId\": 1}", query.getFieldsObject().toJson());
                assertEquals(2, query.getLimit());
                assertEquals(4, query.getSkip());
                return apiCallStatsDtoList;
            });
            doAnswer(invocationOnMock -> {
                Query query = invocationOnMock.getArgument(0);
                assertEquals("{\"id\": {\"$in\": [{\"$oid\": \"" + apiCallStatsDtoList.get(0).getModuleId() + "\"}, {\"$oid\": \"" + apiCallStatsDtoList.get(1).getModuleId() + "\"}]}}", query.getQueryObject().toJson());
                return modulesDtoList;
            }).when(modulesService).findAll(any(Query.class));

            RankListsVo rankListsVo = modulesService.rankLists(filter, userDetail);

            assertEquals(19L, rankListsVo.getTotal());
            assertEquals(2, rankListsVo.getItems().size());
            List<Map<String, Object>> items = rankListsVo.getItems();
            assertEquals(0.01D, items.get(0).get("module 1"));
            assertEquals(0D, items.get(1).get("module 2"));
        }

        @Test
        @DisplayName("test type is responseTime")
        void test2() {
            Filter filter = new Filter();
            filter.setLimit(2);
            filter.setSkip(5);
            filter.where("type", "responseTime");
            filter.setOrder("ASC");
            UserDetail userDetail = mock(UserDetail.class);
            when(userDetail.getUserId()).thenReturn("user1");
            when(apiCallStatsService.count(any(Query.class))).thenAnswer(invocationOnMock -> {
                Query query = invocationOnMock.getArgument(0);
                assertEquals("{\"user_id\": \"user1\"}", query.getQueryObject().toJson());
                return 19L;
            });
            List<ModulesDto> modulesDtoList = new ArrayList<ModulesDto>(){{
                add(new ModulesDto() {{
                    setId(new ObjectId());
                    setName("module 1");
                }});
                add(new ModulesDto() {{
                    setId(new ObjectId());
                    setName("module 2");
                }});
            }};
            List<ApiCallStatsDto> apiCallStatsDtoList = new ArrayList<ApiCallStatsDto>(){{
                add(new ApiCallStatsDto() {{
                    setModuleId(modulesDtoList.get(0).getId().toString());
                    setMaxResponseTime(2000L);
                }});
                add(new ApiCallStatsDto() {{
                    setModuleId(modulesDtoList.get(1).getId().toString());
                    setMaxResponseTime(1000L);
                }});
            }};
            when(apiCallStatsService.findAll(any(Query.class))).thenAnswer(invocationOnMock -> {
                Query query = invocationOnMock.getArgument(0);
                assertEquals("{\"user_id\": \"user1\"}", query.getQueryObject().toJson());
                assertEquals("{\"maxResponseTime\": 1, \"createTime\": -1}", query.getSortObject().toJson());
                assertEquals("{\"maxResponseTime\": 1, \"moduleId\": 1}", query.getFieldsObject().toJson());
                assertEquals(2, query.getLimit());
                assertEquals(4, query.getSkip());
                return apiCallStatsDtoList;
            });
            doAnswer(invocationOnMock -> {
                Query query = invocationOnMock.getArgument(0);
                assertEquals("{\"id\": {\"$in\": [{\"$oid\": \"" + apiCallStatsDtoList.get(0).getModuleId() + "\"}, {\"$oid\": \"" + apiCallStatsDtoList.get(1).getModuleId() + "\"}]}}", query.getQueryObject().toJson());
                return modulesDtoList;
            }).when(modulesService).findAll(any(Query.class));

            RankListsVo rankListsVo = modulesService.rankLists(filter, userDetail);

            assertEquals(19L, rankListsVo.getTotal());
            assertEquals(2, rankListsVo.getItems().size());
            List<Map<String, Object>> items = rankListsVo.getItems();
            assertEquals(2000L, items.get(0).get("module 1"));
            assertEquals(1000L, items.get(1).get("module 2"));
        }
    }

    @Nested
    @DisplayName("Method apiList test")
    class apiListTest {
        @Test
        @DisplayName("test main process")
        void test1() {
            Filter filter = new Filter();
            filter.setOrder("createTime DESC");
            UserDetail userDetail = mock(UserDetail.class);
            when(userDetail.getUserId()).thenReturn("user1");
            MongoCollection<Document> mongoCollection = mock(MongoCollection.class);
            MongoTemplate mongoTemplate = mock(MongoTemplate.class);
            when(mongoTemplate.getCollection("ApiCallStats")).thenReturn(mongoCollection);
            ModulesRepository repository = mock(ModulesRepository.class);
            when(repository.getMongoOperations()).thenReturn(mongoTemplate);
            ReflectionTestUtils.setField(modulesService, "repository", repository);
            MongoCursor<Document> mongoCursor = mock(MongoCursor.class);
            AggregateIterable<Document> aggregateIterable = mock(AggregateIterable.class);
            when(aggregateIterable.allowDiskUse(any(Boolean.class))).thenReturn(aggregateIterable);
            when(aggregateIterable.iterator()).thenReturn(mongoCursor);
            when(mongoCollection.aggregate(any(List.class))).thenAnswer(invocationOnMock -> {
                List<Document> pipeline = invocationOnMock.getArgument(0);
                assertEquals(5, pipeline.size());
                assertEquals("{\"$project\": {\"moduleId\": {\"$toObjectId\": \"$moduleId\"}, \"responseDataRowTotalCount\": 1, \"callTotalCount\": 1, \"transferDataTotalBytes\": 1, \"clientIds\": 1}}", pipeline.get(0).toJson());
                assertEquals("{\"$lookup\": {\"from\": \"Modules\", \"let\": {\"moduleId\": \"$moduleId\"}, \"pipeline\": [{\"$project\": {\"_id\": 1, \"name\": 1, \"status\": 1, \"user_id\": 1, \"createTime\": 1}}, {\"$match\": {\"$expr\": {\"$eq\": [\"$_id\", \"$$moduleId\"]}}}], \"as\": \"modules\"}}", pipeline.get(1).toJson());
                assertEquals("{\"$unwind\": {\"path\": \"$modules\"}}", pipeline.get(2).toJson());
                assertEquals("{\"$match\": {\"modules.user_id\": \"user1\"}}", pipeline.get(3).toJson());
                assertEquals("{\"$facet\": {\"total\": [{\"$group\": {\"_id\": null, \"total\": {\"$sum\": 1}}}], \"items\": [{\"$sort\": {\"modules.createTime\": -1}}, {\"$limit\": 20}]}}", pipeline.get(4).toJson());
                return aggregateIterable;
            });
            String json = "{\"total\": [{\"_id\": null, \"total\": 54}], \"items\": [{\"_id\": {\"$oid\": \"66d16f366d155a6f5d5b54a2\"}, \"callTotalCount\": 29008, \"clientIds\": [\"5c0e750b7a5cd42464a5099d\"], \"responseDataRowTotalCount\": 7617120, \"transferDataTotalBytes\": 161297942, \"moduleId\": {\"$oid\": \"65fcf4a7edf441365e98dff2\"}, \"modules\": {\"_id\": {\"$oid\": \"65fcf4a7edf441365e98dff2\"}, \"name\": \"POSS_IM_COMPANY\", \"status\": \"pending\", \"createTime\": {\"$date\": \"2024-03-22T03:01:59.379Z\"}, \"user_id\": \"62bc5008d4958d013d97c7a6\"}}, {\"_id\": {\"$oid\": \"66d16f366d155a6f5d5b54a1\"}, \"callTotalCount\": 20002, \"clientIds\": [\"5c0e750b7a5cd42464a5099d\"], \"responseDataRowTotalCount\": 2429040, \"transferDataTotalBytes\": 104957808, \"moduleId\": {\"$oid\": \"65fced81edf441365e98d9ad\"}, \"modules\": {\"_id\": {\"$oid\": \"65fced81edf441365e98d9ad\"}, \"name\": \"POSS_IV_STYLE_CATG\", \"status\": \"active\", \"createTime\": {\"$date\": \"2024-03-22T02:31:29.986Z\"}, \"user_id\": \"62bc5008d4958d013d97c7a6\"}}, {\"_id\": {\"$oid\": \"66d16f356d155a6f5d5b54a0\"}, \"callTotalCount\": 28001, \"clientIds\": [\"5c0e750b7a5cd42464a5099d\"], \"responseDataRowTotalCount\": 3604020, \"transferDataTotalBytes\": 124127362, \"moduleId\": {\"$oid\": \"65fc02faedf441365e97a697\"}, \"modules\": {\"_id\": {\"$oid\": \"65fc02faedf441365e97a697\"}, \"name\": \"MDM_catalogItem_changelog\", \"status\": \"active\", \"createTime\": {\"$date\": \"2024-03-21T09:50:50.036Z\"}, \"user_id\": \"62bc5008d4958d013d97c7a6\"}}, {\"_id\": {\"$oid\": \"66d16f356d155a6f5d5b549f\"}, \"callTotalCount\": 0, \"clientIds\": [], \"responseDataRowTotalCount\": 0, \"transferDataTotalBytes\": 0, \"moduleId\": {\"$oid\": \"65fbfa61edf441365e979cfd\"}, \"modules\": {\"_id\": {\"$oid\": \"65fbfa61edf441365e979cfd\"}, \"name\": \"MDM_model_changelog\", \"status\": \"active\", \"createTime\": {\"$date\": \"2024-03-21T09:14:09.967Z\"}, \"user_id\": \"62bc5008d4958d013d97c7a6\"}}, {\"_id\": {\"$oid\": \"66d16f356d155a6f5d5b549e\"}, \"callTotalCount\": 37001, \"clientIds\": [\"5c0e750b7a5cd42464a5099d\"], \"responseDataRowTotalCount\": 4973000, \"transferDataTotalBytes\": 194749360, \"moduleId\": {\"$oid\": \"65fbf59dedf441365e9790f6\"}, \"modules\": {\"_id\": {\"$oid\": \"65fbf59dedf441365e9790f6\"}, \"name\": \"NonStockModel_changelog\", \"status\": \"active\", \"createTime\": {\"$date\": \"2024-03-21T08:53:49.59Z\"}, \"user_id\": \"62bc5008d4958d013d97c7a6\"}}]}";
            Document aggregateReturn = Document.parse(json);
            when(mongoCursor.hasNext()).thenReturn(true);
            when(mongoCursor.next()).thenReturn(aggregateReturn);

            Page<ApiListVo> apiListVoPage = modulesService.apiList(filter, userDetail);

            assertEquals(54, apiListVoPage.getTotal());
            assertEquals(5, apiListVoPage.getItems().size());
        }
    }

    @Nested
    @DisplayName("Method apiDetail test")
    class apiDetailTest {

        private ApiCallStatsService apiCallStatsService;
        private ApiCallService apiCallService;
        private ApplicationService applicationService;
        private ApiCallMinuteStatsService apiCallMinuteStatsService;

        @BeforeEach
        void setUp() {
            apiCallStatsService = mock(ApiCallStatsService.class);
            ReflectionTestUtils.setField(modulesService, "apiCallStatsService", apiCallStatsService);
            apiCallService = mock(ApiCallService.class);
            ReflectionTestUtils.setField(modulesService, "apiCallService", apiCallService);
            applicationService = mock(ApplicationService.class);
            ReflectionTestUtils.setField(modulesService, "applicationService", applicationService);
            apiCallMinuteStatsService = mock(ApiCallMinuteStatsService.class);
            ReflectionTestUtils.setField(modulesService, "apiCallMinuteStatsService", apiCallMinuteStatsService);
        }

        @Test
        @DisplayName("test main process")
        void test1() {
            ApiDetailParam apiDetailParam = new ApiDetailParam();
            apiDetailParam.setId("1");
            apiDetailParam.setGuanluary(5);
            apiDetailParam.setType("latency");
            ApiCallStatsDto apiCallStatsDto = new ApiCallStatsDto() {{
                setClientIds(new HashSet<String>() {{
                    add(new ObjectId().toString());
                }});
                setTransferDataTotalBytes(1000L);
                setTotalResponseTime(10L);
            }};
            when(apiCallStatsService.findOne(any(Query.class))).thenAnswer(invocationOnMock -> {
                Query query = invocationOnMock.getArgument(0);
                assertEquals("{\"moduleId\": \"1\"}", query.getQueryObject().toJson());
                return apiCallStatsDto;
            });
            ApiCallEntity apiCallEntity = new ApiCallEntity(){{
                setResRows(100L);
                setLatency(1000L);
            }};
            when(apiCallService.findOne(any(Query.class))).thenAnswer(invocationOnMock -> {
                Query query = invocationOnMock.getArgument(0);
                assertEquals("{\"allPathId\": \"1\"}", query.getQueryObject().toJson());
                assertEquals("{\"res_rows\": 1, \"latency\": 1}", query.getFieldsObject().toJson());
                assertEquals("{\"_id\": -1}", query.getSortObject().toJson());
                assertEquals(1, query.getLimit());
                return apiCallEntity;
            });
            List<ApplicationDto> applicationDtoList = new ArrayList<>();
            applicationDtoList.add(new ApplicationDto(){{
                setId(new ObjectId(apiCallStatsDto.getClientIds().iterator().next()));
                setName("app");
            }});
            when(applicationService.findAll(any(Query.class))).thenAnswer(invocationOnMock -> {
                Query query = invocationOnMock.getArgument(0);
                assertEquals("{\"id\": {\"$in\": [\"" + apiCallStatsDto.getClientIds().iterator().next() + "\"]}}", query.getQueryObject().toJson());
                assertEquals("{\"name\": 1, \"id\": 1}", query.getFieldsObject().toJson());
                return applicationDtoList;
            });
            List<ApiCallMinuteStatsDto> apiCallMinuteStatsDtoList = new ArrayList<>();
            apiCallMinuteStatsDtoList.add(new ApiCallMinuteStatsDto(){{
                setApiCallTime(Date.from(ZonedDateTime.now(ZoneId.systemDefault()).minusMinutes(1).withSecond(0).withNano(0).toInstant()));
                setTotalResponseTime(1000L);
            }});
            when(apiCallMinuteStatsService.findAll(any(Query.class))).thenReturn(apiCallMinuteStatsDtoList);

            ApiDetailVo apiDetailVo = modulesService.apiDetail(apiDetailParam);
            assertEquals(5, apiDetailVo.getTime().size());
            assertEquals(5, apiDetailVo.getValue().size());
            assertEquals(1000L, apiDetailVo.getValue().get(4));
            assertEquals(10D, apiDetailVo.getResponseTime());
            assertEquals(1000L, apiDetailVo.getTimeConsuming());
            assertEquals(100000D, apiDetailVo.getSpeed());
        }
    }

    @Nested
    @DisplayName("Method pickValue test")
    class pickValueTest {

        private ApiCallMinuteStatsDto apiCallMinuteStatsDto;

        @BeforeEach
        void setUp() {
            apiCallMinuteStatsDto = new ApiCallMinuteStatsDto(){{
                setTotalResponseTime(1L);
                setResponseDataRowTotalCount(2L);
                setResponseTimePerRow(3D);
                setTransferBytePerSecond(4D);
            }};
        }

        @Test
        @DisplayName("type is latency")
        void test1() {
            ApiDetailParam apiDetailParam = new ApiDetailParam();
            apiDetailParam.setType("latency");
            assertEquals(1L, modulesService.pickValue(apiDetailParam, apiCallMinuteStatsDto));
        }

        @Test
        @DisplayName("type is visitTotalLine")
        void test2() {
            ApiDetailParam apiDetailParam = new ApiDetailParam();
            apiDetailParam.setType("visitTotalLine");
            assertEquals(2L, modulesService.pickValue(apiDetailParam, apiCallMinuteStatsDto));
        }

        @Test
        @DisplayName("type is responseTime")
        void test3() {
            ApiDetailParam apiDetailParam = new ApiDetailParam();
            apiDetailParam.setType("responseTime");
            assertEquals(3D, modulesService.pickValue(apiDetailParam, apiCallMinuteStatsDto));
        }

        @Test
        @DisplayName("type is speed")
        void test4() {
            ApiDetailParam apiDetailParam = new ApiDetailParam();
            apiDetailParam.setType("speed");
            assertEquals(4D, modulesService.pickValue(apiDetailParam, apiCallMinuteStatsDto));
        }

        @Test
        @DisplayName("type is other")
        void test5() {
            ApiDetailParam apiDetailParam = new ApiDetailParam();
            apiDetailParam.setType("other");
            assertEquals(0, modulesService.pickValue(apiDetailParam, apiCallMinuteStatsDto));
        }
    }

    @Nested
    class apiDefinitionTest {
        UserDetail userDetail;
        @BeforeEach
        void beforeEach() {
            userDetail = mock(UserDetail.class);
        }
        @Test
        void testApiDefinitionNormal() {
            modulesService = spy(modulesService);
            List<ModulesDto> apis = new ArrayList<>();
            ModulesDto modulesDto = new ModulesDto();
            modulesDto.setConnection(new ObjectId());
            apis.add(modulesDto);
            doReturn(apis).when(modulesService).findAllActiveApi(ModuleStatusEnum.ACTIVE);
            List<DataSourceConnectionDto> dataSourceConnectionDtoList = new ArrayList<>();
            DataSourceConnectionDto dataSourceConnectionDto = new DataSourceConnectionDto();
            Map<String, Object> config = new HashMap<>();
            config.put("isUri", true);
            config.put("ssl", true);
            config.put("uri", "mongodb://root:******@mongo-ssl.internal.tapdata.io:27018/test?authSource=admin&ssl=true");
            config.put("sslKey", "----test key----");
            config.put("sslValidate", true);
            config.put("sslCA", "----test ca----");
            config.put("_connectionType", "source_and_target");
            config.put("id", "677648e54a46a10e04af5446");
            dataSourceConnectionDto.setConfig(config);
            dataSourceConnectionDto.setDatabase_type("MongoDB");
            dataSourceConnectionDtoList.add(dataSourceConnectionDto);
            when(dataSourceService.findAll(any(Query.class))).thenReturn(dataSourceConnectionDtoList);
            DataSourceDefinitionDto definitionDto = new DataSourceDefinitionDto();
            LinkedHashMap<String, Object> properties = new LinkedHashMap<>();
            LinkedHashMap<String, Object> connection = new LinkedHashMap<>();
            LinkedHashMap<String, Object> prop = new LinkedHashMap<>();
            LinkedHashMap<Object, Object> optional = new LinkedHashMap<>();
            optional.put("type","void");
            LinkedHashMap<Object, Object> value = new LinkedHashMap<>();
            LinkedHashMap<Object, Object> value1 = new LinkedHashMap<>();
            value1.put("type","boolean");
            value1.put("apiServerKey","ssl");
            value.put("ssl", value1);
            LinkedHashMap<Object, Object> value2 = new LinkedHashMap<>();
            value2.put("type","string");
            value2.put("apiServerKey","sslKey");
            value.put("sslKey", value2);
            LinkedHashMap<Object, Object> value3 = new LinkedHashMap<>();
            value3.put("type","string");
            value3.put("apiServerKey","sslPass");
            value.put("sslPass", value3);
            LinkedHashMap<Object, Object> value4 = new LinkedHashMap<>();
            value4.put("type","boolean");
            value4.put("apiServerKey","sslValidate");
            value.put("sslValidate", value4);
            LinkedHashMap<Object, Object> value5 = new LinkedHashMap<>();
            value5.put("type","string");
            value5.put("apiServerKey","sslCA");
            value.put("sslCA", value5);
            LinkedHashMap<Object, Object> value6 = new LinkedHashMap<>();
            value6.put("type","object");
            value6.put("apiServerKey","sslCA");
            value.put("sslCA", value6);
            optional.put("properties", value);
            prop.put("OPTIONAL_FIELDS", optional);
            connection.put("type", "object");
            connection.put("properties", prop);
            properties.put("connection", connection);
            definitionDto.setProperties(properties);
            when(dataSourceDefinitionService.getByDataSourceType(dataSourceConnectionDto.getDatabase_type(), userDetail)).thenReturn(definitionDto);
            ApiDefinitionVo actual = modulesService.apiDefinition(userDetail);
            assertEquals(1, actual.getConnections().size());
            assertTrue(actual.getConnections().get(0).getSsl());
            assertEquals("----test ca----", actual.getConnections().get(0).getSslCA());
        }
        @Test
        void testApiDefinitionSimple() {
            modulesService = spy(modulesService);
            List<ModulesDto> apis = new ArrayList<>();
            ModulesDto modulesDto = new ModulesDto();
            modulesDto.setConnection(new ObjectId());
            apis.add(modulesDto);
            doReturn(apis).when(modulesService).findAllActiveApi(ModuleStatusEnum.ACTIVE);
            List<DataSourceConnectionDto> dataSourceConnectionDtoList = new ArrayList<>();
            DataSourceConnectionDto dataSourceConnectionDto = new DataSourceConnectionDto();
            Map<String, Object> config = new HashMap<>();
            config.put("isUri", true);
            config.put("user", "root");
            config.put("password", "123456");
            config.put("host", "127.0.0.1:27017");
            config.put("database", "test");
            config.put("ssl", false);
            config.put("uri", null);
            config.put("_connectionType", "source_and_target");
            config.put("id", "677648e54a46a10e04af5446");
            dataSourceConnectionDto.setConfig(config);
            dataSourceConnectionDto.setDatabase_type("MongoDB");
            dataSourceConnectionDtoList.add(dataSourceConnectionDto);
            when(dataSourceService.findAll(any(Query.class))).thenReturn(dataSourceConnectionDtoList);
            DataSourceDefinitionDto definitionDto = new DataSourceDefinitionDto();
            LinkedHashMap<String, Object> properties = new LinkedHashMap<>();
            LinkedHashMap<String, Object> connection = new LinkedHashMap<>();
            connection.put("properties", new LinkedHashMap<>());
            properties.put("connection", connection);
            definitionDto.setProperties(properties);
            when(dataSourceDefinitionService.getByDataSourceType(dataSourceConnectionDto.getDatabase_type(), userDetail)).thenReturn(definitionDto);
            ApiDefinitionVo actual = modulesService.apiDefinition(userDetail);
            assertEquals(1, actual.getConnections().size());
            assertNull(actual.getConnections().get(0).getSsl());
            assertNull(actual.getConnections().get(0).getSslCA());
        }
    }
}
