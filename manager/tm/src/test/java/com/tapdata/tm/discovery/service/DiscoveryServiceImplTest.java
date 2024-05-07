package com.tapdata.tm.discovery.service;

import com.alibaba.fastjson.JSON;
import com.tapdata.tm.MockJsonUtils;
import com.tapdata.tm.apiServer.dto.ApiServerDto;
import com.tapdata.tm.apiServer.service.ApiServerService;
import com.tapdata.tm.cluster.dto.ClusterStateDto;
import com.tapdata.tm.cluster.dto.SystemInfo;
import com.tapdata.tm.cluster.service.ClusterStateService;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.Tag;
import com.tapdata.tm.commons.schema.bean.SourceDto;
import com.tapdata.tm.commons.task.dto.TaskCollectionObjDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.discovery.bean.*;
import com.tapdata.tm.metadatadefinition.dto.MetadataDefinitionDto;
import com.tapdata.tm.metadatadefinition.service.MetadataDefinitionService;
import com.tapdata.tm.metadatainstance.repository.MetadataInstancesRepository;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.modules.dto.ModulesDto;
import com.tapdata.tm.modules.dto.Param;
import com.tapdata.tm.modules.entity.Path;
import com.tapdata.tm.modules.service.ModulesService;
import com.tapdata.tm.task.repository.TaskCollectionObjRepository;
import com.tapdata.tm.task.service.TaskCollectionObjService;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.worker.dto.WorkerDto;
import com.tapdata.tm.worker.service.WorkerService;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.aggregation.TypedAggregation;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.util.ReflectionTestUtils;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

class DiscoveryServiceImplTest {
    DiscoveryServiceImpl discoveryService;

    public String id = "test";

    public UserDetail userDetail = mock(UserDetail.class);

    @BeforeEach
    void allSetUp() {
        DiscoveryServiceImpl discoveryService1 = new DiscoveryServiceImpl();
        discoveryService = spy(discoveryService1);
    }
    @Nested
    class TaskOverviewTest{
        TaskCollectionObjService mockTaskService;
        ClusterStateService mockClusterStateService;
        WorkerService mockWorkerService;

        @BeforeEach
        void setUp() {
            userDetail = mock(UserDetail.class);
            mockClusterStateService = mock(ClusterStateService.class);
            ReflectionTestUtils.setField(discoveryService, "clusterStateService", mockClusterStateService);
            mockTaskService = mock(TaskCollectionObjService.class);
            ReflectionTestUtils.setField(discoveryService, "taskService", mockTaskService);
            mockWorkerService = mock(WorkerService.class);
            ReflectionTestUtils.setField(discoveryService, "workerService", mockWorkerService);

        }
        @DisplayName("test taskOverview when dag is not null")
        @Test
        void test1(){
            TaskCollectionObjDto taskCollectionObjDto = getMockTaskConnectionsDto();
            when(mockTaskService.findById(MongoUtils.toObjectId(id),userDetail)).thenReturn(taskCollectionObjDto);
            TaskConnectionsDto taskConnectionsDto=mock(TaskConnectionsDto.class);
            List<TaskConnectionsDto> taskConnectionsDtos=new ArrayList<>();
            taskConnectionsDtos.add(taskConnectionsDto);
            doReturn(taskConnectionsDtos).when(discoveryService).getTaskConnection(taskCollectionObjDto.getDag());
            DiscoveryTaskOverviewDto discoveryTaskOverviewDto = discoveryService.taskOverview(id, userDetail);
            assertEquals(2,discoveryTaskOverviewDto.getNodeNum());
        }
        @DisplayName("test taskOverview when dag is null")
        @Test
        void test2(){
            UserDetail userDetail = mock(UserDetail.class);
            TaskCollectionObjDto taskCollectionObjDto = getMockTaskConnectionsDto();
            taskCollectionObjDto.setDag(null);
            when(mockTaskService.findById(MongoUtils.toObjectId(id),userDetail)).thenReturn(taskCollectionObjDto);
            TaskConnectionsDto taskConnectionsDto=mock(TaskConnectionsDto.class);
            List<TaskConnectionsDto> taskConnectionsDtos=new ArrayList<>();
            taskConnectionsDtos.add(taskConnectionsDto);
            doReturn(taskConnectionsDtos).when(discoveryService).getTaskConnection(taskCollectionObjDto.getDag());
            DiscoveryTaskOverviewDto discoveryTaskOverviewDto = discoveryService.taskOverview(id, userDetail);
            assertEquals(0,discoveryTaskOverviewDto.getNodeNum());
        }
        @DisplayName("test taskOverView when taskDto AgentId is not null and cluster is not null")
        @Test
        void test3() {
            TaskCollectionObjDto taskCollectionObjDto = getMockTaskConnectionsDto();
            taskCollectionObjDto.setAgentId("testAgentId");
            when(mockTaskService.findById(MongoUtils.toObjectId(id), userDetail)).thenReturn(taskCollectionObjDto);
            TaskConnectionsDto taskConnectionsDto = mock(TaskConnectionsDto.class);
            List<TaskConnectionsDto> taskConnectionsDtos = new ArrayList<>();
            taskConnectionsDtos.add(taskConnectionsDto);
            doReturn(taskConnectionsDtos).when(discoveryService).getTaskConnection(taskCollectionObjDto.getDag());
            ClusterStateDto clusterStateDto = new ClusterStateDto();
            SystemInfo systemInfo = mock(SystemInfo.class);
            when(systemInfo.getIp()).thenReturn("192.168.1.1");
            when(systemInfo.getHostname()).thenReturn("tapdata");
            clusterStateDto.setSystemInfo(systemInfo);
            when(mockClusterStateService.findOne(any())).thenReturn(clusterStateDto);
            DiscoveryTaskOverviewDto discoveryTaskOverviewDto = discoveryService.taskOverview(id, userDetail);
            assertEquals("192.168.1.1", discoveryTaskOverviewDto.getAgentDesc());
        }
        @DisplayName("test taskOverView when taskDto AgentId is not null and cluster null")
        @Test
        void test4() {
            TaskCollectionObjDto taskCollectionObjDto = getMockTaskConnectionsDto();
            taskCollectionObjDto.setAgentId("testAgentId");
            when(mockTaskService.findById(MongoUtils.toObjectId(id), userDetail)).thenReturn(taskCollectionObjDto);
            TaskConnectionsDto taskConnectionsDto = mock(TaskConnectionsDto.class);
            List<TaskConnectionsDto> taskConnectionsDtos = new ArrayList<>();
            taskConnectionsDtos.add(taskConnectionsDto);
            doReturn(taskConnectionsDtos).when(discoveryService).getTaskConnection(taskCollectionObjDto.getDag());
            WorkerDto workerDto = mock(WorkerDto.class);
            when(workerDto.getHostname()).thenReturn("tapdata");
            when(mockWorkerService.findOne(any(Query.class), any(UserDetail.class))).thenReturn(workerDto);
            DiscoveryTaskOverviewDto discoveryTaskOverviewDto = discoveryService.taskOverview(id, userDetail);
            assertEquals("tapdata", discoveryTaskOverviewDto.getAgentName());
        }
        protected TaskCollectionObjDto getMockTaskConnectionsDto() {
            TaskCollectionObjDto taskCollectionObjDto = new TaskCollectionObjDto();
            Date mockDate = new Date();
            taskCollectionObjDto.setCreateAt(mockDate);
            taskCollectionObjDto.setLastUpdAt(mockDate);
            DAG mockDag = mock(DAG.class);
            taskCollectionObjDto.setDag(mockDag);
            Node mock1 = mock(Node.class);
            Node mock2 = mock(Node.class);
            List<Node> mockNodeList = new ArrayList<>();
            mockNodeList.add(mock1);
            mockNodeList.add(mock2);
            when(mockDag.getNodes()).thenReturn(mockNodeList);
            return taskCollectionObjDto;
        }
    }
    @Nested
    class testApiOverview{
        ModulesService modulesService;
        @BeforeEach
        void setUp(){
            modulesService = mock(ModulesService.class);
            ReflectionTestUtils.setField(discoveryService, "modulesService", modulesService);
        }
        @DisplayName("test apiOverView path is not empty")
        @Test
        void test1(){
            ModulesDto modulesDto = new ModulesDto();
            modulesDto.setName("Api Test Name");
            Param param = new Param();
            param.setName("testParam");
            List<Param> params = new ArrayList<>();
            params.add(param);
            Path path = new Path();
            path.setParams(params);
            List<Path> paths = new ArrayList<>();
            paths.add(path);
            modulesDto.setPaths(paths);
            Field field=new Field();
            field.setFieldName("testFieldName");
            List<Field> fields = new ArrayList<>();
            fields.add(field);
            modulesDto.setFields(fields);
            when(modulesService.findById(MongoUtils.toObjectId(id), userDetail)).thenReturn(modulesDto);
            doReturn("testClientId").when(discoveryService).getClientURI(userDetail);
            DiscoveryApiOverviewDto discoveryApiOverviewDto = discoveryService.apiOverview(id, userDetail);
            assertEquals(1,discoveryApiOverviewDto.getInputParamNum());
        }
        @DisplayName("test apiOverView path is empty ")
        @Test
        void test2(){
            ModulesDto modulesDto = new ModulesDto();
            modulesDto.setName("Api Test Name");
            List<Path> paths = new ArrayList<>();
            modulesDto.setPaths(paths);
            Field field=new Field();
            field.setFieldName("testFieldName");
            List<Field> fields = new ArrayList<>();
            fields.add(field);
            modulesDto.setFields(fields);
            when(modulesService.findById(MongoUtils.toObjectId(id), userDetail)).thenReturn(modulesDto);
            doReturn("testClientId").when(discoveryService).getClientURI(userDetail);
            DiscoveryApiOverviewDto discoveryApiOverviewDto = discoveryService.apiOverview(id, userDetail);
            assertEquals(4,discoveryApiOverviewDto.getInputParamNum());
        }
    }
    @Nested
    class testGetClientURI{
        ApiServerService apiServerService;
        @BeforeEach
        void setUp(){
            apiServerService=mock(ApiServerService.class);
            ReflectionTestUtils.setField(discoveryService,"apiServerService",apiServerService);

        }
        @Test
        void test1(){
            ApiServerDto apiServerDto = new ApiServerDto();
            apiServerDto.setClientURI("://test");
            when(apiServerService.findOne(any(Query.class),any(UserDetail.class))).thenReturn(apiServerDto);
            String clientURI = discoveryService.getClientURI(userDetail);
            assertEquals("test",clientURI);
        }
    }
    @Nested
    class filterListTest{
        MetadataInstancesRepository mockMetadataInstancesRepository;
        TaskCollectionObjRepository mockTaskRepository;
        @BeforeEach
        void setUp(){
            mockMetadataInstancesRepository = mock(MetadataInstancesRepository.class);
            ReflectionTestUtils.setField(discoveryService,"metaDataRepository",mockMetadataInstancesRepository);
            mockTaskRepository = mock(TaskCollectionObjRepository.class);
            ReflectionTestUtils.setField(discoveryService,"taskRepository",mockTaskRepository);
        }
        @Test
        void test1(){
            List<ObjectFilterEnum> objectFilterEnums = new ArrayList<>();
            objectFilterEnums.add(ObjectFilterEnum.objCategory);
            List<String> objCategoryNameEnum =  new ArrayList<>();
            objCategoryNameEnum.add("storage");
            objCategoryNameEnum.add("job");
            objCategoryNameEnum.add("api");
            Map<ObjectFilterEnum, List<String>> objectFilterEnumListMap = discoveryService.filterList(objectFilterEnums, userDetail);
            assertEquals(objCategoryNameEnum,objectFilterEnumListMap.get(ObjectFilterEnum.objCategory));
        }
        @Test
        void test2(){
            List<ObjectFilterEnum> objectFilterEnums = new ArrayList<>();
            objectFilterEnums.add(ObjectFilterEnum.objType);
            List objCategoryName = new ArrayList();
            objCategoryName.add("database");
            when(mockMetadataInstancesRepository.findDistinct(any(), any(), any(), any())).thenReturn(objCategoryName);
            Map<ObjectFilterEnum, List<String>> objectFilterEnumListMap = discoveryService.filterList(objectFilterEnums, userDetail);
            assertEquals(2,objectFilterEnumListMap.get(ObjectFilterEnum.objType).size());
        }
        @Test
        void test3(){
            List<ObjectFilterEnum> objectFilterEnums = new ArrayList<>();
            objectFilterEnums.add(ObjectFilterEnum.sourceCategory);
            Map<ObjectFilterEnum, List<String>> objectFilterEnumListMap = discoveryService.filterList(objectFilterEnums, userDetail);
            assertEquals(3,objectFilterEnumListMap.get(ObjectFilterEnum.sourceCategory).size());
        }
        @Test
        void test4(){
            List<ObjectFilterEnum> objectFilterEnums = new ArrayList<>();
            objectFilterEnums.add(ObjectFilterEnum.itemType);
            List<String> itemTypeList=new ArrayList<>();
            itemTypeList.add("resource");
            itemTypeList.add("task");
            Map<ObjectFilterEnum, List<String>> objectFilterEnumListMap = discoveryService.filterList(objectFilterEnums, userDetail);
            assertEquals(itemTypeList,objectFilterEnumListMap.get(ObjectFilterEnum.itemType));
        }
        @DisplayName("test filter sourceType List")
        @Test
        void test5(){
            String mysqlType ="mysql";
            List mysqlTypeList=new ArrayList();
            mysqlTypeList.add(mysqlType);
            when(mockMetadataInstancesRepository.findDistinct(any(), any(), any(), any())).thenReturn(mysqlTypeList);
            List agentIdList=new ArrayList<>();
            String agentId="testAgentId";
            agentIdList.add(agentId);
            when(mockTaskRepository.findDistinct(any(), any(), any(), any())).thenReturn(agentIdList);
            List<ObjectFilterEnum> objectFilterEnums = new ArrayList<>();
            objectFilterEnums.add(ObjectFilterEnum.sourceType);
            Map<ObjectFilterEnum, List<String>> objectFilterEnumListMap = discoveryService.filterList(objectFilterEnums, userDetail);
            assertEquals(2,objectFilterEnumListMap.get(ObjectFilterEnum.sourceType).size());
        }
    }
    @DisplayName("test listtags and allTag is empty")
    @Test
    void testGetUpdate1(){
        Update update = DiscoveryServiceImpl.getUpdate(new ArrayList<>(), new ArrayList<>(), null, false);
        Document setObject = (Document) update.getUpdateObject().get("$set");
        List<Tag> listtags = setObject.getList("listtags", Tag.class);
        assertEquals(0,listtags.size());
    }
    @DisplayName("test getUpdate param add is false")
    @Test
    void testGetUpdate2(){
        Tag tag1 = new Tag("test1", "test1");
        Tag tag2 = new Tag("test2", "test2");
        List<Tag> listTags=new ArrayList<>();
        listTags.add(tag1);
        listTags.add(tag2);
        List<Tag> allTags=new ArrayList<>();
        allTags.add(tag1);
        allTags.add(tag2);
        Update update = DiscoveryServiceImpl.getUpdate(allTags, new ArrayList<>(), listTags, false);
        Document setObject = (Document) update.getUpdateObject().get("$set");
        List<Tag> listtags = setObject.getList("listtags", Tag.class);
        assertEquals(0,listtags.size());
    }
    @DisplayName("test getUpdate param add is true and old tag is empty,listTags contain allTag")
    @Test
    void testGetUpdate3(){
        Tag tag1 = new Tag("test1", "test1");
        Tag tag2 = new Tag("test2", "test2");
        List<Tag> listTags=new ArrayList<>();
        listTags.add(tag1);
        listTags.add(tag2);
        List<Tag> allTags=new ArrayList<>();
        allTags.add(tag1);
        allTags.add(tag2);
        Update update = DiscoveryServiceImpl.getUpdate(allTags, new ArrayList<>(), listTags, true);
        Document setObject = (Document) update.getUpdateObject().get("$set");
        List<Tag> listtags = setObject.getList("listtags", Tag.class);
        assertEquals(2,listtags.size());
    }
    @DisplayName("test getUpdate param add is true and old tag is empty,listTags no contain allTag")
    @Test
    void testGetUpdate4(){
        Tag tag1 = new Tag("test1", "test1");
        Tag tag2 = new Tag("test2", "test2");
        Tag tag3 = new Tag("test3", "test2");
        List<Tag> listTags=new ArrayList<>();
        listTags.add(tag1);
        listTags.add(tag2);
        List<Tag> allTags=new ArrayList<>();
        allTags.add(tag1);
        allTags.add(tag2);
        allTags.add(tag3);
        Update update = DiscoveryServiceImpl.getUpdate(allTags, new ArrayList<>(), listTags, true);
        Document setObject = (Document) update.getUpdateObject().get("$set");
        List<Tag> listtags = setObject.getList("listtags", Tag.class);
        assertEquals(3,listtags.size());
    }
    @DisplayName("test getUpdate param add is true and old tag is empty,listTags no contain allTag,and oldTagIds is not Empty")
    @Test
    void testGetUpdate5(){
        Tag tag1 = new Tag("test1", "test1");
        Tag tag2 = new Tag("test2", "test2");
        Tag tag3 = new Tag("test3", "test2");
        List<Tag> listTags=new ArrayList<>();
        listTags.add(tag1);
        listTags.add(tag2);
        listTags.add(tag3);
        List<Tag> allTags=new ArrayList<>();
        allTags.add(tag1);
        allTags.add(tag2);
        List<String> oldListTags=new ArrayList<>();
        oldListTags.add("test3");
        Update update = DiscoveryServiceImpl.getUpdate(allTags,oldListTags, listTags, true);
        Document setObject = (Document) update.getUpdateObject().get("$set");
        List<Tag> listtags = setObject.getList("listtags", Tag.class);
        assertEquals(2,listtags.size());
    }
    @Nested
    class TapTypeStringTest{
        public final Locale localeChina = Locale.CHINA;

        @DisplayName("test get Array message")
        @Test
        void test1(){
            Map<String,Byte> typeMap=new HashMap<>();
            typeMap.put("type", (byte) 2);
            String jsonString = JSON.toJSONString(typeMap);
            String tapTypeString = discoveryService.tapTypeString(jsonString, localeChina);
            assertEquals("数组",tapTypeString);
        }
        @DisplayName("test get Binary message")
        @Test
        void test2(){
            Map<String,Byte> typeMap=new HashMap<>();
            typeMap.put("type", (byte) 3);
            String jsonString = JSON.toJSONString(typeMap);
            String tapTypeString = discoveryService.tapTypeString(jsonString, localeChina);
            assertEquals("布尔值",tapTypeString);
        }
        @DisplayName("test get Binary message")
        @Test
        void test3(){
            Map<String,Byte> typeMap=new HashMap<>();
            typeMap.put("type", (byte) 9);
            String jsonString = JSON.toJSONString(typeMap);
            String tapTypeString = discoveryService.tapTypeString(jsonString, localeChina);
            System.out.println(tapTypeString);
            assertEquals("字节数组",tapTypeString);
        }
        @DisplayName("test get Date message")
        @Test
        void test4(){
            Map<String,Byte> typeMap=new HashMap<>();
            typeMap.put("type", (byte) 11);
            String jsonString = JSON.toJSONString(typeMap);
            String tapTypeString = discoveryService.tapTypeString(jsonString, localeChina);
            assertEquals("日期",tapTypeString);
        }
        @DisplayName("test get DateTime message")
        @Test
        void test5(){
            Map<String,Byte> typeMap=new HashMap<>();
            typeMap.put("type", (byte) 1);
            String jsonString = JSON.toJSONString(typeMap);
            String tapTypeString = discoveryService.tapTypeString(jsonString, localeChina);
            assertEquals("日期时间",tapTypeString);
        }
        @DisplayName("test get Map message")
        @Test
        void test6(){
            Map<String,Byte> typeMap=new HashMap<>();
            typeMap.put("type", (byte) 4);
            String jsonString = JSON.toJSONString(typeMap);
            String tapTypeString = discoveryService.tapTypeString(jsonString, localeChina);
            assertEquals("映射",tapTypeString);
        }
        @DisplayName("test get String message")
        @Test
        void test7(){
            Map<String,Byte> typeMap=new HashMap<>();
            typeMap.put("type", (byte)10);
            String jsonString = JSON.toJSONString(typeMap);
            String tapTypeString = discoveryService.tapTypeString(jsonString, localeChina);
            System.out.println(tapTypeString);
            assertEquals("字符串",tapTypeString);
        }
        @DisplayName("test get Time message")
        @Test
        void test8(){
            Map<String,Byte> typeMap=new HashMap<>();
            typeMap.put("type", (byte)6);
            String jsonString = JSON.toJSONString(typeMap);
            String tapTypeString = discoveryService.tapTypeString(jsonString, localeChina);
            System.out.println(tapTypeString);
            assertEquals("时间",tapTypeString);
        }
        @DisplayName("test get Year message")
        @Test
        void test9(){
            Map<String,Byte> typeMap=new HashMap<>();
            typeMap.put("type", (byte)5);
            String jsonString = JSON.toJSONString(typeMap);
            String tapTypeString = discoveryService.tapTypeString(jsonString, localeChina);
            assertEquals("日期（年）",tapTypeString);
        }
        @DisplayName("test get Unknow message")
        @Test
        void test10(){
            Map<String,Byte> typeMap=new HashMap<>();
            typeMap.put("type", (byte)13);
            String jsonString = JSON.toJSONString(typeMap);
            String tapTypeString = discoveryService.tapTypeString(jsonString, localeChina);
            assertEquals("未知",tapTypeString);
        }
        @DisplayName("test get Number message")
        @Test
        void test11(){
            Map<String,Byte> typeMap=new HashMap<>();
            typeMap.put("type", (byte)8);
            String jsonString = JSON.toJSONString(typeMap);
            String tapTypeString = discoveryService.tapTypeString(jsonString, localeChina);
            assertEquals("数值",tapTypeString);
        }
    }

    @Nested
    class GetDefaultObjEnumTest {
        MetadataDefinitionService mockMetadataDefinitionService;
        ObjectId testId = MongoUtils.toObjectId("6510f74ca270a1cf5533d1b2");
        List<String> itemType;

        @BeforeEach
        void beforeSetUp() {
            mockMetadataDefinitionService = mock(MetadataDefinitionService.class);
            ReflectionTestUtils.setField(discoveryService, "metadataDefinitionService", mockMetadataDefinitionService);
            itemType = new ArrayList<>();
        }
        @DisplayName("test when metadataDefinitionDtoMap is null and itemType is storage")
        @Test
        void test1() {
            MetadataDefinitionDto metadataDefinitionDto = new MetadataDefinitionDto();
            metadataDefinitionDto.setId(testId);
            itemType.add("storage");
            metadataDefinitionDto.setItemType(itemType);
            when(mockMetadataDefinitionService.findById(testId)).thenReturn(metadataDefinitionDto);
            DataObjCategoryEnum defaultObjEnum = discoveryService.getDefaultObjEnum(null, testId);
            assertEquals(DataObjCategoryEnum.storage,defaultObjEnum);
        }
        @DisplayName("test when metadataDefinitionDtoMap is null and itemType is job")
        @Test
        void test2() {
            MetadataDefinitionDto metadataDefinitionDto = new MetadataDefinitionDto();
            metadataDefinitionDto.setId(testId);
            itemType.add("job");
            metadataDefinitionDto.setItemType(itemType);
            when(mockMetadataDefinitionService.findById(testId)).thenReturn(metadataDefinitionDto);
            DataObjCategoryEnum defaultObjEnum = discoveryService.getDefaultObjEnum(null, testId);
            assertEquals(DataObjCategoryEnum.job,defaultObjEnum);
        }
        @DisplayName("test when metadataDefinitionDtoMap is null and itemType is job")
        @Test
        void test3() {
            MetadataDefinitionDto metadataDefinitionDto = new MetadataDefinitionDto();
            metadataDefinitionDto.setId(testId);
            itemType.add("apis");
            metadataDefinitionDto.setItemType(itemType);
            when(mockMetadataDefinitionService.findById(testId)).thenReturn(metadataDefinitionDto);
            DataObjCategoryEnum defaultObjEnum = discoveryService.getDefaultObjEnum(null, testId);
            assertEquals(DataObjCategoryEnum.api,defaultObjEnum);
        }
        @DisplayName("test when metadataDefinitionDtoMap is null and itemType is job")
        @Test
        void test4() {
            MetadataDefinitionDto metadataDefinitionDto = new MetadataDefinitionDto();
            metadataDefinitionDto.setId(testId);
            metadataDefinitionDto.setItemType(itemType);
            when(mockMetadataDefinitionService.findById(testId)).thenReturn(metadataDefinitionDto);
            DataObjCategoryEnum defaultObjEnum = discoveryService.getDefaultObjEnum(null, testId);
            assertEquals(DataObjCategoryEnum.storage,defaultObjEnum);
        }

        @DisplayName("test when metadataDefinitionDtoMap is null and itemType is job")
        @Test
        void test5() {
            Map<ObjectId, MetadataDefinitionDto> metadataDefinitionDtoMap =new HashMap<>();
            String parentId = "6510f74ca270a1cf5533d1b3";
            ObjectId parentObjId = MongoUtils.toObjectId(parentId);
            MetadataDefinitionDto metadataDefinitionDto = new MetadataDefinitionDto();
            metadataDefinitionDto.setParent_id(parentId);
            metadataDefinitionDto.setId(testId);
            metadataDefinitionDto.setItemType(itemType);
            metadataDefinitionDtoMap.put(testId,metadataDefinitionDto);
            MetadataDefinitionDto parentMetadataDefinitionDto = new MetadataDefinitionDto();
            List<String> parentItemType=new ArrayList<>();
            parentItemType.add("apis");
            parentMetadataDefinitionDto.setId(parentObjId);
            parentMetadataDefinitionDto.setItemType(parentItemType);
            metadataDefinitionDtoMap.put(parentObjId,parentMetadataDefinitionDto);
            DataObjCategoryEnum defaultObjEnum = discoveryService.getDefaultObjEnum(metadataDefinitionDtoMap, testId);
            assertEquals(DataObjCategoryEnum.api, defaultObjEnum);
        }
    }
    @Nested
    class AddListTagsTest {
        MetadataDefinitionService metadataDefinitionService;
        List<TagBindingParam> tagBindingParamList;

        String id = "6510f74ca270a1cf5533d1b3";

        @BeforeEach
        void beforeSetUp() {
            metadataDefinitionService = mock(MetadataDefinitionService.class);
            ReflectionTestUtils.setField(discoveryService, "metadataDefinitionService", metadataDefinitionService);
            MetadataDefinitionDto metadataDefinitionDto = new MetadataDefinitionDto();
            metadataDefinitionDto.setId(MongoUtils.toObjectId(id));
            List<MetadataDefinitionDto> metadataDefinitionDtos = new ArrayList<>();
            metadataDefinitionDtos.add(metadataDefinitionDto);
            when(metadataDefinitionService.findAll(any(Query.class))).thenReturn(metadataDefinitionDtos);
            tagBindingParamList = new ArrayList<>();
        }
        @DisplayName("test addListTags when ObjCategory is storage ")
        @Test
        void test1() {
            TagBindingParam tagBindingParam = new TagBindingParam();
            tagBindingParam.setId(id);
            tagBindingParam.setObjCategory(DataObjCategoryEnum.storage);
            tagBindingParamList.add(tagBindingParam);
            MetadataInstancesDto metadataInstancesDto = mock(MetadataInstancesDto.class);
            MetadataInstancesService metadataInstancesService = mock(MetadataInstancesService.class);
            when(metadataInstancesService.findById(any(ObjectId.class), any(com.tapdata.tm.base.dto.Field.class))).thenReturn(metadataInstancesDto);
            ReflectionTestUtils.setField(discoveryService, "metadataInstancesService", metadataInstancesService);
            discoveryService.addListTags(tagBindingParamList, Arrays.asList(id), null, userDetail, false);
            verify(metadataInstancesService,times(1)).updateById(any(ObjectId.class),any(Update.class),any(UserDetail.class));
        }
        @DisplayName("test addListTags when ObjCategory is job ")
        @Test
        void test2(){
            TagBindingParam tagBindingParam = new TagBindingParam();
            tagBindingParam.setId(id);
            tagBindingParam.setObjCategory(DataObjCategoryEnum.job);
            tagBindingParamList.add(tagBindingParam);

            TaskCollectionObjService taskCollectionObjService = mock(TaskCollectionObjService.class);
            ReflectionTestUtils.setField(discoveryService, "taskService", taskCollectionObjService);

            TaskCollectionObjDto taskCollectionObjDto = mock(TaskCollectionObjDto.class);
            when(taskCollectionObjService.findById(any(ObjectId.class),any(com.tapdata.tm.base.dto.Field.class))).thenReturn(taskCollectionObjDto);
            discoveryService.addListTags(tagBindingParamList, Arrays.asList(id), null, userDetail, false);
            verify(taskCollectionObjService,times(1)).updateById(any(ObjectId.class),any(Update.class),any(UserDetail.class));
        }
        @DisplayName("test addListTags when ObjCategory is api ")
        @Test
        void test3(){
            TagBindingParam tagBindingParam = new TagBindingParam();
            tagBindingParam.setId(id);
            tagBindingParam.setObjCategory(DataObjCategoryEnum.api);
            tagBindingParamList.add(tagBindingParam);
            ModulesService modulesService = mock(ModulesService.class);
            ReflectionTestUtils.setField(discoveryService, "modulesService", modulesService);
            ModulesDto modulesDto = mock(ModulesDto.class);
            when(modulesService.findById(any(ObjectId.class),any(com.tapdata.tm.base.dto.Field.class))).thenReturn(modulesDto);
            discoveryService.addListTags(tagBindingParamList, Arrays.asList(id), null, userDetail, false);
            verify(modulesService,times(1)).updateById(any(ObjectId.class),any(Update.class),any(UserDetail.class));
        }
    }

    @Nested
    class ConvertToDataDiscoveryTest {
        List<Tag> listTags;
        List<MetadataDefinitionDto> metadataDefinitionDtos;

        @BeforeEach
        void beforeSetUp() {
            String id1 = "6510f74ca270a1cf5533d1b4";
            String id2 = "6510f74ca270a1cf5533d1b5";
            Tag tag1 = new Tag(id1, "test1");
            Tag tag2 = new Tag(id2, "test2");
            listTags = new ArrayList<>();
            listTags.add(tag1);
            listTags.add(tag2);
            metadataDefinitionDtos = new ArrayList<>();
            MetadataDefinitionDto metadataDefinitionDto = new MetadataDefinitionDto();
            metadataDefinitionDto.setId(MongoUtils.toObjectId(id1));
            metadataDefinitionDto.setValue("test1");
            MetadataDefinitionDto metadataDefinitionDto1 = new MetadataDefinitionDto();
            metadataDefinitionDto1.setId(MongoUtils.toObjectId(id2));
            metadataDefinitionDto1.setValue("test2");
            metadataDefinitionDtos.add(metadataDefinitionDto);
            metadataDefinitionDtos.add(metadataDefinitionDto1);
            MetadataDefinitionService metadataDefinitionService = mock(MetadataDefinitionService.class);
            ReflectionTestUtils.setField(discoveryService, "metadataDefinitionService", metadataDefinitionService);
            when(metadataDefinitionService.findAndParent(any(),any())).thenReturn(metadataDefinitionDtos);
        }
        @DisplayName("test convertToDataDiscovery two params when SyncType is not blank")
        @Test
        void test1(){
            UnionQueryResult unionQueryResult = new UnionQueryResult();
            unionQueryResult.setSyncType("cdc");
            unionQueryResult.set_id(MongoUtils.toObjectId("6510f74ca270a1cf5533d1b3"));
            unionQueryResult.setListtags(listTags);
            DataDiscoveryDto dataDiscoveryDto = discoveryService.convertToDataDiscovery(unionQueryResult, null);
            assertEquals(2,dataDiscoveryDto.getAllTags().size());
            assertEquals(DataObjCategoryEnum.job,dataDiscoveryDto.getCategory());
        }
        @DisplayName("test convertToDataDiscovery two params when syncType and Meta_type is blank")
        @Test
        void test2(){
            UnionQueryResult unionQueryResult = new UnionQueryResult();
            unionQueryResult.set_id(MongoUtils.toObjectId("6510f74ca270a1cf5533d1b3"));
            unionQueryResult.setListtags(listTags);
            DataDiscoveryDto dataDiscoveryDto = discoveryService.convertToDataDiscovery(unionQueryResult, null);
            assertEquals(2,dataDiscoveryDto.getAllTags().size());
            assertEquals(DataObjCategoryEnum.api,dataDiscoveryDto.getCategory());
        }
        @DisplayName("test convertToDataDiscovery two params Meta_type is not blank")
        @Test
        void test3(){
            Map<String, DataSourceConnectionDto> connectionMap =new HashMap<>();
            connectionMap.put("6510f74ca270a1cf5533d1b3",new DataSourceConnectionDto());
            UnionQueryResult unionQueryResult = new UnionQueryResult();
            com.tapdata.tm.commons.schema.bean.SourceDto sourceDto = new SourceDto();
            sourceDto.set_id("6510f74ca270a1cf5533d1b3");
            unionQueryResult.setSource(sourceDto);
            unionQueryResult.setOriginal_name("mysql");
            unionQueryResult.set_id(MongoUtils.toObjectId("6510f74ca270a1cf5533d1b3"));
            unionQueryResult.setListtags(listTags);
            unionQueryResult.setMeta_type("database");
            doReturn("sourceInfo").when(discoveryService).getConnectInfo(any(),any());
            DataDiscoveryDto dataDiscoveryDto = discoveryService.convertToDataDiscovery(unionQueryResult, connectionMap);
            assertEquals(2,dataDiscoveryDto.getAllTags().size());
            assertEquals(DataObjCategoryEnum.storage,dataDiscoveryDto.getCategory());
        }
        @DisplayName("test convertToDataDiscovery one params when SyncType is not blank")
        @Test
        void test4(){
            UnionQueryResult unionQueryResult = new UnionQueryResult();
            unionQueryResult.setSyncType("cdc");
            unionQueryResult.set_id(MongoUtils.toObjectId("6510f74ca270a1cf5533d1b3"));
            unionQueryResult.setListtags(listTags);
            DataDirectoryDto dataDirectoryDto = discoveryService.convertToDataDirectory(unionQueryResult);
            assertEquals(2,dataDirectoryDto.getAllTags().size());
            assertEquals(DataObjCategoryEnum.job,dataDirectoryDto.getCategory());
        }

        @DisplayName("test convertToDataDiscovery one params when syncType and Meta_type is blank")
        @Test
        void test5() {
            UnionQueryResult unionQueryResult = new UnionQueryResult();
            unionQueryResult.set_id(MongoUtils.toObjectId("6510f74ca270a1cf5533d1b3"));
            unionQueryResult.setListtags(listTags);
            DataDirectoryDto dataDirectoryDto = discoveryService.convertToDataDirectory(unionQueryResult);
            assertEquals(2, dataDirectoryDto.getAllTags().size());
            assertEquals(DataObjCategoryEnum.api, dataDirectoryDto.getCategory());
        }
        @DisplayName("test convertToDataDiscovery two params Meta_type is not blank")
        @Test
        void test6(){
            UnionQueryResult unionQueryResult = new UnionQueryResult();
            unionQueryResult.set_id(MongoUtils.toObjectId("6510f74ca270a1cf5533d1b3"));
            unionQueryResult.setListtags(listTags);
            unionQueryResult.setMeta_type("database");
            DataDirectoryDto dataDirectoryDto = discoveryService.convertToDataDirectory(unionQueryResult);
            assertEquals(2,dataDirectoryDto.getAllTags().size());
            assertEquals(DataObjCategoryEnum.storage,dataDirectoryDto.getCategory());
        }
    }

    @Nested
    class AddObjCountTest {
        MetadataDefinitionService metadataDefinitionService;
        MetadataInstancesService metadataInstancesService;
        TaskCollectionObjRepository taskRepository;
        ModulesService modulesService;
        MongoTemplate mongoOperation;
        public final String LINK_ID = "6510f74ca270a1cf5533link";
        public final String DefinitionId = "6510f74ca270a1cf5533d1b3";
        public final String PARENT_ID="6510f74ca270a1cf5533d1b4";
        public final String USER_ID="userId";

        @BeforeEach
        void allSetUp() {
            metadataDefinitionService = mock(MetadataDefinitionService.class);
            taskRepository = mock(TaskCollectionObjRepository.class);
            mongoOperation = mock(MongoTemplate.class);
            metadataInstancesService = mock(MetadataInstancesService.class);
            modulesService = mock(ModulesService.class);
            ReflectionTestUtils.setField(discoveryService, "taskRepository", taskRepository);
            ReflectionTestUtils.setField(discoveryService,"metadataInstancesService",metadataInstancesService);
            ReflectionTestUtils.setField(discoveryService,"modulesService",modulesService);

            when(taskRepository.getMongoOperations()).thenReturn(mongoOperation);
        }
        @DisplayName("test method addObjCount when aggregate result is not null and is Default is false")
        @Test
        void test1() {
            List<MetadataDefinitionDto> tagDtos = new ArrayList<>();
            MetadataDefinitionDto metadataDefinitionDto = new MetadataDefinitionDto();
            metadataDefinitionDto.setId(MongoUtils.toObjectId(DefinitionId));
            metadataDefinitionDto.setParent_id(PARENT_ID);
            List<String> itemTypes = new ArrayList<>();
            itemTypes.add("storage");
            metadataDefinitionDto.setItemType(itemTypes);
            tagDtos.add(metadataDefinitionDto);

            when(metadataDefinitionService.findAllDto(any(Query.class),any())).thenReturn(tagDtos);
            ReflectionTestUtils.setField(discoveryService, "metadataDefinitionService", metadataDefinitionService);

            AggregationResults<GroupMetadata> metadataResults= mock(AggregationResults.class);
            List<GroupMetadata> metadataInstanceResultsList=new ArrayList<>();
            GroupMetadata metadataGroupMetadata = new GroupMetadata();
            metadataGroupMetadata.set_id(LINK_ID);
            metadataGroupMetadata.setCount(2);
            metadataInstanceResultsList.add(metadataGroupMetadata);
            when(metadataResults.getMappedResults()).thenReturn(metadataInstanceResultsList);
            doReturn(metadataResults).when(mongoOperation).aggregate(any(Aggregation.class), eq("MetadataInstances"), any());

            AggregationResults<GroupMetadata> taskResults= mock(AggregationResults.class);
            List<GroupMetadata> taskResultsList=new ArrayList<>();
            GroupMetadata taskGroupMetadata = new GroupMetadata();
            taskGroupMetadata.set_id(LINK_ID);
            taskGroupMetadata.setCount(2);
            taskResultsList.add(taskGroupMetadata);
            when(taskResults.getMappedResults()).thenReturn(taskResultsList);
            doReturn(taskResults).when(mongoOperation).aggregate(any(Aggregation.class), eq("TaskCollectionObj"), any());

            when(metadataInstancesService.count(any(Query.class),eq(userDetail))).thenReturn(1L);
            when(taskRepository.count(any(Query.class),eq(userDetail))).thenReturn(2L);
            when(modulesService.count(any(Query.class),eq(userDetail))).thenReturn(3L);
            discoveryService.addObjCount(tagDtos, userDetail);
            assertEquals(6L,metadataDefinitionDto.getObjCount());
        }
        @DisplayName("test method addObjCount when aggregate result is null and is Default is false")
        @Test
        void test2(){
            List<MetadataDefinitionDto> tagDtos = new ArrayList<>();
            MetadataDefinitionDto metadataDefinitionDto = new MetadataDefinitionDto();
            metadataDefinitionDto.setId(MongoUtils.toObjectId(DefinitionId));
            metadataDefinitionDto.setParent_id(PARENT_ID);
            List<String> itemTypes = new ArrayList<>();
            itemTypes.add("storage");
            metadataDefinitionDto.setItemType(itemTypes);
            tagDtos.add(metadataDefinitionDto);

            when(metadataDefinitionService.findAllDto(any(Query.class),any())).thenReturn(tagDtos);
            ReflectionTestUtils.setField(discoveryService, "metadataDefinitionService", metadataDefinitionService);

            AggregationResults<GroupMetadata> metadataResults= mock(AggregationResults.class);
            when(metadataResults.getMappedResults()).thenReturn(null);
            doReturn(metadataResults).when(mongoOperation).aggregate(any(Aggregation.class), eq("MetadataInstances"), any());

            AggregationResults<GroupMetadata> taskResults= mock(AggregationResults.class);
            when(taskResults.getMappedResults()).thenReturn(null);
            doReturn(taskResults).when(mongoOperation).aggregate(any(Aggregation.class), eq("TaskCollectionObj"), any());

            when(metadataInstancesService.count(any(Query.class),eq(userDetail))).thenReturn(1L);
            when(taskRepository.count(any(Query.class),eq(userDetail))).thenReturn(2L);
            when(modulesService.count(any(Query.class),eq(userDetail))).thenReturn(3L);
            discoveryService.addObjCount(tagDtos, userDetail);
            assertEquals(6L,metadataDefinitionDto.getObjCount());
        }
        @DisplayName("test method addObjCount when aggregate result is not null, default is true,tag metadataDefinitionDto Value is not root,type is storage")
        @Test
        void test3(){
            List<MetadataDefinitionDto> tagDtos = new ArrayList<>();
            MetadataDefinitionDto metadataDefinitionDto = new MetadataDefinitionDto();
            metadataDefinitionDto.setId(MongoUtils.toObjectId(DefinitionId));
            metadataDefinitionDto.setParent_id(PARENT_ID);
            metadataDefinitionDto.setValue("user");
            metadataDefinitionDto.setLinkId(LINK_ID);
            List<String> itemTypes = new ArrayList<>();
            itemTypes.add("storage");
            itemTypes.add("default");
            metadataDefinitionDto.setItemType(itemTypes);
            tagDtos.add(metadataDefinitionDto);

            when(metadataDefinitionService.findAllDto(any(Query.class),any())).thenReturn(tagDtos);
            when(metadataDefinitionService.findAndChild(eq(null),any(MetadataDefinitionDto.class),any())).thenReturn(tagDtos);
            ReflectionTestUtils.setField(discoveryService, "metadataDefinitionService", metadataDefinitionService);

            AggregationResults<GroupMetadata> metadataResults= mock(AggregationResults.class);
            List<GroupMetadata> metadataInstanceResultsList=new ArrayList<>();
            GroupMetadata metadataGroupMetadata = new GroupMetadata();
            metadataGroupMetadata.set_id(LINK_ID);
            metadataGroupMetadata.setCount(2);
            metadataInstanceResultsList.add(metadataGroupMetadata);
            when(metadataResults.getMappedResults()).thenReturn(metadataInstanceResultsList);
            doReturn(metadataResults).when(mongoOperation).aggregate(any(Aggregation.class), eq("MetadataInstances"), any());

            AggregationResults<GroupMetadata> taskResults= mock(AggregationResults.class);
            List<GroupMetadata> taskResultsList=new ArrayList<>();
            GroupMetadata taskGroupMetadata = new GroupMetadata();
            taskGroupMetadata.set_id(LINK_ID);
            taskGroupMetadata.setCount(2);
            taskResultsList.add(taskGroupMetadata);
            when(taskResults.getMappedResults()).thenReturn(taskResultsList);
            doReturn(taskResults).when(mongoOperation).aggregate(any(Aggregation.class), eq("TaskCollectionObj"), any());

            discoveryService.addObjCount(tagDtos, userDetail);
            assertEquals(2L,metadataDefinitionDto.getObjCount());
        }
        @DisplayName("test method addObjCount when aggregate result is not null,itemType default、apis,metadataDefinitionDto value is not root,user is root ")
        @Test
        void test4(){
            List<MetadataDefinitionDto> tagDtos = new ArrayList<>();
            MetadataDefinitionDto metadataDefinitionDto = new MetadataDefinitionDto();
            metadataDefinitionDto.setId(MongoUtils.toObjectId(DefinitionId));
            metadataDefinitionDto.setParent_id(PARENT_ID);
            metadataDefinitionDto.setValue("user");
            List<String> itemTypes = new ArrayList<>();
            itemTypes.add("apis");
            itemTypes.add("default");
            metadataDefinitionDto.setItemType(itemTypes);
            tagDtos.add(metadataDefinitionDto);

            when(userDetail.isRoot()).thenReturn(true);
            when(modulesService.count(any(Query.class),eq(userDetail))).thenReturn(1L);
            when(metadataDefinitionService.findAllDto(any(Query.class),any())).thenReturn(tagDtos);
            when(metadataDefinitionService.findAndChild(eq(null),any(MetadataDefinitionDto.class),any())).thenReturn(tagDtos);
            ReflectionTestUtils.setField(discoveryService, "metadataDefinitionService", metadataDefinitionService);

            AggregationResults<GroupMetadata> metadataResults= mock(AggregationResults.class);
            List<GroupMetadata> metadataInstanceResultsList=new ArrayList<>();
            GroupMetadata metadataGroupMetadata = new GroupMetadata();
            metadataGroupMetadata.setCount(2);
            metadataInstanceResultsList.add(metadataGroupMetadata);
            when(metadataResults.getMappedResults()).thenReturn(metadataInstanceResultsList);
            doReturn(metadataResults).when(mongoOperation).aggregate(any(Aggregation.class), eq("MetadataInstances"), any());

            AggregationResults<GroupMetadata> taskResults= mock(AggregationResults.class);
            List<GroupMetadata> taskResultsList=new ArrayList<>();
            GroupMetadata taskGroupMetadata = new GroupMetadata();
            taskGroupMetadata.set_id(LINK_ID);
            taskGroupMetadata.setCount(2);
            taskResultsList.add(taskGroupMetadata);
            when(taskResults.getMappedResults()).thenReturn(taskResultsList);
            doReturn(taskResults).when(mongoOperation).aggregate(any(Aggregation.class), eq("TaskCollectionObj"), any());

            discoveryService.addObjCount(tagDtos, userDetail);
            assertEquals(1L,metadataDefinitionDto.getObjCount());
        }
        @DisplayName("test method addObjCount when aggregate result is not null,itemType default、apis,metadataDefinitionDto value is not root,user is not Root")
        @Test
        void test5(){
            List<MetadataDefinitionDto> tagDtos = new ArrayList<>();
            MetadataDefinitionDto metadataDefinitionDto = new MetadataDefinitionDto();
            metadataDefinitionDto.setId(MongoUtils.toObjectId(DefinitionId));
            metadataDefinitionDto.setParent_id(PARENT_ID);
            metadataDefinitionDto.setValue("user");
            List<String> itemTypes = new ArrayList<>();
            itemTypes.add("apis");
            itemTypes.add("default");
            metadataDefinitionDto.setItemType(itemTypes);
            tagDtos.add(metadataDefinitionDto);

            when(userDetail.isRoot()).thenReturn(false);
            when(modulesService.count(any(Query.class),eq(userDetail))).thenReturn(3L);
            when(metadataDefinitionService.findAllDto(any(Query.class),any())).thenReturn(tagDtos);
            when(metadataDefinitionService.findAndChild(eq(null),any(MetadataDefinitionDto.class),any())).thenReturn(tagDtos);
            ReflectionTestUtils.setField(discoveryService, "metadataDefinitionService", metadataDefinitionService);

            AggregationResults<GroupMetadata> metadataResults= mock(AggregationResults.class);
            List<GroupMetadata> metadataInstanceResultsList=new ArrayList<>();
            GroupMetadata metadataGroupMetadata = new GroupMetadata();
            metadataGroupMetadata.setCount(2);
            metadataInstanceResultsList.add(metadataGroupMetadata);
            when(metadataResults.getMappedResults()).thenReturn(metadataInstanceResultsList);
            doReturn(metadataResults).when(mongoOperation).aggregate(any(Aggregation.class), eq("MetadataInstances"), any());

            AggregationResults<GroupMetadata> taskResults= mock(AggregationResults.class);
            List<GroupMetadata> taskResultsList=new ArrayList<>();
            GroupMetadata taskGroupMetadata = new GroupMetadata();
            taskGroupMetadata.set_id(LINK_ID);
            taskGroupMetadata.setCount(2);
            taskResultsList.add(taskGroupMetadata);
            when(taskResults.getMappedResults()).thenReturn(taskResultsList);
            doReturn(taskResults).when(mongoOperation).aggregate(any(Aggregation.class), eq("TaskCollectionObj"), any());

            discoveryService.addObjCount(tagDtos, userDetail);
            assertEquals(3L,metadataDefinitionDto.getObjCount());
        }
        @DisplayName("test method addObjCount when aggregate result is not null,itemType default、job,metadataDefinitionDto value is migrate,user is not Root")
        @Test
        void test6(){
            List<MetadataDefinitionDto> tagDtos = new ArrayList<>();
            MetadataDefinitionDto metadataDefinitionDto = new MetadataDefinitionDto();
            metadataDefinitionDto.setId(MongoUtils.toObjectId(DefinitionId));
            metadataDefinitionDto.setParent_id(PARENT_ID);
            metadataDefinitionDto.setValue(TaskDto.SYNC_TYPE_MIGRATE);
            metadataDefinitionDto.setUserId(USER_ID);
            List<String> itemTypes = new ArrayList<>();
            itemTypes.add("job");
            itemTypes.add("default");
            metadataDefinitionDto.setItemType(itemTypes);
            tagDtos.add(metadataDefinitionDto);


            userDetail.setUserId(USER_ID);
            when(metadataDefinitionService.findAllDto(any(Query.class),any())).thenReturn(tagDtos);
            when(metadataDefinitionService.findAndChild(eq(null),any(MetadataDefinitionDto.class),any())).thenReturn(tagDtos);
            ReflectionTestUtils.setField(discoveryService, "metadataDefinitionService", metadataDefinitionService);

            AggregationResults<GroupMetadata> metadataResults= mock(AggregationResults.class);
            List<GroupMetadata> metadataInstanceResultsList=new ArrayList<>();
            GroupMetadata metadataGroupMetadata = new GroupMetadata();
            metadataGroupMetadata.setCount(2);
            metadataInstanceResultsList.add(metadataGroupMetadata);
            when(metadataResults.getMappedResults()).thenReturn(metadataInstanceResultsList);
            doReturn(metadataResults).when(mongoOperation).aggregate(any(Aggregation.class), eq("MetadataInstances"), any());

            AggregationResults<GroupMetadata> taskResults= mock(AggregationResults.class);
            List<GroupMetadata> taskResultsList=new ArrayList<>();
            GroupMetadata taskGroupMetadata = new GroupMetadata();
            taskGroupMetadata.set_id(USER_ID);
            taskGroupMetadata.setCount(2);
            taskResultsList.add(taskGroupMetadata);
            when(taskResults.getMappedResults()).thenReturn(taskResultsList);
            doReturn(taskResults).when(mongoOperation).aggregate(any(Aggregation.class), eq("TaskCollectionObj"), any());

            discoveryService.addObjCount(tagDtos, userDetail);
            assertEquals(2L,metadataDefinitionDto.getObjCount());
        }
        @DisplayName("test method addObjCount when throw exception")
        @Test
        void test7(){
            List<MetadataDefinitionDto> tagDtos = new ArrayList<>();
            MetadataDefinitionDto metadataDefinitionDto = new MetadataDefinitionDto();
            metadataDefinitionDto.setId(MongoUtils.toObjectId(DefinitionId));
            metadataDefinitionDto.setParent_id(PARENT_ID);
            metadataDefinitionDto.setLinkId(LINK_ID);
            metadataDefinitionDto.setValue("Root");
            List<String> itemTypes = new ArrayList<>();
            itemTypes.add("job");
            itemTypes.add("default");
            metadataDefinitionDto.setItemType(itemTypes);
            tagDtos.add(metadataDefinitionDto);

            userDetail.setUserId(USER_ID);
            when(metadataDefinitionService.findAllDto(any(Query.class),any())).thenReturn(tagDtos);
            when(metadataDefinitionService.findAndChild(eq(null),any(MetadataDefinitionDto.class),any())).thenAnswer(invocationOnMock -> {
                throw new RuntimeException("count faild");
            });
            ReflectionTestUtils.setField(discoveryService, "metadataDefinitionService", metadataDefinitionService);

            AggregationResults<GroupMetadata> metadataResults= mock(AggregationResults.class);
            List<GroupMetadata> metadataInstanceResultsList=new ArrayList<>();
            GroupMetadata metadataGroupMetadata = new GroupMetadata();
            metadataGroupMetadata.set_id(LINK_ID);
            metadataGroupMetadata.setCount(2);
            metadataInstanceResultsList.add(metadataGroupMetadata);
            when(metadataResults.getMappedResults()).thenReturn(metadataInstanceResultsList);
            doReturn(metadataResults).when(mongoOperation).aggregate(any(Aggregation.class), eq("MetadataInstances"), any());

            AggregationResults<GroupMetadata> taskResults= mock(AggregationResults.class);
            List<GroupMetadata> taskResultsList=new ArrayList<>();
            GroupMetadata taskGroupMetadata = new GroupMetadata();
            taskGroupMetadata.set_id(USER_ID);
            taskGroupMetadata.setCount(2);
            taskResultsList.add(taskGroupMetadata);
            when(taskResults.getMappedResults()).thenReturn(taskResultsList);
            doReturn(taskResults).when(mongoOperation).aggregate(any(Aggregation.class), eq("TaskCollectionObj"), any());
            assertEquals(0,metadataDefinitionDto.getObjCount());
        }
    }
}
