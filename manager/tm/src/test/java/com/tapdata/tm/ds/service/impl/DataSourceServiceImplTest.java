package com.tapdata.tm.ds.service.impl;


import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.Settings.constant.SettingUtil;
import com.tapdata.tm.agent.service.AgentGroupService;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.dag.AccessNodeTypeEnum;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.task.dto.ImportModeEnum;
import com.tapdata.tm.commons.util.MetaDataBuilderUtils;
import com.tapdata.tm.commons.util.MetaType;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.discovery.service.DefaultDataDirectoryService;
import com.tapdata.tm.ds.entity.DataSourceEntity;
import com.tapdata.tm.ds.repository.DataSourceRepository;
import com.tapdata.tm.messagequeue.dto.MessageQueueDto;
import com.tapdata.tm.messagequeue.service.MessageQueueService;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.metadatainstance.vo.SourceTypeEnum;
import com.tapdata.tm.permissions.DataPermissionHelper;
import com.tapdata.tm.report.service.UserDataReportService;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import com.tapdata.tm.externalStorage.service.ExternalStorageService;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.*;
import org.mockito.Answers;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class DataSourceServiceImplTest {
    DataSourceServiceImpl dataSourceService;
    @Nested
    class RestoreAccessNodeTypeTest{
        DataSourceConnectionDto updateDto;
        DataSourceConnectionDto connectionDto;
        List<String> accessNodeProcessIdList;
        @BeforeEach
        void init() {
            dataSourceService = mock(DataSourceServiceImpl.class);

            updateDto = mock(DataSourceConnectionDto.class);
            connectionDto = mock(DataSourceConnectionDto.class);
            accessNodeProcessIdList = new ArrayList<>();
            accessNodeProcessIdList.add("");

            when(connectionDto.getAccessNodeType()).thenReturn("");
            doNothing().when(updateDto).setAccessNodeType(anyString());
            //doNothing().when(updateDto).setAccessNodeProcessId(anyString());
            doNothing().when(updateDto).setAccessNodeProcessIdList(accessNodeProcessIdList);
            doNothing().when(updateDto).setAccessNodeTypeEmpty(false);

            when(updateDto.isAccessNodeTypeEmpty()).thenReturn(true);

            doCallRealMethod().when(dataSourceService).restoreAccessNodeType(updateDto, connectionDto, accessNodeProcessIdList);
        }
        @Test
        void testNormal() {
            assertDoesNotThrow(() -> dataSourceService.restoreAccessNodeType(updateDto, connectionDto, accessNodeProcessIdList));
            verify(connectionDto, times(1)).getAccessNodeType();
            verify(updateDto, times(1)).setAccessNodeType(anyString());
            //verify(updateDto, times(1)).setAccessNodeProcessId(anyString());
            verify(updateDto, times(1)).setAccessNodeProcessIdList(accessNodeProcessIdList);
            verify(updateDto, times(1)).setAccessNodeTypeEmpty(false);
            verify(updateDto, times(1)).isAccessNodeTypeEmpty();
        }
        @Test
        void testNotIsAccessNodeTypeEmpty() {
            when(updateDto.isAccessNodeTypeEmpty()).thenReturn(false);
            assertDoesNotThrow(() -> dataSourceService.restoreAccessNodeType(updateDto, connectionDto, accessNodeProcessIdList));
            verify(connectionDto, times(0)).getAccessNodeType();
            verify(updateDto, times(0)).setAccessNodeType(anyString());
            //verify(updateDto, times(0)).setAccessNodeProcessId(anyString());
            verify(updateDto, times(0)).setAccessNodeProcessIdList(accessNodeProcessIdList);
            verify(updateDto, times(0)).setAccessNodeTypeEmpty(false);
            verify(updateDto, times(1)).isAccessNodeTypeEmpty();
        }
        @Test
        void testAccessNodeProcessIdListIsEmpty() {
            accessNodeProcessIdList.remove(0);
            assertDoesNotThrow(() -> dataSourceService.restoreAccessNodeType(updateDto, connectionDto, accessNodeProcessIdList));
            verify(connectionDto, times(0)).getAccessNodeType();
            verify(updateDto, times(0)).setAccessNodeType(anyString());
            //verify(updateDto, times(0)).setAccessNodeProcessId(anyString());
            verify(updateDto, times(0)).setAccessNodeProcessIdList(accessNodeProcessIdList);
            verify(updateDto, times(0)).setAccessNodeTypeEmpty(false);
            verify(updateDto, times(1)).isAccessNodeTypeEmpty();
        }

    }
    @Nested
    class Copy{
        DataSourceRepository dataSourceRepository = mock(DataSourceRepository.class);
        DataSourceServiceImpl dataSourceService = new DataSourceServiceImpl(dataSourceRepository);
        WorkerService workerService;
        @BeforeEach
        void before(){
            workerService = mock(WorkerService.class);
            ReflectionTestUtils.setField(dataSourceService,"workerService",workerService);
            ReflectionTestUtils.setField(dataSourceService,"defaultDataDirectoryService",mock(DefaultDataDirectoryService.class));
        }
        @Test
        void test(){
            try(MockedStatic<SettingUtil> settingUtilMockedStatic = Mockito.mockStatic(SettingUtil.class);
                MockedStatic<DataPermissionHelper> dataPermissionHelperMockedStatic = Mockito.mockStatic(DataPermissionHelper.class)){
                settingUtilMockedStatic.when(()->SettingUtil.getValue(anyString(),anyString())).thenReturn("true");
                DataSourceEntity dataSourceEntity = new DataSourceEntity();
                dataSourceEntity.setConfig(new HashMap<>());
                when(dataSourceRepository.findById(any(),any(UserDetail.class))).thenReturn(Optional.of(dataSourceEntity));
                when(dataSourceRepository.save(any(),any())).thenReturn(mock(DataSourceEntity.class));
                dataPermissionHelperMockedStatic.when(()->DataPermissionHelper.convert(any(),any())).thenAnswer(invocationOnMock -> {
                    return null;
                });
                when(workerService.findAvailableAgent(any())).thenReturn(null);
                DataSourceConnectionDto result = dataSourceService.copy(mock(UserDetail.class),new ObjectId().toHexString(),"test");
                Assertions.assertNull(result.getConfig());
            }

        }
    }

    @Nested
    class add{
        DataSourceRepository dataSourceRepository = mock(DataSourceRepository.class);
        DataSourceServiceImpl dataSourceService = mock(DataSourceServiceImpl.class, Answers.CALLS_REAL_METHODS);
        WorkerService workerService;
        @BeforeEach
        void before(){
            workerService = mock(WorkerService.class);
            ReflectionTestUtils.setField(dataSourceService,"workerService",workerService);
            ReflectionTestUtils.setField(dataSourceService,"repository",dataSourceRepository);
            ReflectionTestUtils.setField(dataSourceService,"defaultDataDirectoryService",mock(DefaultDataDirectoryService.class));
        }
        @Test
        void test() {
            UserDataReportService userDataReportService = mock(UserDataReportService.class);
            ReflectionTestUtils.setField(dataSourceService,"userDataReportService",userDataReportService);
            doNothing().when(dataSourceService).beforeSave(any(),any());
            when(dataSourceRepository.save(any(),any())).thenReturn(mock(DataSourceEntity.class));
            DataSourceConnectionDto result = dataSourceService.add(new DataSourceConnectionDto(),mock(UserDetail.class));
            Assertions.assertNull(result.getConfig());
        }
    }


    @Nested
    class AssertProcessNodeTest {
        DataSourceServiceImpl instance;
        String nodeType;
        String accessProcessId;
        Collection<String> processNodeListWithGroup;
        @BeforeEach
        void init() {
            instance = mock(DataSourceServiceImpl.class);
        }
        @Test
        void testNormal() {
            nodeType = AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.name();
            accessProcessId = null;
            processNodeListWithGroup = mock(Collection.class);
            doCallRealMethod().when(instance).assertProcessNode(nodeType, accessProcessId, processNodeListWithGroup);
            Assertions.assertDoesNotThrow(() -> instance.assertProcessNode(nodeType, accessProcessId, processNodeListWithGroup));
            verify(processNodeListWithGroup, times(0)).isEmpty();
        }

        @Test
        void testIsGroupManuallyAndAccessProcessIdIsBlank() {
            nodeType = AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP.name();
            accessProcessId = null;
            processNodeListWithGroup = mock(Collection.class);
            doCallRealMethod().when(instance).assertProcessNode(nodeType, accessProcessId, processNodeListWithGroup);
            Assertions.assertThrows(BizException.class, () -> instance.assertProcessNode(nodeType, accessProcessId, processNodeListWithGroup));
            verify(processNodeListWithGroup, times(0)).isEmpty();
        }
        @Test
        void testIsGroupManuallyButAccessProcessIdNotBlank() {
            nodeType = AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP.name();
            accessProcessId = "id";
            processNodeListWithGroup = mock(Collection.class);
            doCallRealMethod().when(instance).assertProcessNode(nodeType, accessProcessId, processNodeListWithGroup);
            Assertions.assertDoesNotThrow(() -> instance.assertProcessNode(nodeType, accessProcessId, processNodeListWithGroup));
            verify(processNodeListWithGroup, times(0)).isEmpty();
        }

        @Test
        void testProcessNodeListWithGroupIsEmpty() {
            nodeType = AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name();
            accessProcessId = null;
            processNodeListWithGroup = mock(Collection.class);
            when(processNodeListWithGroup.isEmpty()).thenReturn(true);
            doCallRealMethod().when(instance).assertProcessNode(nodeType, accessProcessId, processNodeListWithGroup);
            Assertions.assertThrows(BizException.class, () -> instance.assertProcessNode(nodeType, accessProcessId, processNodeListWithGroup));
            verify(processNodeListWithGroup, times(1)).isEmpty();
        }

        @Test
        void testProcessNodeListWithGroupNotEmpty() {
            nodeType = AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name();
            accessProcessId = null;
            processNodeListWithGroup = mock(Collection.class);
            when(processNodeListWithGroup.isEmpty()).thenReturn(false);
            doCallRealMethod().when(instance).assertProcessNode(nodeType, accessProcessId, processNodeListWithGroup);
            Assertions.assertDoesNotThrow( () -> instance.assertProcessNode(nodeType, accessProcessId, processNodeListWithGroup));
            verify(processNodeListWithGroup, times(1)).isEmpty();
        }
    }

    @Nested
    @DisplayName("Method flushDatabaseMetadataInstanceLastUpdate test")
    class flushDatabaseMetadataInstanceLastUpdateTest {

        private DataSourceRepository dataSourceRepository;

        private MetadataInstancesService metadataInstancesService;
        private UserDetail userDetail;
        private long time;
        private String connectionId;

        @BeforeEach
        void setUp() {
            dataSourceRepository = mock(DataSourceRepository.class);
            metadataInstancesService = mock(MetadataInstancesService.class);
            dataSourceService = spy(new DataSourceServiceImpl(dataSourceRepository));
            ReflectionTestUtils.setField(dataSourceService,"metadataInstancesService",metadataInstancesService);
            userDetail = mock(UserDetail.class);
            time = new Date().getTime();
            connectionId = new ObjectId().toHexString();
        }

        @Test
        @DisplayName("test main process")
        void testMainProcess() {
            try (
                    MockedStatic<MetaDataBuilderUtils> metaDataBuilderUtilsMockedStatic = mockStatic(MetaDataBuilderUtils.class)
            ){
                DataSourceConnectionDto dataSourceConnectionDto = new DataSourceConnectionDto();
                doReturn(dataSourceConnectionDto).when(dataSourceService).findById(any(ObjectId.class), eq(userDetail));
                String qualifiedName = "test-qualified-name";
                metaDataBuilderUtilsMockedStatic.when(() -> MetaDataBuilderUtils.generateQualifiedName(MetaType.database.name(), dataSourceConnectionDto, null)).thenReturn(qualifiedName);
                assertDoesNotThrow(() -> dataSourceService.flushDatabaseMetadataInstanceLastUpdate("finished", connectionId, time, userDetail));
                verify(metadataInstancesService, times(1)).update(any(Query.class), any(Update.class), eq(userDetail));
            }

        }

        @Test
        @DisplayName("test field status is not finished")
        void testFieldStatusNotFinished() {
            assertDoesNotThrow(()->dataSourceService.flushDatabaseMetadataInstanceLastUpdate("loading", connectionId, time, userDetail));

            verify(dataSourceRepository, never()).update(any(Query.class), any(Update.class), eq(userDetail));
        }

        @Test
        @DisplayName("test input null field status")
        void testInputNullFieldStatus() {
            assertDoesNotThrow(()->dataSourceService.flushDatabaseMetadataInstanceLastUpdate(null, connectionId, time, userDetail));

            verify(dataSourceRepository, never()).update(any(Query.class), any(Update.class), eq(userDetail));
        }

        @Test
        @DisplayName("test input empty/null connectionId")
        void testInputEmptyConnectionId() {
            assertDoesNotThrow(() -> dataSourceService.flushDatabaseMetadataInstanceLastUpdate("finished", "", time, userDetail));
            assertDoesNotThrow(() -> dataSourceService.flushDatabaseMetadataInstanceLastUpdate("finished", null, time, userDetail));

            verify(dataSourceRepository, never()).update(any(Query.class), any(Update.class), eq(userDetail));
        }

        @Test
        @DisplayName("test input null, 0L, negative lastUpdate")
        void testInputInvalidLastUpdate() {
            assertDoesNotThrow(() -> dataSourceService.flushDatabaseMetadataInstanceLastUpdate("finished", connectionId, -1L, userDetail));
            assertDoesNotThrow(() -> dataSourceService.flushDatabaseMetadataInstanceLastUpdate("finished", connectionId, 0L, userDetail));
            assertDoesNotThrow(() -> dataSourceService.flushDatabaseMetadataInstanceLastUpdate("finished", connectionId, null, userDetail));

            verify(dataSourceRepository, never()).update(any(Query.class), any(Update.class), eq(userDetail));
        }

        @Test
        @DisplayName("test input null userDetail")
        void inputNullUserDetail() {
            assertDoesNotThrow(() -> dataSourceService.flushDatabaseMetadataInstanceLastUpdate("finished", connectionId, time, null));

            verify(dataSourceRepository, never()).update(any(Query.class), any(Update.class), eq(userDetail));
        }

        @Test
        @DisplayName("test findById return null")
        void testFindByIdReturnNull() {
            doReturn(null).when(dataSourceService).findById(any(ObjectId.class), eq(userDetail));
            assertDoesNotThrow(() -> dataSourceService.flushDatabaseMetadataInstanceLastUpdate("finished", connectionId, time, null));
            verify(dataSourceRepository, never()).update(any(Query.class), any(Update.class), eq(userDetail));
        }
    }

    @Nested
    @DisplayName("Method hiddenMqPasswd test")
    class hiddenMqPasswdTest {
        private DataSourceServiceImpl dataSourceService;
        private DataSourceRepository dataSourceRepository;

        @BeforeEach
        void setUp() {
            dataSourceRepository = mock(DataSourceRepository.class);
            dataSourceService = spy(new DataSourceServiceImpl(dataSourceRepository));
        }

        @Test
        void testHiddenMqPasswd_NullOrEmpty() {
            DataSourceConnectionDto dto = null;
            doReturn(false).when(dataSourceService).isAgentReq();
            dataSourceService.hiddenMqPasswd(dto);

            dto = new DataSourceConnectionDto();
            dataSourceService.hiddenMqPasswd(dto);
            Assertions.assertNull(dto.getConfig(), "Config should be null when DataSourceConnectionDto is empty");

            dto.setConfig(new HashMap<>());
            dataSourceService.hiddenMqPasswd(dto);
            Assertions.assertTrue(dto.getConfig().isEmpty(), "Config should be empty when DataSourceConnectionDto is empty");
        }
        @Test
        void testHiddenMqPasswd_WithMongoUri() {
            String originalUri = "mongodb://user:password@localhost:27017/db";
            Map<String, Object> config = new HashMap<>();
            config.put("uri", originalUri);

            DataSourceConnectionDto dto = new DataSourceConnectionDto();
            dto.setConfig(config);
            dto.setDatabase_type("mongo");
            doReturn(false).when(dataSourceService).isAgentReq();
            dataSourceService.hiddenMqPasswd(dto);

            Assertions.assertEquals("mongodb://user:******@localhost:27017/db", dto.getConfig().get("uri"),
                    "MongoDB URI password should be masked");
        }

        @Test
        void testHiddenMqPasswd_WithMongoAtlasUri() {
            String originalUri = "mongodb+srv://tapdata:123456@test/test";
            Map<String, Object> config = new HashMap<>();
            config.put("uri", originalUri);

            DataSourceConnectionDto dto = new DataSourceConnectionDto();
            dto.setConfig(config);
            dto.setDatabase_type("mongo");
            doReturn(false).when(dataSourceService).isAgentReq();
            dataSourceService.hiddenMqPasswd(dto);

            Assertions.assertEquals("mongodb+srv://tapdata:******@test/test", dto.getConfig().get("uri"),
                    "MongoDB URI password should be masked");
        }

        @Test
        void testHiddenMqPasswd_WithMongoAtlasUriError() {
            String originalUri = "test";
            Map<String, Object> config = new HashMap<>();
            config.put("uri", originalUri);

            DataSourceConnectionDto dto = new DataSourceConnectionDto();
            dto.setConfig(config);
            dto.setDatabase_type("mongo");
            doReturn(false).when(dataSourceService).isAgentReq();
            dataSourceService.hiddenMqPasswd(dto);

            Assertions.assertEquals("test", dto.getConfig().get("uri"),
                    "MongoDB URI password should be masked");
        }
    }
    @Nested
    @DisplayName("Method sendTestConnection test")
    class sendTestConnectionTest{
        private DataSourceServiceImpl dataSourceService;
        private DataSourceRepository dataSourceRepository;
        private WorkerService workerService;
        private AgentGroupService agentGroupService;
        private MessageQueueService messageQueueService;

        @BeforeEach
        void setUp() {
            dataSourceRepository = mock(DataSourceRepository.class);
            workerService = mock(WorkerService.class);
            agentGroupService = mock(AgentGroupService.class);
            messageQueueService = mock(MessageQueueService.class);
            dataSourceService = spy(new DataSourceServiceImpl(dataSourceRepository));
            ReflectionTestUtils.setField(dataSourceService, "workerService", workerService);
            ReflectionTestUtils.setField(dataSourceService, "agentGroupService", agentGroupService);
            ReflectionTestUtils.setField(dataSourceService, "messageQueueService", messageQueueService);
        }
        @Test
        void test_MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP(){
            DataSourceConnectionDto connectionDto = new DataSourceConnectionDto();
            connectionDto.setName("test");
            connectionDto.setAccessNodeType("MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP");
            connectionDto.setPriorityProcessId("work_2");
            List<Worker> workers = new ArrayList<>();
            Worker worker1 = new Worker();
            worker1.setProcessId("work_1");
            Worker worker2 = new Worker();
            worker2.setProcessId("work_2");
            workers.add(worker1);
            workers.add(worker2);
            when(workerService.findAvailableAgentByAccessNode(any(),any())).thenReturn(workers);
            doAnswer(invocationOnMock -> {
                MessageQueueDto queueDto = invocationOnMock.getArgument(0);
                Assertions.assertEquals("work_2",queueDto.getReceiver());
                return null;
            }).when(messageQueueService).sendMessage(any());
            dataSourceService.sendTestConnection(connectionDto,true,true,mock(UserDetail.class));
        }

        @Test
        void test_MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP_PriorityProcessWorkIsInaction(){
            DataSourceConnectionDto connectionDto = new DataSourceConnectionDto();
            connectionDto.setName("test");
            connectionDto.setAccessNodeType("MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP");
            connectionDto.setPriorityProcessId("work_2");
            List<Worker> workers = new ArrayList<>();
            Worker worker1 = new Worker();
            worker1.setProcessId("work_1");
            workers.add(worker1);
            when(workerService.findAvailableAgentByAccessNode(any(),any())).thenReturn(workers);
            doAnswer(invocationOnMock -> {
                MessageQueueDto queueDto = invocationOnMock.getArgument(0);
                Assertions.assertEquals("work_1",queueDto.getReceiver());
                return null;
            }).when(messageQueueService).sendMessage(any());
            dataSourceService.sendTestConnection(connectionDto,true,true,mock(UserDetail.class));
        }



        @Test
        void test_MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP_PriorityProcessIdIsNull(){
            DataSourceConnectionDto connectionDto = new DataSourceConnectionDto();
            connectionDto.setName("test");
            connectionDto.setAccessNodeType("MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP");
            List<Worker> workers = new ArrayList<>();
            Worker worker1 = new Worker();
            worker1.setProcessId("work_1");
            Worker worker2 = new Worker();
            worker2.setProcessId("work_2");
            workers.add(worker1);
            workers.add(worker2);
            when(workerService.findAvailableAgentByAccessNode(any(),any())).thenReturn(workers);
            doAnswer(invocationOnMock -> {
                MessageQueueDto queueDto = invocationOnMock.getArgument(0);
                Assertions.assertEquals("work_1",queueDto.getReceiver());
                return null;
            }).when(messageQueueService).sendMessage(any());
            dataSourceService.sendTestConnection(connectionDto,true,true,mock(UserDetail.class));
        }
    }


    @Nested
    class UpdatePartialSchemaTest {
        DataSourceRepository dataSourceRepository;
        MetadataInstancesService metadataInstancesService;
        UserDetail userDetail;

        Long lastUpdate;
        String connId = "test-conn-id";
        String loadFieldsStatus = "finished";
        String fromSchemaVersion = "test-from-version";
        String toSchemaVersion = "test-to-version";

        @BeforeEach
        void setUp() {
            dataSourceRepository = mock(DataSourceRepository.class);
            metadataInstancesService = mock(MetadataInstancesService.class);
            dataSourceService = spy(new DataSourceServiceImpl(dataSourceRepository));
            ReflectionTestUtils.setField(dataSourceService, "metadataInstancesService", metadataInstancesService);
            userDetail = mock(UserDetail.class);
            lastUpdate = System.currentTimeMillis();
        }

        @Test
        void testUnChangeConnection() {
            String filters = null;
            doAnswer(invocation -> {
                Document doc = invocation.<Query>getArgument(0).getQueryObject();
                assertEquals(connId, doc.getString("_id"));
                assertEquals(fromSchemaVersion, doc.getString(DataSourceConnectionDto.FIELD_SCHEMA_VERSION));

                doc = invocation.<Update>getArgument(1).getUpdateObject().get("$set", Document.class);
                assertEquals(toSchemaVersion, doc.getString(DataSourceConnectionDto.FIELD_SCHEMA_VERSION));
                assertEquals(lastUpdate, doc.getLong(DataSourceConnectionDto.FIELD_LAST_UPDATE));
                assertEquals(loadFieldsStatus, doc.getString(DataSourceConnectionDto.FIELD_LOAD_FIELDS_STATUS));
                return null;
            }).when(dataSourceRepository).findAndModify(any(Query.class), any(Update.class), eq(userDetail));

            // 没更新到连接时，不更新模型状态
            long count = dataSourceService.updatePartialSchema(connId, loadFieldsStatus, lastUpdate, fromSchemaVersion, toSchemaVersion, filters, userDetail);
            assertEquals(0, count);
        }

        @Test
        void testChangeConnectionAndNullFilter() {
            String filters = null;

            DataSourceEntity dataSourceEntity = mock(DataSourceEntity.class);
            when(dataSourceRepository.findAndModify(any(Query.class), any(Update.class), eq(userDetail))).thenReturn(dataSourceEntity);

            UpdateResult updateResult = mock(UpdateResult.class);
            doAnswer(invocation -> {
                Document doc = invocation.<Query>getArgument(0).getQueryObject();
                assertEquals(connId, doc.getString("source._id"));
                assertEquals("{\"$ne\": true}", doc.get("is_deleted", Document.class).toJson());
                assertEquals(MetaType.table, doc.get("meta_type"));
                assertEquals(SourceTypeEnum.SOURCE, doc.get("sourceType"));
                assertEquals(fromSchemaVersion, doc.getString("source." + DataSourceConnectionDto.FIELD_SCHEMA_VERSION));

                doc = invocation.<Update>getArgument(1).getUpdateObject().get("$set", Document.class);
                assertEquals(toSchemaVersion, doc.getString("source." + DataSourceConnectionDto.FIELD_SCHEMA_VERSION));
                assertEquals(lastUpdate, doc.getLong(DataSourceConnectionDto.FIELD_LAST_UPDATE));
                return updateResult;
            }).when(metadataInstancesService).updateMany(any(Query.class), any(Update.class));

            long count = dataSourceService.updatePartialSchema(connId, loadFieldsStatus, lastUpdate, fromSchemaVersion, toSchemaVersion, filters, userDetail);
            assertEquals(1, count);
        }

        @Test
        void testChangeConnectionAndNotNullFilter() {
            String filters = "test-filter";

            DataSourceEntity dataSourceEntity = mock(DataSourceEntity.class);
            when(dataSourceRepository.findAndModify(any(Query.class), any(Update.class), eq(userDetail))).thenReturn(dataSourceEntity);

            UpdateResult updateResult = mock(UpdateResult.class);
            when(updateResult.getModifiedCount()).thenReturn(1L);
            doAnswer(invocation -> {
                Document doc = invocation.<Query>getArgument(0).getQueryObject();
                assertEquals(connId, doc.getString("source._id"));
                assertEquals("{\"$ne\": true}", doc.get("is_deleted", Document.class).toJson());
                assertEquals(MetaType.table, doc.get("meta_type"));
                assertEquals(SourceTypeEnum.SOURCE, doc.get("sourceType"));
                assertEquals(fromSchemaVersion, doc.getString("source." + DataSourceConnectionDto.FIELD_SCHEMA_VERSION));
                assertEquals("{\"$nin\": [\"" + filters + "\"]}", doc.get("name", Document.class).toJson());

                doc = invocation.<Update>getArgument(1).getUpdateObject().get("$set", Document.class);
                assertEquals(toSchemaVersion, doc.getString("source." + DataSourceConnectionDto.FIELD_SCHEMA_VERSION));
                assertEquals(lastUpdate, doc.getLong(DataSourceConnectionDto.FIELD_LAST_UPDATE));
                return updateResult;
            }).when(metadataInstancesService).updateMany(any(Query.class), any(Update.class));

            long count = dataSourceService.updatePartialSchema(connId, loadFieldsStatus, lastUpdate, fromSchemaVersion, toSchemaVersion, filters, userDetail);
            assertEquals(2, count);
        }

        @Test
        void testChangeConnectionAndNotMultiFilter() {
            String filters = "test-filter1,test-filter2";

            DataSourceEntity dataSourceEntity = mock(DataSourceEntity.class);
            when(dataSourceRepository.findAndModify(any(Query.class), any(Update.class), eq(userDetail))).thenReturn(dataSourceEntity);

            UpdateResult updateResult = mock(UpdateResult.class);
            when(updateResult.getModifiedCount()).thenReturn(1L);
            doAnswer(invocation -> {
                Document doc = invocation.<Query>getArgument(0).getQueryObject();
                assertEquals(connId, doc.getString("source._id"));
                assertEquals("{\"$ne\": true}", doc.get("is_deleted", Document.class).toJson());
                assertEquals(MetaType.table, doc.get("meta_type"));
                assertEquals(SourceTypeEnum.SOURCE, doc.get("sourceType"));
                assertEquals(fromSchemaVersion, doc.getString("source." + DataSourceConnectionDto.FIELD_SCHEMA_VERSION));
                assertEquals("{\"$nin\": [\"" + String.join("\", \"", filters.split(",")) + "\"]}", doc.get("name", Document.class).toJson());

                doc = invocation.<Update>getArgument(1).getUpdateObject().get("$set", Document.class);
                assertEquals(toSchemaVersion, doc.getString("source." + DataSourceConnectionDto.FIELD_SCHEMA_VERSION));
                assertEquals(lastUpdate, doc.getLong(DataSourceConnectionDto.FIELD_LAST_UPDATE));
                return updateResult;
            }).when(metadataInstancesService).updateMany(any(Query.class), any(Update.class));

            long count = dataSourceService.updatePartialSchema(connId, loadFieldsStatus, lastUpdate, fromSchemaVersion, toSchemaVersion, filters, userDetail);
            assertEquals(2, count);
        }
    }

    @Nested
    @DisplayName("LoadPartTablesByName Tests")
    class LoadPartTablesByNameTest {
        private DataSourceServiceImpl dataSourceService;
        private MetadataInstancesService metadataInstancesService;
        private DataSourceDefinitionService dataSourceDefinitionService;
        private UserDetail userDetail;
        private String connectionId;
        private List<String> tables;
        private DataSourceConnectionDto connectionDto;
        private DataSourceDefinitionDto definitionDto;
        private MetadataInstancesDto databaseModel;

        @BeforeEach
        void setUp() {
            dataSourceService = mock(DataSourceServiceImpl.class);
            metadataInstancesService = mock(MetadataInstancesService.class);
            dataSourceDefinitionService = mock(DataSourceDefinitionService.class);
            userDetail = new UserDetail("6393f084c162f518b18165c3", "customerId", "username", "password", "customerType",
                    "accessCode", false, false, false, false, Arrays.asList(new SimpleGrantedAuthority("role")));
            connectionId = "507f1f77bcf86cd799439011";
            tables = Arrays.asList("table1", "table2", "table3");

            connectionDto = new DataSourceConnectionDto();
            connectionDto.setId(new ObjectId(connectionId));
            connectionDto.setDatabase_type("mysql");

            definitionDto = new DataSourceDefinitionDto();
            definitionDto.setExpression("test_expression");

            databaseModel = new MetadataInstancesDto();
            databaseModel.setId(new ObjectId());

            // Set up mocks using reflection
            ReflectionTestUtils.setField(dataSourceService, "metadataInstancesService", metadataInstancesService);
            ReflectionTestUtils.setField(dataSourceService, "dataSourceDefinitionService", dataSourceDefinitionService);

            // Mock the real method call
            doCallRealMethod().when(dataSourceService).loadPartTablesByName(anyString(), anyList(), any(UserDetail.class));
        }

        @Test
        @DisplayName("Should return early when connection not found")
        void testLoadPartTablesByName_ConnectionNotFound() {
            // Given
            when(dataSourceService.findById(any(ObjectId.class))).thenReturn(null);

            // When
            dataSourceService.loadPartTablesByName(connectionId, tables, userDetail);

            // Then
            verify(dataSourceService).findById(any(ObjectId.class));
            verify(dataSourceDefinitionService, never()).getByDataSourceType(anyString(), any(UserDetail.class));
            verify(metadataInstancesService, never()).findAllDto(any(Query.class), any(UserDetail.class));
        }

        @Test
        @DisplayName("Should return early when definition not found")
        void testLoadPartTablesByName_DefinitionNotFound() {
            // Given
            when(dataSourceService.findById(any(ObjectId.class))).thenReturn(connectionDto);
            when(dataSourceDefinitionService.getByDataSourceType(anyString(), any(UserDetail.class))).thenReturn(null);

            // When
            dataSourceService.loadPartTablesByName(connectionId, tables, userDetail);

            // Then
            verify(dataSourceService).findById(any(ObjectId.class));
            verify(dataSourceDefinitionService).getByDataSourceType("mysql", userDetail);
            verify(dataSourceService, never()).getDatabaseModelId(anyString(), any(UserDetail.class));
        }

        @Test
        @DisplayName("Should return early when database model ID is blank")
        void testLoadPartTablesByName_DatabaseModelIdBlank() {
            // Given
            when(dataSourceService.findById(any(ObjectId.class))).thenReturn(connectionDto);
            when(dataSourceDefinitionService.getByDataSourceType(anyString(), any(UserDetail.class))).thenReturn(definitionDto);
            when(dataSourceService.getDatabaseModelId(anyString(), any(UserDetail.class))).thenReturn("");

            // When
            dataSourceService.loadPartTablesByName(connectionId, tables, userDetail);

            // Then
            verify(dataSourceService).findById(any(ObjectId.class));
            verify(dataSourceDefinitionService).getByDataSourceType("mysql", userDetail);
            verify(dataSourceService).getDatabaseModelId(connectionId, userDetail);
            verify(metadataInstancesService, never()).findAllDto(any(Query.class), any(UserDetail.class));
        }

        @Test
        @DisplayName("Should send test connection for all tables when no existing metadata found")
        void testLoadPartTablesByName_NoExistingMetadata() {
            // Given
            when(dataSourceService.findById(any(ObjectId.class))).thenReturn(connectionDto);
            when(dataSourceDefinitionService.getByDataSourceType(anyString(), any(UserDetail.class))).thenReturn(definitionDto);
            when(dataSourceService.getDatabaseModelId(anyString(), any(UserDetail.class))).thenReturn("databaseModelId123");
            when(metadataInstancesService.findAllDto(any(Query.class), any(UserDetail.class))).thenReturn(Collections.emptyList());
            doNothing().when(dataSourceService).sendTestConnection(any(DataSourceConnectionDto.class), anyBoolean(), anyBoolean(), anyString(), any(UserDetail.class));

            // When
            dataSourceService.loadPartTablesByName(connectionId, tables, userDetail);

            // Then
            verify(dataSourceService).findById(any(ObjectId.class));
            verify(dataSourceDefinitionService).getByDataSourceType("mysql", userDetail);
            verify(dataSourceService).getDatabaseModelId(connectionId, userDetail);
            verify(metadataInstancesService).findAllDto(argThat(query -> {
                Document queryObject = query.getQueryObject();
                return queryObject.containsKey("source._id") &&
                       queryObject.get("source._id").equals(connectionId) &&
                       queryObject.containsKey("sourceType") &&
                       queryObject.get("sourceType").equals("SOURCE") &&
                       queryObject.containsKey("original_name");
            }), eq(userDetail));
            verify(dataSourceService).sendTestConnection(eq(connectionDto), eq(true), eq(true), eq("table1,table2,table3"), eq(userDetail));
        }

        @Test
        @DisplayName("Should send test connection only for missing tables when some metadata exists")
        void testLoadPartTablesByName_PartialExistingMetadata() {
            // Given
            List<MetadataInstancesDto> existingMetadata = new ArrayList<>();
            MetadataInstancesDto existingTable1 = new MetadataInstancesDto();
            existingTable1.setOriginalName("table1");
            existingMetadata.add(existingTable1);

            when(dataSourceService.findById(any(ObjectId.class))).thenReturn(connectionDto);
            when(dataSourceDefinitionService.getByDataSourceType(anyString(), any(UserDetail.class))).thenReturn(definitionDto);
            when(dataSourceService.getDatabaseModelId(anyString(), any(UserDetail.class))).thenReturn("databaseModelId123");
            when(metadataInstancesService.findAllDto(any(Query.class), any(UserDetail.class))).thenReturn(existingMetadata);
            doNothing().when(dataSourceService).sendTestConnection(any(DataSourceConnectionDto.class), anyBoolean(), anyBoolean(), anyString(), any(UserDetail.class));

            // When
            dataSourceService.loadPartTablesByName(connectionId, tables, userDetail);

            // Then
            verify(dataSourceService).sendTestConnection(eq(connectionDto), eq(true), eq(true), eq("table2,table3"), eq(userDetail));
        }

        @Test
        @DisplayName("Should not send test connection when all tables already exist")
        void testLoadPartTablesByName_AllTablesExist() {
            // Given
            List<MetadataInstancesDto> existingMetadata = new ArrayList<>();
            for (String tableName : tables) {
                MetadataInstancesDto metadata = new MetadataInstancesDto();
                metadata.setOriginalName(tableName);
                existingMetadata.add(metadata);
            }

            when(dataSourceService.findById(any(ObjectId.class))).thenReturn(connectionDto);
            when(dataSourceDefinitionService.getByDataSourceType(anyString(), any(UserDetail.class))).thenReturn(definitionDto);
            when(dataSourceService.getDatabaseModelId(anyString(), any(UserDetail.class))).thenReturn("databaseModelId123");
            when(metadataInstancesService.findAllDto(any(Query.class), any(UserDetail.class))).thenReturn(existingMetadata);

            // When
            dataSourceService.loadPartTablesByName(connectionId, tables, userDetail);

            // Then
            verify(dataSourceService, never()).sendTestConnection(any(DataSourceConnectionDto.class), anyBoolean(), anyBoolean(), anyString(), any(UserDetail.class));
        }

        @Test
        @DisplayName("Should handle empty table list")
        void testLoadPartTablesByName_EmptyTableList() {
            // Given
            List<String> emptyTables = Collections.emptyList();
            when(dataSourceService.findById(any(ObjectId.class))).thenReturn(connectionDto);
            when(dataSourceDefinitionService.getByDataSourceType(anyString(), any(UserDetail.class))).thenReturn(definitionDto);
            when(dataSourceService.getDatabaseModelId(anyString(), any(UserDetail.class))).thenReturn("databaseModelId123");
            when(metadataInstancesService.findAllDto(any(Query.class), any(UserDetail.class))).thenReturn(Collections.emptyList());

            // When
            dataSourceService.loadPartTablesByName(connectionId, emptyTables, userDetail);

            // Then
            verify(metadataInstancesService).findAllDto(any(Query.class), eq(userDetail));
            verify(dataSourceService, never()).sendTestConnection(any(DataSourceConnectionDto.class), anyBoolean(), anyBoolean(), anyString(), any(UserDetail.class));
        }


        @Test
        @DisplayName("Should handle case where getDatabaseModelId returns null")
        void testLoadPartTablesByName_DatabaseModelIdNull() {
            // Given
            when(dataSourceService.findById(any(ObjectId.class))).thenReturn(connectionDto);
            when(dataSourceDefinitionService.getByDataSourceType(anyString(), any(UserDetail.class))).thenReturn(definitionDto);
            when(dataSourceService.getDatabaseModelId(anyString(), any(UserDetail.class))).thenReturn(null);

            // When
            dataSourceService.loadPartTablesByName(connectionId, tables, userDetail);

            // Then
            verify(dataSourceService).getDatabaseModelId(connectionId, userDetail);
            verify(metadataInstancesService, never()).findAllDto(any(Query.class), any(UserDetail.class));
            verify(dataSourceService, never()).sendTestConnection(any(DataSourceConnectionDto.class), anyBoolean(), anyBoolean(), anyString(), any(UserDetail.class));
        }


    }

    @Nested
    @DisplayName("BatchImport with ImportModeEnum Tests")
    class BatchImportWithImportModeTest {
        private List<DataSourceConnectionDto> connectionDtos;
        private UserDetail user;
        private com.tapdata.tm.commons.task.dto.ImportModeEnum importMode;
        private DataSourceConnectionDto connectionDto;
        private DataSourceConnectionDto existingConnection;
        private AgentGroupService agentGroupService;
        private ExternalStorageService externalStorageService;

        @BeforeEach
        void setUp() {
            dataSourceService = spy(new DataSourceServiceImpl(mock(DataSourceRepository.class)));
            connectionDtos = new ArrayList<>();
            user = mock(UserDetail.class);
            importMode = com.tapdata.tm.commons.task.dto.ImportModeEnum.REPLACE;

            // Setup connection DTO
            connectionDto = new DataSourceConnectionDto();
            connectionDto.setId(new ObjectId("662877df9179877be8b37075"));
            connectionDto.setName("test_connection");
            connectionDto.setDatabase_type("mysql");
            connectionDtos.add(connectionDto);

            // Setup existing connection
            existingConnection = new DataSourceConnectionDto();
            existingConnection.setId(new ObjectId("662877df9179877be8b37076"));
            existingConnection.setName("test_connection");

            // Mock services
            agentGroupService = mock(AgentGroupService.class);
            externalStorageService = mock(ExternalStorageService.class);

            ReflectionTestUtils.setField(dataSourceService, "agentGroupService", agentGroupService);
            ReflectionTestUtils.setField(dataSourceService, "externalStorageService", externalStorageService);
        }

        @Test
        @DisplayName("test batchImport with REPLACE mode - existing connection")
        void testBatchImportReplaceModeWithExistingConnection() {
            // Setup
            importMode = com.tapdata.tm.commons.task.dto.ImportModeEnum.REPLACE;

            doReturn(existingConnection).when(dataSourceService).findOne(any(Query.class), eq(user));
            doReturn(connectionDto).when(dataSourceService).save(connectionDto, user);
            doNothing().when(agentGroupService).importAgentInfo(connectionDto);

            // Execute
            Map<String, DataSourceConnectionDto> result = dataSourceService.batchImport(connectionDtos, user, importMode);

            // Verify
            assertNotNull(result);
            assertEquals(1, result.size());
            assertTrue(result.containsKey("662877df9179877be8b37075"));

            // Verify that the connection ID was replaced with existing ID
            assertEquals(existingConnection.getId(), connectionDto.getId());
            verify(dataSourceService, times(1)).save(connectionDto, user);
            verify(agentGroupService, times(1)).importAgentInfo(connectionDto);
        }

        @Test
        @DisplayName("test batchImport with REPLACE mode - no existing connection")
        void testBatchImportReplaceModeNoExistingConnection() {
            // Setup
            importMode = com.tapdata.tm.commons.task.dto.ImportModeEnum.REPLACE;

            doReturn(null).when(dataSourceService).findOne(any(Query.class), eq(user));
            doReturn(connectionDto).when(dataSourceService).handleImportAsCopyConnection(connectionDto, user);

            // Execute
            Map<String, DataSourceConnectionDto> result = dataSourceService.batchImport(connectionDtos, user, importMode);

            // Verify
            assertNotNull(result);
            assertEquals(1, result.size());
            verify(dataSourceService, times(1)).handleImportAsCopyConnection(connectionDto, user);
        }

        @Test
        @DisplayName("test batchImport with IMPORT_AS_COPY mode")
        void testBatchImportCopyMode() {
            // Setup
            importMode = com.tapdata.tm.commons.task.dto.ImportModeEnum.IMPORT_AS_COPY;

            doReturn(connectionDto).when(dataSourceService).handleImportAsCopyConnection(connectionDto, user);

            // Execute
            Map<String, DataSourceConnectionDto> result = dataSourceService.batchImport(connectionDtos, user, importMode);

            // Verify
            assertNotNull(result);
            assertEquals(1, result.size());
            verify(dataSourceService, times(1)).handleImportAsCopyConnection(connectionDto, user);
        }

        @Test
        @DisplayName("test batchImport with CANCEL_IMPORT mode - existing connection")
        void testBatchImportCancelModeExistingConnection() {
            // Setup
            importMode = com.tapdata.tm.commons.task.dto.ImportModeEnum.CANCEL_IMPORT;

            doReturn(existingConnection).when(dataSourceService).findOne(any(Query.class), eq(user));

            // Execute
            Map<String, DataSourceConnectionDto> result = dataSourceService.batchImport(connectionDtos, user, importMode);

            // Verify
            assertNotNull(result);
            assertEquals(1, result.size());
            assertTrue(result.containsKey("662877df9179877be8b37075"));
            assertNull(result.get("662877df9179877be8b37075")); // Should be null for cancel import with existing connection

            verify(dataSourceService, never()).handleImportAsCopyConnection(any(), any());
        }

        @Test
        @DisplayName("test batchImport with CANCEL_IMPORT mode - no existing connection")
        void testBatchImportCancelModeNoExistingConnection() {
            // Setup
            importMode = com.tapdata.tm.commons.task.dto.ImportModeEnum.CANCEL_IMPORT;

            doReturn(null).when(dataSourceService).findOne(any(Query.class), eq(user));
            doReturn(connectionDto).when(dataSourceService).handleImportAsCopyConnection(connectionDto, user);

            // Execute
            Map<String, DataSourceConnectionDto> result = dataSourceService.batchImport(connectionDtos, user, importMode);

            // Verify
            assertNotNull(result);
            assertEquals(1, result.size());
            verify(dataSourceService, times(1)).handleImportAsCopyConnection(connectionDto, user);
        }

        @Test
        @DisplayName("test batchImport with REUSE_EXISTING mode - existing connection")
        void testBatchImportReuseExistingModeWithExistingConnection() {
            // Setup
            importMode = ImportModeEnum.REUSE_EXISTING;
            doReturn(existingConnection).when(dataSourceService).findOne(any(Query.class), eq(user));

            // Execute
            Map<String, DataSourceConnectionDto> result = dataSourceService.batchImport(connectionDtos, user, importMode);

            // Verify
            assertNotNull(result);
            assertEquals(1, result.size());
            assertTrue(result.containsKey("662877df9179877be8b37075"));

            // Verify that the connection ID was replaced with existing ID
            assertEquals(existingConnection.getId(), result.get("662877df9179877be8b37075").getId());
            verify(dataSourceService, times(0)).save(connectionDto, user);
        }

        @Test
        @DisplayName("test batchImport with REUSE_EXISTING mode - no existing connection")
        void testBatchImportReuseExistingModeNoExistingConnection() {
            // Setup
            importMode = com.tapdata.tm.commons.task.dto.ImportModeEnum.REUSE_EXISTING;

            doReturn(null).when(dataSourceService).findOne(any(Query.class), eq(user));
            doReturn(connectionDto).when(dataSourceService).handleImportAsCopyConnection(connectionDto, user);

            // Execute
            Map<String, DataSourceConnectionDto> result = dataSourceService.batchImport(connectionDtos, user, importMode);

            // Verify
            assertNotNull(result);
            assertEquals(1, result.size());
            verify(dataSourceService, times(1)).handleImportAsCopyConnection(connectionDto, user);
        }
    }

    @Nested
    @DisplayName("HandleImportAsCopyConnection Tests")
    class HandleImportAsCopyConnectionTest {
        private DataSourceConnectionDto connectionDto;
        private DataSourceConnectionDto existingConnectionById;
        private UserDetail user;
        private AgentGroupService agentGroupService;
        private ExternalStorageService externalStorageService;
        private ExternalStorageDto externalStorageDto;
        private ExternalStorageDto defaultExternalStorage;

        @BeforeEach
        void setUp() {
            dataSourceService = spy(new DataSourceServiceImpl(mock(DataSourceRepository.class)));
            user = mock(UserDetail.class);

            // Setup connection DTO
            connectionDto = new DataSourceConnectionDto();
            connectionDto.setId(new ObjectId("662877df9179877be8b37075"));
            connectionDto.setName("test_connection");
            connectionDto.setDatabase_type("mysql");

            // Setup existing connection by ID
            existingConnectionById = new DataSourceConnectionDto();
            existingConnectionById.setId(new ObjectId("662877df9179877be8b37075"));
            existingConnectionById.setName("existing_connection");

            // Mock services
            agentGroupService = mock(AgentGroupService.class);
            externalStorageService = mock(ExternalStorageService.class);
            externalStorageDto = mock(ExternalStorageDto.class);
            defaultExternalStorage = mock(ExternalStorageDto.class);

            ReflectionTestUtils.setField(dataSourceService, "agentGroupService", agentGroupService);
            ReflectionTestUtils.setField(dataSourceService, "externalStorageService", externalStorageService);

            when(defaultExternalStorage.getId()).thenReturn(new ObjectId("662877df9179877be8b37080"));
        }

        @Test
        @DisplayName("test handleImportAsCopyConnection with existing connection by ID")
        void testHandleImportAsCopyConnectionWithExistingById() {
            // Setup
            doReturn(existingConnectionById).when(dataSourceService).findOne(any(Query.class));
            doReturn(false).when(dataSourceService).checkRepeatNameBool(user, "test_connection", null);
            doReturn(connectionDto).when(dataSourceService).importEntity(connectionDto, user);
            doNothing().when(agentGroupService).importAgentInfo(connectionDto);

            // Execute
            DataSourceConnectionDto result = dataSourceService.handleImportAsCopyConnection(connectionDto, user);

            // Verify
            assertNotNull(result);
            assertNull(connectionDto.getId()); // ID should be set to null for new connection
            verify(dataSourceService, times(1)).importEntity(connectionDto, user);
            verify(agentGroupService, times(1)).importAgentInfo(connectionDto);
        }

        @Test
        @DisplayName("test handleImportAsCopyConnection with no existing connection by ID")
        void testHandleImportAsCopyConnectionNoExistingById() {
            // Setup
            doReturn(null).when(dataSourceService).findOne(any(Query.class));
            doReturn(false).when(dataSourceService).checkRepeatNameBool(user, "test_connection", null);
            doReturn(connectionDto).when(dataSourceService).importEntity(connectionDto, user);
            doNothing().when(agentGroupService).importAgentInfo(connectionDto);

            // Execute
            DataSourceConnectionDto result = dataSourceService.handleImportAsCopyConnection(connectionDto, user);

            // Verify
            assertNotNull(result);
            assertEquals(new ObjectId("662877df9179877be8b37075"), connectionDto.getId()); // ID should remain unchanged
            verify(dataSourceService, times(1)).importEntity(connectionDto, user);
            verify(agentGroupService, times(1)).importAgentInfo(connectionDto);
        }

        @Test
        @DisplayName("test handleImportAsCopyConnection with name conflict")
        void testHandleImportAsCopyConnectionWithNameConflict() {
            // Setup
            doReturn(null).when(dataSourceService).findOne(any(Query.class));
            doReturn(true, true, false).when(dataSourceService).checkRepeatNameBool(eq(user), anyString(), eq(null));
            doReturn(connectionDto).when(dataSourceService).importEntity(connectionDto, user);
            doNothing().when(agentGroupService).importAgentInfo(connectionDto);

            // Execute
            DataSourceConnectionDto result = dataSourceService.handleImportAsCopyConnection(connectionDto, user);

            // Verify
            assertNotNull(result);
            assertEquals("test_connection_import_import", connectionDto.getName()); // Name should be modified to avoid conflict
            verify(dataSourceService, times(3)).checkRepeatNameBool(eq(user), anyString(), eq(null));
            verify(dataSourceService, times(1)).importEntity(connectionDto, user);
        }

        @Test
        @DisplayName("test handleImportAsCopyConnection with external storage")
        void testHandleImportAsCopyConnectionWithExternalStorage() {
            // Setup
            connectionDto.setShareCDCExternalStorageId("662877df9179877be8b37079");

            doReturn(null).when(dataSourceService).findOne(any(Query.class));
            doReturn(false).when(dataSourceService).checkRepeatNameBool(user, "test_connection", null);
            doReturn(connectionDto).when(dataSourceService).importEntity(connectionDto, user);
            doNothing().when(agentGroupService).importAgentInfo(connectionDto);

            when(externalStorageService.findById(any(ObjectId.class))).thenReturn(externalStorageDto);

            // Execute
            DataSourceConnectionDto result = dataSourceService.handleImportAsCopyConnection(connectionDto, user);

            // Verify
            assertNotNull(result);
            assertEquals("662877df9179877be8b37079", connectionDto.getShareCDCExternalStorageId());
            verify(externalStorageService, times(1)).findById(any(ObjectId.class));
            verify(dataSourceService, times(1)).importEntity(connectionDto, user);
        }

        @Test
        @DisplayName("test handleImportAsCopyConnection with missing external storage")
        void testHandleImportAsCopyConnectionWithMissingExternalStorage() {
            // Setup
            connectionDto.setShareCDCExternalStorageId("662877df9179877be8b37079");

            doReturn(null).when(dataSourceService).findOne(any(Query.class));
            doReturn(false).when(dataSourceService).checkRepeatNameBool(user, "test_connection", null);
            doReturn(connectionDto).when(dataSourceService).importEntity(connectionDto, user);
            doNothing().when(agentGroupService).importAgentInfo(connectionDto);

            when(externalStorageService.findById(any(ObjectId.class))).thenReturn(null);
            when(externalStorageService.findOne(any(Query.class))).thenReturn(defaultExternalStorage);

            // Execute
            DataSourceConnectionDto result = dataSourceService.handleImportAsCopyConnection(connectionDto, user);

            // Verify
            assertNotNull(result);
            assertEquals("662877df9179877be8b37080", connectionDto.getShareCDCExternalStorageId());
            verify(externalStorageService, times(1)).findById(any(ObjectId.class));
            verify(externalStorageService, times(1)).findOne(any(Query.class));
            verify(dataSourceService, times(1)).importEntity(connectionDto, user);
        }

        @Test
        @DisplayName("test handleImportAsCopyConnection with blank external storage ID")
        void testHandleImportAsCopyConnectionWithBlankExternalStorageId() {
            // Setup
            connectionDto.setShareCDCExternalStorageId(""); // Blank storage ID

            doReturn(null).when(dataSourceService).findOne(any(Query.class));
            doReturn(false).when(dataSourceService).checkRepeatNameBool(user, "test_connection", null);
            doReturn(connectionDto).when(dataSourceService).importEntity(connectionDto, user);
            doNothing().when(agentGroupService).importAgentInfo(connectionDto);

            // Execute
            DataSourceConnectionDto result = dataSourceService.handleImportAsCopyConnection(connectionDto, user);

            // Verify
            assertNotNull(result);
            verify(externalStorageService, never()).findById(any(ObjectId.class));
            verify(dataSourceService, times(1)).importEntity(connectionDto, user);
        }
    }

    @Nested
    @DisplayName("buildPdkRealName Tests")
    class buildPdkRealNameTest {
        DataSourceServiceImpl dc;
        DataSourceDefinitionService dataSourceDefinitionService;
        @BeforeEach
        void init() {
            dc = mock(DataSourceServiceImpl.class);
            dataSourceDefinitionService = mock(DataSourceDefinitionService.class);
            ReflectionTestUtils.setField(dc, "dataSourceDefinitionService", dataSourceDefinitionService);
        }

        @Test
        void shouldDoNothingWhenPdkHashListIsEmpty() {
            doCallRealMethod().when(dc).buildPdkRealName(anyList(), any(UserDetail.class));
            when(dataSourceDefinitionService.findByPdkHashList(anySet(), any(UserDetail.class))).thenReturn(Collections.emptyList());
            dc.buildPdkRealName(Collections.singletonList(new DataSourceConnectionDto()), mock(UserDetail.class));
            verify(dataSourceDefinitionService, times(0)).findByPdkHashList(anySet(), any(UserDetail.class));
        }

        @Test
        void shouldDoNothingWhenPdkHashListNotEmpty() {
            doCallRealMethod().when(dc).buildPdkRealName(anyList(), any(UserDetail.class));
            List<DataSourceConnectionDto> connectionDto = new ArrayList<>();
            DataSourceConnectionDto item = new DataSourceConnectionDto();
            item.setPdkHash("123");
            connectionDto.add(item);
            dc.buildPdkRealName(connectionDto, mock(UserDetail.class));
            List<DataSourceDefinitionDto> all = new ArrayList<>();
            DataSourceDefinitionDto definitionDto = new DataSourceDefinitionDto();
            definitionDto.setRealName("123");
            definitionDto.setPdkHash("123");
            all.add(definitionDto);
            when(dataSourceDefinitionService.findByPdkHashList(anySet(), any(UserDetail.class))).thenReturn(all);
            verify(dataSourceDefinitionService, times(1)).findByPdkHashList(anySet(), any(UserDetail.class));
        }
    }

    @Nested
    class bulidGetDatabaseTypesTest {
        DataSourceServiceImpl dc;
        DataSourceDefinitionService dataSourceDefinitionService;
        DataSourceRepository dataSourceRepository;
        @BeforeEach
        void init() {
            dc = mock(DataSourceServiceImpl.class);
            dataSourceDefinitionService = mock(DataSourceDefinitionService.class);
            dataSourceRepository = mock(DataSourceRepository.class);
            ReflectionTestUtils.setField(dc, "repository", dataSourceRepository);
            ReflectionTestUtils.setField(dc, "dataSourceDefinitionService", dataSourceDefinitionService);
        }

        @Test
        void shouldDoNothingWhenPdkHashListIsEmpty() {
            Query query = mock(Query.class);
            Document queryObject = new Document();
            when(query.getQueryObject()).thenReturn(queryObject);
            when(dataSourceRepository.applyUserDetail(any(Query.class), any(UserDetail.class))).thenReturn(query);

            List<Document> documents = new ArrayList<>();
            AggregationResults<Document> results = mock(AggregationResults.class);
            when(results.getMappedResults()).thenReturn(documents);
            when(dataSourceRepository.aggregate(any(Aggregation.class), any(Class.class))).thenReturn(results);

            doCallRealMethod().when(dc).bulidGetDatabaseTypes(any(UserDetail.class));

            when(dataSourceDefinitionService.findByPdkHashList(anySet(), any(UserDetail.class))).thenReturn(Collections.emptyList());

            when(dataSourceDefinitionService.findByPdkHashList(anySet(), any(UserDetail.class))).thenReturn(Collections.emptyList());

            dc.bulidGetDatabaseTypes(mock(UserDetail.class));
            verify(dataSourceDefinitionService, times(0)).findByPdkHashList(anySet(), any(UserDetail.class));
            verify(dataSourceRepository, times(1)).aggregate(any(Aggregation.class), any(Class.class));
            verify(dataSourceRepository, times(1)).applyUserDetail(any(Query.class), any(UserDetail.class));
            verify(query, times(1)).getQueryObject();
            verify(results, times(1)).getMappedResults();
        }

        @Test
        void shouldDoNothingWhenPdkHashListNotEmpty() {
            Query query = mock(Query.class);
            Document queryObject = new Document();
            when(query.getQueryObject()).thenReturn(queryObject);
            when(dataSourceRepository.applyUserDetail(any(Query.class), any(UserDetail.class))).thenReturn(query);

            List<Document> documents = new ArrayList<>();
            Document d1 = new Document();
            d1.put(DataSourceServiceImpl.PDK_HASH, "123456");
            documents.add(d1);
            AggregationResults<Document> results = mock(AggregationResults.class);
            when(results.getMappedResults()).thenReturn(documents);
            when(dataSourceRepository.aggregate(any(Aggregation.class), any(Class.class))).thenReturn(results);

            doCallRealMethod().when(dc).bulidGetDatabaseTypes(any(UserDetail.class));

            List<DataSourceDefinitionDto> all = new ArrayList<>();
            DataSourceDefinitionDto definitionDto = new DataSourceDefinitionDto();
            definitionDto.setRealName("123");
            definitionDto.setPdkHash("123456");
            all.add(definitionDto);
            when(dataSourceDefinitionService.findByPdkHashList(anySet(), any(UserDetail.class))).thenReturn(all);

            dc.bulidGetDatabaseTypes(mock(UserDetail.class));
            verify(dataSourceDefinitionService, times(1)).findByPdkHashList(anySet(), any(UserDetail.class));
            verify(dataSourceRepository, times(1)).aggregate(any(Aggregation.class), any(Class.class));
            verify(dataSourceRepository, times(1)).applyUserDetail(any(Query.class), any(UserDetail.class));
            verify(query, times(1)).getQueryObject();
            verify(results, times(1)).getMappedResults();
        }
    }
}
