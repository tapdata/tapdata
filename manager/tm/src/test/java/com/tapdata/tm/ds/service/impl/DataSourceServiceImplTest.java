package com.tapdata.tm.ds.service.impl;


import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.Settings.constant.SettingUtil;
import com.tapdata.tm.agent.service.AgentGroupService;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.dag.AccessNodeTypeEnum;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.util.MetaDataBuilderUtils;
import com.tapdata.tm.commons.util.MetaType;
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
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.*;
import org.mockito.Answers;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
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
}
