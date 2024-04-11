package com.tapdata.tm.ds.service.impl;


import com.tapdata.tm.Settings.constant.SettingUtil;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.util.MetaDataBuilderUtils;
import com.tapdata.tm.commons.util.MetaType;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.discovery.service.DefaultDataDirectoryService;
import com.tapdata.tm.ds.entity.DataSourceEntity;
import com.tapdata.tm.ds.repository.DataSourceRepository;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.permissions.DataPermissionHelper;
import com.tapdata.tm.worker.service.WorkerService;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.*;
import org.mockito.Answers;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
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
            doNothing().when(dataSourceService).beforeSave(any(),any());
            when(dataSourceRepository.save(any(),any())).thenReturn(mock(DataSourceEntity.class));
            DataSourceConnectionDto result = dataSourceService.add(new DataSourceConnectionDto(),mock(UserDetail.class));
            Assertions.assertNull(result.getConfig());
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
}