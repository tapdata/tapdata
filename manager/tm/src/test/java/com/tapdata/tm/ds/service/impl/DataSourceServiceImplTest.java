package com.tapdata.tm.ds.service.impl;


import com.tapdata.tm.Settings.constant.SettingUtil;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.discovery.service.DefaultDataDirectoryService;
import com.tapdata.tm.ds.entity.DataSourceEntity;
import com.tapdata.tm.ds.repository.DataSourceRepository;
import com.tapdata.tm.permissions.DataPermissionHelper;
import com.tapdata.tm.worker.service.WorkerService;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
            Assertions.assertDoesNotThrow(() -> dataSourceService.restoreAccessNodeType(updateDto, connectionDto, accessNodeProcessIdList));
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
            Assertions.assertDoesNotThrow(() -> dataSourceService.restoreAccessNodeType(updateDto, connectionDto, accessNodeProcessIdList));
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
            Assertions.assertDoesNotThrow(() -> dataSourceService.restoreAccessNodeType(updateDto, connectionDto, accessNodeProcessIdList));
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
}