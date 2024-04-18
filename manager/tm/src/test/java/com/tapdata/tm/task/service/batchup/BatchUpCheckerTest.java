package com.tapdata.tm.task.service.batchup;


import com.tapdata.tm.Unit4Util;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceDefinitionService;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.slf4j.Logger;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class BatchUpCheckerTest {
    BatchUpChecker batchUpChecker;
    Logger log;
    DataSourceDefinitionService dataSourceDefinitionService;
    MetadataInstancesService metadataInstancesService;
    DataSourceService dataSourceService;
    UserDetail user;

    @BeforeEach
    void init() {
        batchUpChecker = mock(BatchUpChecker.class);
        log = mock(Logger.class);
        Unit4Util.mockSlf4jLog(batchUpChecker, log);
        dataSourceDefinitionService = mock(DataSourceDefinitionService.class);
        ReflectionTestUtils.setField(batchUpChecker, "dataSourceDefinitionService", dataSourceDefinitionService);
        metadataInstancesService = mock(MetadataInstancesService.class);
        ReflectionTestUtils.setField(batchUpChecker, "metadataInstancesService", metadataInstancesService);
        dataSourceService = mock(DataSourceService.class);
        ReflectionTestUtils.setField(batchUpChecker, "dataSourceService", dataSourceService);

        user = mock(UserDetail.class);
    }

    @Test
    void testAllSetter() {
        doCallRealMethod().when(batchUpChecker).setDataSourceDefinitionService(dataSourceDefinitionService);
        doCallRealMethod().when(batchUpChecker).setMetadataInstancesService(metadataInstancesService);
        doCallRealMethod().when(batchUpChecker).setDataSourceService(dataSourceService);
        batchUpChecker.setDataSourceDefinitionService(dataSourceDefinitionService);
        batchUpChecker.setMetadataInstancesService(metadataInstancesService);
        batchUpChecker.setDataSourceService(dataSourceService);
    }

    @Nested
    class CheckDataSourceConnectionTest {
        List<DataSourceConnectionDto> connections;
        DataSourceConnectionDto connection;

        List<DataSourceDefinitionDto> all;
        DataSourceDefinitionDto dataSourceDefinitionDto;
        @BeforeEach
        void init() {
            connections = new ArrayList<>();
            connection = mock(DataSourceConnectionDto.class);
            connections.add(connection);
            when(connection.getName()).thenReturn("name");
            when(connection.getDatabase_type()).thenReturn("dataType");
            when(connection.getDefinitionPdkAPIVersion()).thenReturn("1.0.0-RELEASE");

            all = new ArrayList<>();
            dataSourceDefinitionDto = mock(DataSourceDefinitionDto.class);
            all.add(dataSourceDefinitionDto);

            when(batchUpChecker.findDataSourceDefinitionByDataSourceConnectionDto(connection, user)).thenReturn(all);

            when(dataSourceDefinitionDto.getPdkId()).thenReturn("1");
            when(dataSourceDefinitionDto.getPdkAPIVersion()).thenReturn("1.0.0-RELEASE");
            doNothing().when(connection).setDefinitionPdkId("1");

            when(batchUpChecker.sortByPdkApiVersion(any(DataSourceDefinitionDto.class), any(DataSourceDefinitionDto.class))).thenReturn(1);
            when(batchUpChecker.checkConnectionVersion(anyString(), anyString())).thenReturn(Check.LESS);
            doNothing().when(log).warn(
                    "Connection {} does not include attribute: PDK API Version. After importing, please manually check the connection configuration item and task node configuration",
                    "name"
            );

            doNothing().when(log).warn(
                    "The connector {}, type is {} version API ({}) used in the import task is too low. The API version used by connector {} in the current environment is: {}. After importing, please manually check the connection configuration item and task node configuration",
                    "name", "dataType", "1.0.0-RELEASE", "dataType", "1.0.0-RELEASE"
            );

            doCallRealMethod().when(batchUpChecker).checkDataSourceConnection(connections, user);
        }
        void assertVerify(int getName, int getDatabaseType, int getDefinitionPdkAPIVersion,
                          int findDataSourceDefinitionByDataSourceConnectionDto,
                          int warn1, int sortByPdkApiVersion, int getPdkId, int getPdkAPIVersion, int setDefinitionPdkId, int checkConnectionVersion,
                          int warn2) {
            batchUpChecker.checkDataSourceConnection(connections, user);
            verify(connection, times(getName)).getName();
            verify(connection, times(getDatabaseType)).getDatabase_type();
            verify(connection, times(getDefinitionPdkAPIVersion)).getDefinitionPdkAPIVersion();
            verify(batchUpChecker, times(findDataSourceDefinitionByDataSourceConnectionDto)).findDataSourceDefinitionByDataSourceConnectionDto(connection, user);
            verify(log, times(warn1)).warn(
                    "Connection {} does not include attribute: PDK API Version. After importing, please manually check the connection configuration item and task node configuration",
                    "name"
            );
            verify(batchUpChecker, times(sortByPdkApiVersion)).sortByPdkApiVersion(any(DataSourceDefinitionDto.class), any(DataSourceDefinitionDto.class));
            verify(dataSourceDefinitionDto, times(getPdkId)).getPdkId();
            verify(dataSourceDefinitionDto, times(getPdkAPIVersion)).getPdkAPIVersion();
            verify(connection, times(setDefinitionPdkId)).setDefinitionPdkId(anyString());
            verify(batchUpChecker, times(checkConnectionVersion)).checkConnectionVersion(anyString(), anyString());
            verify(log, times(warn2)).warn(
                    "The connector {}, type is {} version API ({}) used in the import task is too low. The API version used by connector {} in the current environment is: {}. After importing, please manually check the connection configuration item and task node configuration",
                    "name", "dataType", "1.0.0-RELEASE", "dataType", "1.0.0-RELEASE"
            );
        }
        @Test
        void testNormal() {
            Assertions.assertDoesNotThrow(() -> assertVerify(1, 1, 1,
                    1,
                    0,
                    0, 1, 1, 1, 0,
                    0));
        }
        @Test
        void testDefinitionPdkAPIVersionIsEmpty() {
            when(connection.getDefinitionPdkAPIVersion()).thenReturn(null);
            Assertions.assertDoesNotThrow(() -> assertVerify(1, 1, 1,
                    0,
                    1,
                    0, 0, 0, 0, 0,
                    0));
        }
        @Test
        void testConnectionsIsEmpty() {
            connections.remove(0);
            doNothing().when(log).warn("An task importing not any connections");
            Assertions.assertDoesNotThrow(() -> assertVerify(0, 0, 0,
                    0,
                    0,
                    0, 0, 0, 0, 0,
                    0));
            verify(log, times(1)).warn("An task importing not any connections");
        }
        @Test
        void testNotRegisterConnector() {
            all.remove(0);
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    assertVerify(1, 1, 1,
                            1,
                            0,
                            0, 0, 0, 0, 0,
                            0);
                } catch (BizException e) {
                    Assertions.assertEquals("task.import.connection.check.ConnectorNotRegister", e.getErrorCode());
                    throw e;
                }
            });
        }
        @Test
        void testVersionIsTooLower() {
            when(dataSourceDefinitionDto.getPdkAPIVersion()).thenReturn("1.0-RELEASE");
            doNothing().when(log).warn(
                    "The connector {}, type is {} version API ({}) used in the import task is too low. The API version used by connector {} in the current environment is: {}. After importing, please manually check the connection configuration item and task node configuration",
                    "name", "dataType", "1.0-RELEASE", "dataType", "1.0.0-RELEASE"
            );
            Assertions.assertDoesNotThrow(() -> assertVerify(1, 1, 1,
                    1,
                    0,
                    0, 1, 1, 1, 1,
                    0));
            verify(log, times(1)).warn(
                    "The connector {}, type is {} version API ({}) used in the import task is too low. The API version used by connector {} in the current environment is: {}. After importing, please manually check the connection configuration item and task node configuration",
                    "name", "dataType", "1.0-RELEASE", "dataType", "1.0.0-RELEASE"
            );
        }
        @Test
        void testVersionIsTooHeight() {
            when(dataSourceDefinitionDto.getPdkAPIVersion()).thenReturn("1.0.1-RELEASE");
            when(batchUpChecker.checkConnectionVersion(anyString(), anyString())).thenReturn(Check.MORE);
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    assertVerify(1, 1, 1,
                            1,
                            0,
                            1, 1, 1, 1, 1,
                            0);
                } catch (BizException e) {
                    Assertions.assertEquals("task.import.connection.check.ConnectorVersionTooHeight", e.getErrorCode());
                    throw e;
                }
            });
        }
        @Test
        void testVersionIsTooHeightV2() {
            when(dataSourceDefinitionDto.getPdkAPIVersion()).thenReturn("1.0.1-RELEASE");
            when(batchUpChecker.checkConnectionVersion(anyString(), anyString())).thenReturn(Check.MORE, Check.EQUALS);
            Assertions.assertDoesNotThrow(() -> assertVerify(1, 1, 1,
                    1,
                    0,
                    0, 1, 2, 0, 2,
                    0));
        }
    }

    @Nested
    class FindDataSourceDefinitionByDataSourceConnectionDtoTest {
        DataSourceConnectionDto connection;
        @BeforeEach
        void init() {
            connection = mock(DataSourceConnectionDto.class);
            when(connection.getDatabase_type()).thenReturn("dataType");
            when(user.getCustomerId()).thenReturn("id");
            when(dataSourceDefinitionService.findAll(any(Query.class))).thenReturn(mock(List.class));
            when(batchUpChecker.findDataSourceDefinitionByDataSourceConnectionDto(connection, user)).thenCallRealMethod();
        }
        @Test
        void testNormal() {
            Assertions.assertDoesNotThrow(() -> batchUpChecker.findDataSourceDefinitionByDataSourceConnectionDto(connection, user));
            verify(connection).getDatabase_type();
            verify(user).getCustomerId();
            verify(dataSourceDefinitionService).findAll(any(Query.class));
            verify(batchUpChecker).findDataSourceDefinitionByDataSourceConnectionDto(connection, user);
        }
    }

    @Nested
    class SortByPdkApiVersionTest {
        DataSourceDefinitionDto p1, p2;
        @BeforeEach
        void init() {
            p1 = mock(DataSourceDefinitionDto.class);
            p2 = mock(DataSourceDefinitionDto.class);
            when(p1.getPdkAPIVersion()).thenReturn("x");
            when(p2.getPdkAPIVersion()).thenReturn("y");
            when(batchUpChecker.sortByPdkApiVersion(p1, p2)).thenCallRealMethod();
        }
        @Test
        void testEquals() {
            when(batchUpChecker.checkConnectionVersion(anyString(), anyString())).thenReturn(Check.EQUALS);
            Assertions.assertEquals(Check.EQUALS.status, batchUpChecker.sortByPdkApiVersion(p1, p2));
        }
        @Test
        void testLess() {
            when(batchUpChecker.checkConnectionVersion(anyString(), anyString())).thenReturn(Check.LESS);
            Assertions.assertEquals(Check.LESS.status, batchUpChecker.sortByPdkApiVersion(p1, p2));
        }
        @Test
        void testMore() {
            when(batchUpChecker.checkConnectionVersion(anyString(), anyString())).thenReturn(Check.MORE);
            Assertions.assertEquals(Check.MORE.status, batchUpChecker.sortByPdkApiVersion(p1, p2));
        }
    }

    @Nested
    class CheckConnectionVersionTest {
        String definitionPdkAPIVersion;
        String checker;
        @BeforeEach
        void init() {
            definitionPdkAPIVersion = "1.2.2-RELEASE";
            checker = "1.2.3-RELEASE";
            when(batchUpChecker.checkConnectionVersion(anyString(), anyString())).thenCallRealMethod();
        }
        @Test
        void testLess() {
            try (MockedStatic<CommonUtils> cu = mockStatic(CommonUtils.class)) {
                cu.when(() -> CommonUtils.getPdkBuildNumer(definitionPdkAPIVersion)).thenReturn(1);
                cu.when(() -> CommonUtils.getPdkBuildNumer(checker)).thenReturn(2);
                Assertions.assertEquals(Check.LESS, batchUpChecker.checkConnectionVersion(definitionPdkAPIVersion, checker));
                cu.verify(() -> CommonUtils.getPdkBuildNumer(definitionPdkAPIVersion));
                cu.verify(() -> CommonUtils.getPdkBuildNumer(checker));
            }
        }
        @Test
        void testMore() {
            try (MockedStatic<CommonUtils> cu = mockStatic(CommonUtils.class)) {
                cu.when(() -> CommonUtils.getPdkBuildNumer(definitionPdkAPIVersion)).thenReturn(2);
                cu.when(() -> CommonUtils.getPdkBuildNumer(checker)).thenReturn(1);
                Assertions.assertEquals(Check.MORE, batchUpChecker.checkConnectionVersion(definitionPdkAPIVersion, checker));
                cu.verify(() -> CommonUtils.getPdkBuildNumer(definitionPdkAPIVersion));
                cu.verify(() -> CommonUtils.getPdkBuildNumer(checker));
            }
        }
        @Test
        void testEquals() {
            try (MockedStatic<CommonUtils> cu = mockStatic(CommonUtils.class)) {
                cu.when(() -> CommonUtils.getPdkBuildNumer(definitionPdkAPIVersion)).thenReturn(1);
                cu.when(() -> CommonUtils.getPdkBuildNumer(checker)).thenReturn(1);
                Assertions.assertEquals(Check.EQUALS, batchUpChecker.checkConnectionVersion(definitionPdkAPIVersion, checker));
                cu.verify(() -> CommonUtils.getPdkBuildNumer(definitionPdkAPIVersion));
                cu.verify(() -> CommonUtils.getPdkBuildNumer(checker));
            }
        }
    }
}