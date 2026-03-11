package io.tapdata.flow.engine.V2.util;

import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.mongo.RestTemplateOperator;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.utils.PdkSourceUtils;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.ObjectSerializable;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.flow.engine.V2.entity.PdkStateMap;
import io.tapdata.pdk.apis.context.ConfigContext;
import io.tapdata.pdk.apis.entity.ConnectorCapabilities;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.utils.CommonUtils;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.*;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.internal.verification.Times;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class PdkUtilTest {
    private HttpClientMongoOperator httpClientMongoOperator;
    private String pdkHash;
    private String fileName;
    private String resourceId;
    private RestTemplateOperator.Callback callback;
    private Map<String, String> fileMd5Map;
    @BeforeEach
    void buildNumber(){
        httpClientMongoOperator = mock(HttpClientMongoOperator.class);
        pdkHash = "12345";
        fileName = "mysql.jar";
        resourceId = "67890";
        callback = mock(RestTemplateOperator.Callback.class);
    }
    @Nested
    @DisplayName("downloadPdkFileIfNeed method test")
    class downloadPdkFileIfNeedTest{
        @Test
        @SneakyThrows
        @DisplayName("downloadPdkFileIfNeed method test when not exists file")
        void testDownloadPdkFileIfNeedWithNoFile(){
            try (MockedStatic<CommonUtils> commonUtilsMockedStatic = Mockito
                    .mockStatic(CommonUtils.class)) {
                commonUtilsMockedStatic.when(CommonUtils::getPdkBuildNumer).thenReturn(1);
                try (MockedStatic<PDKIntegration> mb = Mockito
                        .mockStatic(PDKIntegration.class)) {
                    mb.when(() -> PDKIntegration.refreshJars(anyString())).thenAnswer(invocationOnMock -> null);
                    try (MockedStatic<PdkSourceUtils> pdkSourceUtilsMockedStatic = Mockito
                            .mockStatic(PdkSourceUtils.class)) {
                        pdkSourceUtilsMockedStatic.when(() -> PdkSourceUtils.getFileMD5(any(File.class))).thenReturn("1234567890123456").thenReturn("123456");
                        when(httpClientMongoOperator.findOne(anyMap(), anyString(), any())).thenReturn("1234567890123456");
                        PdkUtil.downloadPdkFileIfNeed(httpClientMongoOperator, pdkHash, fileName, resourceId, callback);
                        verify(httpClientMongoOperator, new Times(1)).downloadFile(anyMap(), anyString(), anyString(), anyBoolean(), any());
                    }
                }
            }
        }
        @Test
        @SneakyThrows
        @DisplayName("downloadPdkFileIfNeed method test when exists file")
        void testDownloadPdkFileIfNeed(){
            try (MockedStatic<CommonUtils> commonUtilsMockedStatic = Mockito
                    .mockStatic(CommonUtils.class)) {
                commonUtilsMockedStatic.when(CommonUtils::getPdkBuildNumer).thenReturn(1);
                try (MockedStatic<PDKIntegration> mb = Mockito
                        .mockStatic(PDKIntegration.class)) {
                    mb.when(() -> PDKIntegration.refreshJars(anyString())).thenAnswer(invocationOnMock -> null);
                    try (MockedStatic<PdkSourceUtils> pdkSourceUtilsMockedStatic = Mockito
                            .mockStatic(PdkSourceUtils.class)) {
                        pdkSourceUtilsMockedStatic.when(() -> PdkSourceUtils.getFileMD5(any(File.class))).thenReturn("1234567890123456").thenReturn("123456");
                        String dir = System.getProperty("user.dir") + File.separator + "dist";
                        File file = new File(dir + "/mysql__67890__.jar");
                        file.createNewFile();
                        when(httpClientMongoOperator.findOne(anyMap(), anyString(), any())).thenReturn("1234567890123456");
                        PdkUtil.downloadPdkFileIfNeed(httpClientMongoOperator, pdkHash, fileName, resourceId, callback);
                        verify(httpClientMongoOperator, new Times(0)).downloadFile(anyMap(), anyString(), anyString(), anyBoolean(), any());
                        file.delete();
                    }
                }
            }
        }
    }
    @Nested
    class reDownloadIfNeedTest{
        @Test
        @DisplayName("no need to download again when md5 is correspond")
        void testReDownloadIfNeedOnce(){
            try (MockedStatic<FileUtils> fileUtilsMockedStatic = Mockito
                    .mockStatic(FileUtils.class)) {
                fileUtilsMockedStatic.when(()->FileUtils.deleteQuietly(any())).thenAnswer(invocationOnMock -> null);
                try (MockedStatic<PdkSourceUtils> mb = Mockito
                        .mockStatic(PdkSourceUtils.class)) {
                    mb.when(() -> PdkSourceUtils.getFileMD5(any(File.class))).thenReturn("123456");
                    String dir = System.getProperty("user.dir") + File.separator + "dist";
                    File theFilePath = new File(dir + "/mysql__67890__.jar");
                    when(httpClientMongoOperator.findOne(anyMap(), anyString(), any())).thenReturn("123456");
                    PdkUtil.reDownloadIfNeed(httpClientMongoOperator, pdkHash, fileName, theFilePath);
                    fileUtilsMockedStatic.verify(() -> FileUtils.deleteQuietly(any()), new Times(0));
                }
            }
        }
        @Test
        @DisplayName("need to reDownload when md5 is not correspond")
        void testReDownloadIfNeedTwice() {
            try (MockedStatic<FileUtils> fileUtilsMockedStatic = Mockito
                    .mockStatic(FileUtils.class)) {
                fileUtilsMockedStatic.when(() -> FileUtils.deleteQuietly(any())).thenAnswer(invocationOnMock -> null);
                try (MockedStatic<PdkSourceUtils> mb = Mockito
                        .mockStatic(PdkSourceUtils.class)) {
                    mb.when(() -> PdkSourceUtils.getFileMD5(any(File.class))).thenReturn("111111").thenReturn("123456");
                    try (MockedStatic<CommonUtils> commonUtilsMockedStatic = Mockito
                            .mockStatic(CommonUtils.class)) {
                        commonUtilsMockedStatic.when(CommonUtils::getPdkBuildNumer).thenReturn(1);
                        try (MockedStatic<PDKIntegration> pdkIntegrationMockedStatic = Mockito
                                .mockStatic(PDKIntegration.class)) {
                            pdkIntegrationMockedStatic.when(() -> PDKIntegration.refreshJars(anyString())).thenAnswer(invocationOnMock -> null);
                            String dir = System.getProperty("user.dir") + File.separator + "dist";
                            File theFilePath = new File(dir + "/mysql__67890__.jar");
                            when(httpClientMongoOperator.findOne(anyMap(), anyString(), any())).thenReturn("123456");
                            PdkUtil.reDownloadIfNeed(httpClientMongoOperator, pdkHash, fileName, theFilePath);
                            fileUtilsMockedStatic.verify(() -> FileUtils.deleteQuietly(any()), new Times(1));
                        }
                    }
                }
            }
        }
    }

    @Nested
    @DisplayName("Method decodeOffset test")
    class decodeOffsetTest{
        @Test
        @DisplayName("test main process")
        void test1() {
            String offset = "offset";
            ConnectorNode connectorNode = mock(ConnectorNode.class);
            ObjectSerializable objectSerializable = mock(ObjectSerializable.class);
            Object mockDecodeResult = new Object();
            when(objectSerializable.toObject(any(), any(),any())).thenReturn(mockDecodeResult);
            try (
                    MockedStatic<InstanceFactory> instanceFactoryMockedStatic = mockStatic(InstanceFactory.class)
            ) {
                instanceFactoryMockedStatic.when(() -> InstanceFactory.instance(ObjectSerializable.class)).thenReturn(objectSerializable);

                Object result = assertDoesNotThrow(() -> PdkUtil.decodeOffset(offset, connectorNode));
                assertSame(mockDecodeResult, result);
            }
        }

        @Test
        @DisplayName("test offset is blank or null")
        void test2() {
            ConnectorNode connectorNode = mock(ConnectorNode.class);
            assertNull(assertDoesNotThrow(()->PdkUtil.decodeOffset("", connectorNode)));
            assertNull(assertDoesNotThrow(() -> PdkUtil.decodeOffset(null, connectorNode)));
        }

        @Test
        @DisplayName("test connector node is null")
        void test3() {
            String offset = "offset";
            assertNull(assertDoesNotThrow(() -> PdkUtil.decodeOffset(offset, null)));
        }
    }

    @Nested
    @DisplayName("Method createNode test")
    class CreateNodeTest {
        private HttpClientMongoOperator httpClientMongoOperator;
        private DatabaseTypeEnum.DatabaseType databaseType;
        private KVReadOnlyMap<TapTable> pdkTableMap;
        private PdkStateMap pdkStateMap;
        private PdkStateMap globalStateMap;
        private Log log;
        private String dagId;
        private String associateId;
        private Map<String, Object> connectionConfig;
        private Map<String, Object> nodeConfig;
        private Map<String, Map<String, Object>> tableNodeConfig;
        private ConnectorCapabilities connectorCapabilities;
        private ConfigContext configContext;
        private TaskDto taskDto;

        @BeforeEach
        void setUp() {
            httpClientMongoOperator = mock(HttpClientMongoOperator.class);
            databaseType = mock(DatabaseTypeEnum.DatabaseType.class);
            pdkTableMap = mock(KVReadOnlyMap.class);
            pdkStateMap = mock(PdkStateMap.class);
            globalStateMap = mock(PdkStateMap.class);
            log = mock(Log.class);
            connectorCapabilities = mock(ConnectorCapabilities.class);
            configContext = mock(ConfigContext.class);
            taskDto = mock(TaskDto.class);

            dagId = "test-dag-id";
            associateId = "test-associate-id";
            connectionConfig = new HashMap<>();
            connectionConfig.put("host", "localhost");
            connectionConfig.put("port", 3306);
            nodeConfig = new HashMap<>();
            nodeConfig.put("batchSize", 1000);
            tableNodeConfig = new HashMap<>();
            Map<String, Object> tableConfig = new HashMap<>();
            tableConfig.put("tableName", "test_table");
            tableNodeConfig.put("table1", tableConfig);

            // Mock DatabaseType methods
            when(databaseType.getPdkHash()).thenReturn("test-pdk-hash");
            when(databaseType.getJarFile()).thenReturn("test-connector.jar");
            when(databaseType.getJarRid()).thenReturn("test-resource-id");
            when(databaseType.getGroup()).thenReturn("io.tapdata");
            when(databaseType.getVersion()).thenReturn("1.0.0");
            when(databaseType.getPdkId()).thenReturn("test-pdk-id");
        }

        @Test
        @DisplayName("test createNode with all parameters - success case")
        @SneakyThrows
        void testCreateNodeWithAllParameters() {
            ConnectorNode mockConnectorNode = mock(ConnectorNode.class);
            PDKIntegration.ConnectorBuilder<ConnectorNode> mockBuilder = mock(PDKIntegration.ConnectorBuilder.class);

            try (MockedStatic<PdkUtil> pdkUtilMock = mockStatic(PdkUtil.class, Mockito.CALLS_REAL_METHODS);
                 MockedStatic<PDKIntegration> pdkIntegrationMock = mockStatic(PDKIntegration.class)) {

                // Mock downloadPdkFileIfNeed to do nothing
                pdkUtilMock.when(() -> PdkUtil.downloadPdkFileIfNeed(
                        any(HttpClientMongoOperator.class),
                        anyString(),
                        anyString(),
                        anyString(),
                        anyBoolean()
                )).thenAnswer(invocation -> null);

                // Mock PDKIntegration.createConnectorBuilder
                pdkIntegrationMock.when(PDKIntegration::createConnectorBuilder).thenReturn(mockBuilder);

                // Mock builder chain
                when(mockBuilder.withLog(any())).thenReturn(mockBuilder);
                when(mockBuilder.withDagId(anyString())).thenReturn(mockBuilder);
                when(mockBuilder.withAssociateId(anyString())).thenReturn(mockBuilder);
                when(mockBuilder.withConfigContext(any())).thenReturn(mockBuilder);
                when(mockBuilder.withGroup(anyString())).thenReturn(mockBuilder);
                when(mockBuilder.withVersion(anyString())).thenReturn(mockBuilder);
                when(mockBuilder.withPdkId(anyString())).thenReturn(mockBuilder);
                when(mockBuilder.withTableMap(any())).thenReturn(mockBuilder);
                when(mockBuilder.withStateMap(any())).thenReturn(mockBuilder);
                when(mockBuilder.withGlobalStateMap(any())).thenReturn(mockBuilder);
                when(mockBuilder.withConnectionConfig(any(DataMap.class))).thenReturn(mockBuilder);
                when(mockBuilder.withNodeConfig(any(DataMap.class))).thenReturn(mockBuilder);
                when(mockBuilder.withTableNodeConfig(anyMap())).thenReturn(mockBuilder);
                when(mockBuilder.withConnectorCapabilities(any())).thenReturn(mockBuilder);
                when(mockBuilder.build()).thenReturn(mockConnectorNode);

                // Mock taskDto
                when(taskDto.isPreviewTask()).thenReturn(false);

                // Execute
                ConnectorNode result = PdkUtil.createNode(
                        dagId,
                        databaseType,
                        httpClientMongoOperator,
                        associateId,
                        connectionConfig,
                        nodeConfig,
                        tableNodeConfig,
                        pdkTableMap,
                        pdkStateMap,
                        globalStateMap,
                        connectorCapabilities,
                        configContext,
                        log,
                        taskDto
                );

                // Verify
                assertNotNull(result);
                assertSame(mockConnectorNode, result);

                // Verify downloadPdkFileIfNeed was called
                pdkUtilMock.verify(() -> PdkUtil.downloadPdkFileIfNeed(
                        eq(httpClientMongoOperator),
                        eq("test-pdk-hash"),
                        eq("test-connector.jar"),
                        eq("test-resource-id"),
                        eq(true)
                ), times(1));

                // Verify builder methods were called
                verify(mockBuilder).withLog(log);
                verify(mockBuilder).withDagId(dagId);
                verify(mockBuilder).withAssociateId(associateId);
                verify(mockBuilder).withConfigContext(configContext);
                verify(mockBuilder).withGroup("io.tapdata");
                verify(mockBuilder).withVersion("1.0.0");
                verify(mockBuilder).withPdkId("test-pdk-id");
                verify(mockBuilder).withTableMap(pdkTableMap);
                verify(mockBuilder).withStateMap(pdkStateMap);
                verify(mockBuilder).withGlobalStateMap(globalStateMap);
                verify(mockBuilder).withConnectionConfig(any(DataMap.class));
                verify(mockBuilder).withNodeConfig(any(DataMap.class));
                verify(mockBuilder).withTableNodeConfig(anyMap());
                verify(mockBuilder).withConnectorCapabilities(connectorCapabilities);
                verify(mockBuilder).build();
            }
        }

        @Test
        @DisplayName("test createNode with minimal parameters (overload method)")
        @SneakyThrows
        void testCreateNodeWithMinimalParameters() {
            ConnectorNode mockConnectorNode = mock(ConnectorNode.class);
            PDKIntegration.ConnectorBuilder<ConnectorNode> mockBuilder = mock(PDKIntegration.ConnectorBuilder.class);

            try (MockedStatic<PdkUtil> pdkUtilMock = mockStatic(PdkUtil.class, Mockito.CALLS_REAL_METHODS);
                 MockedStatic<PDKIntegration> pdkIntegrationMock = mockStatic(PDKIntegration.class)) {

                pdkUtilMock.when(() -> PdkUtil.downloadPdkFileIfNeed(
                        any(HttpClientMongoOperator.class),
                        anyString(),
                        anyString(),
                        anyString(),
                        anyBoolean()
                )).thenAnswer(invocation -> null);

                pdkIntegrationMock.when(PDKIntegration::createConnectorBuilder).thenReturn(mockBuilder);

                when(mockBuilder.withLog(any())).thenReturn(mockBuilder);
                when(mockBuilder.withDagId(anyString())).thenReturn(mockBuilder);
                when(mockBuilder.withAssociateId(anyString())).thenReturn(mockBuilder);
                when(mockBuilder.withConfigContext(any())).thenReturn(mockBuilder);
                when(mockBuilder.withGroup(anyString())).thenReturn(mockBuilder);
                when(mockBuilder.withVersion(anyString())).thenReturn(mockBuilder);
                when(mockBuilder.withPdkId(anyString())).thenReturn(mockBuilder);
                when(mockBuilder.withTableMap(any())).thenReturn(mockBuilder);
                when(mockBuilder.withStateMap(any())).thenReturn(mockBuilder);
                when(mockBuilder.withGlobalStateMap(any())).thenReturn(mockBuilder);
                when(mockBuilder.build()).thenReturn(mockConnectorNode);

                // Execute with minimal parameters (no nodeConfig, tableNodeConfig, etc.)
                ConnectorNode result = PdkUtil.createNode(
                        dagId,
                        databaseType,
                        httpClientMongoOperator,
                        associateId,
                        connectionConfig,
                        pdkTableMap,
                        pdkStateMap,
                        globalStateMap,
                        log
                );

                // Verify
                assertNotNull(result);
                assertSame(mockConnectorNode, result);

                // Verify builder methods were called
                verify(mockBuilder).withLog(log);
                verify(mockBuilder).withDagId(dagId);
                verify(mockBuilder).withAssociateId(associateId);
                verify(mockBuilder).build();
            }
        }

        @Test
        @DisplayName("test createNode with empty connectionConfig")
        @SneakyThrows
        void testCreateNodeWithEmptyConnectionConfig() {
            ConnectorNode mockConnectorNode = mock(ConnectorNode.class);
            PDKIntegration.ConnectorBuilder<ConnectorNode> mockBuilder = mock(PDKIntegration.ConnectorBuilder.class);

            try (MockedStatic<PdkUtil> pdkUtilMock = mockStatic(PdkUtil.class, Mockito.CALLS_REAL_METHODS);
                 MockedStatic<PDKIntegration> pdkIntegrationMock = mockStatic(PDKIntegration.class)) {

                pdkUtilMock.when(() -> PdkUtil.downloadPdkFileIfNeed(
                        any(HttpClientMongoOperator.class),
                        anyString(),
                        anyString(),
                        anyString(),
                        anyBoolean()
                )).thenAnswer(invocation -> null);

                pdkIntegrationMock.when(PDKIntegration::createConnectorBuilder).thenReturn(mockBuilder);

                when(mockBuilder.withLog(any())).thenReturn(mockBuilder);
                when(mockBuilder.withDagId(anyString())).thenReturn(mockBuilder);
                when(mockBuilder.withAssociateId(anyString())).thenReturn(mockBuilder);
                when(mockBuilder.withConfigContext(any())).thenReturn(mockBuilder);
                when(mockBuilder.withGroup(anyString())).thenReturn(mockBuilder);
                when(mockBuilder.withVersion(anyString())).thenReturn(mockBuilder);
                when(mockBuilder.withPdkId(anyString())).thenReturn(mockBuilder);
                when(mockBuilder.withTableMap(any())).thenReturn(mockBuilder);
                when(mockBuilder.withStateMap(any())).thenReturn(mockBuilder);
                when(mockBuilder.withGlobalStateMap(any())).thenReturn(mockBuilder);
                when(mockBuilder.build()).thenReturn(mockConnectorNode);

                // Execute with empty connectionConfig
                ConnectorNode result = PdkUtil.createNode(
                        dagId,
                        databaseType,
                        httpClientMongoOperator,
                        associateId,
                        new HashMap<>(), // empty config
                        null,
                        null,
                        pdkTableMap,
                        pdkStateMap,
                        globalStateMap,
                        null,
                        configContext,
                        log,
                        null
                );

                // Verify
                assertNotNull(result);

                // Verify withConnectionConfig was NOT called for empty map
                verify(mockBuilder, never()).withConnectionConfig(any(DataMap.class));
            }
        }

        @Test
        @DisplayName("test createNode with preview task (no retry download)")
        @SneakyThrows
        void testCreateNodeWithPreviewTask() {
            ConnectorNode mockConnectorNode = mock(ConnectorNode.class);
            PDKIntegration.ConnectorBuilder<ConnectorNode> mockBuilder = mock(PDKIntegration.ConnectorBuilder.class);

            try (MockedStatic<PdkUtil> pdkUtilMock = mockStatic(PdkUtil.class, Mockito.CALLS_REAL_METHODS);
                 MockedStatic<PDKIntegration> pdkIntegrationMock = mockStatic(PDKIntegration.class)) {

                pdkUtilMock.when(() -> PdkUtil.downloadPdkFileIfNeed(
                        any(HttpClientMongoOperator.class),
                        anyString(),
                        anyString(),
                        anyString(),
                        anyBoolean()
                )).thenAnswer(invocation -> null);

                pdkIntegrationMock.when(PDKIntegration::createConnectorBuilder).thenReturn(mockBuilder);

                when(mockBuilder.withLog(any())).thenReturn(mockBuilder);
                when(mockBuilder.withDagId(anyString())).thenReturn(mockBuilder);
                when(mockBuilder.withAssociateId(anyString())).thenReturn(mockBuilder);
                when(mockBuilder.withConfigContext(any())).thenReturn(mockBuilder);
                when(mockBuilder.withGroup(anyString())).thenReturn(mockBuilder);
                when(mockBuilder.withVersion(anyString())).thenReturn(mockBuilder);
                when(mockBuilder.withPdkId(anyString())).thenReturn(mockBuilder);
                when(mockBuilder.withTableMap(any())).thenReturn(mockBuilder);
                when(mockBuilder.withStateMap(any())).thenReturn(mockBuilder);
                when(mockBuilder.withGlobalStateMap(any())).thenReturn(mockBuilder);
                when(mockBuilder.build()).thenReturn(mockConnectorNode);

                // Mock preview task
                when(taskDto.isPreviewTask()).thenReturn(true);

                // Execute
                ConnectorNode result = PdkUtil.createNode(
                        dagId,
                        databaseType,
                        httpClientMongoOperator,
                        associateId,
                        connectionConfig,
                        null,
                        null,
                        pdkTableMap,
                        pdkStateMap,
                        globalStateMap,
                        null,
                        configContext,
                        log,
                        taskDto
                );

                // Verify
                assertNotNull(result);

                // Verify downloadPdkFileIfNeed was called with needRetry=false for preview task
                pdkUtilMock.verify(() -> PdkUtil.downloadPdkFileIfNeed(
                        eq(httpClientMongoOperator),
                        eq("test-pdk-hash"),
                        eq("test-connector.jar"),
                        eq("test-resource-id"),
                        eq(false) // preview task should not retry
                ), times(1));
            }
        }

        @Test
        @DisplayName("test createNode throws exception when build fails")
        @SneakyThrows
        void testCreateNodeThrowsException() {
            PDKIntegration.ConnectorBuilder<ConnectorNode> mockBuilder = mock(PDKIntegration.ConnectorBuilder.class);

            try (MockedStatic<PdkUtil> pdkUtilMock = mockStatic(PdkUtil.class, Mockito.CALLS_REAL_METHODS);
                 MockedStatic<PDKIntegration> pdkIntegrationMock = mockStatic(PDKIntegration.class)) {

                pdkUtilMock.when(() -> PdkUtil.downloadPdkFileIfNeed(
                        any(HttpClientMongoOperator.class),
                        anyString(),
                        anyString(),
                        anyString(),
                        anyBoolean()
                )).thenAnswer(invocation -> null);

                pdkIntegrationMock.when(PDKIntegration::createConnectorBuilder).thenReturn(mockBuilder);

                when(mockBuilder.withLog(any())).thenReturn(mockBuilder);
                when(mockBuilder.withDagId(anyString())).thenReturn(mockBuilder);
                when(mockBuilder.withAssociateId(anyString())).thenReturn(mockBuilder);
                when(mockBuilder.withConfigContext(any())).thenReturn(mockBuilder);
                when(mockBuilder.withGroup(anyString())).thenReturn(mockBuilder);
                when(mockBuilder.withVersion(anyString())).thenReturn(mockBuilder);
                when(mockBuilder.withPdkId(anyString())).thenReturn(mockBuilder);
                when(mockBuilder.withTableMap(any())).thenReturn(mockBuilder);
                when(mockBuilder.withStateMap(any())).thenReturn(mockBuilder);
                when(mockBuilder.withGlobalStateMap(any())).thenReturn(mockBuilder);

                // Mock build to throw exception
                when(mockBuilder.build()).thenThrow(new RuntimeException("Build failed"));

                // Execute and verify exception
                RuntimeException exception = assertThrows(RuntimeException.class, () -> {
                    PdkUtil.createNode(
                            dagId,
                            databaseType,
                            httpClientMongoOperator,
                            associateId,
                            connectionConfig,
                            null,
                            null,
                            pdkTableMap,
                            pdkStateMap,
                            globalStateMap,
                            null,
                            configContext,
                            log,
                            null
                    );
                });

                // Verify exception message
                assertTrue(exception.getMessage().contains("Failed to create pdk connector node"));
                assertTrue(exception.getMessage().contains("Build failed"));
            }
        }
    }
}
