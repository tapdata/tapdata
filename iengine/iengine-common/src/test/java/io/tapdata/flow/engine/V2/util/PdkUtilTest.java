package io.tapdata.flow.engine.V2.util;

import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.mongo.RestTemplateOperator;
import com.tapdata.tm.utils.PdkSourceUtils;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.ObjectSerializable;
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
            when(objectSerializable.toObject(any(), any())).thenReturn(mockDecodeResult);
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
}
