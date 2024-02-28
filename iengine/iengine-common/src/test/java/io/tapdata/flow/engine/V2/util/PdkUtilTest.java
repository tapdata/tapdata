package io.tapdata.flow.engine.V2.util;

import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.mongo.RestTemplateOperator;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.utils.CommonUtils;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.internal.verification.Times;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.File;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

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
                    mb.when(()->PDKIntegration.refreshJars(anyString())).thenAnswer(invocationOnMock -> null);
                    PdkUtil.downloadPdkFileIfNeed(httpClientMongoOperator,pdkHash,fileName,resourceId,callback);
                    verify(httpClientMongoOperator,new Times(1)).downloadFile(anyMap(),anyString(),anyString(),anyBoolean(),any());
                }
            }
        }
        @Test
        @SneakyThrows
        @DisplayName("downloadPdkFileIfNeed method test when exists file but not complete")
        void testDownloadPdkFileIfNeed(){
            try (MockedStatic<CommonUtils> commonUtilsMockedStatic = Mockito
                    .mockStatic(CommonUtils.class)) {
                commonUtilsMockedStatic.when(CommonUtils::getPdkBuildNumer).thenReturn(1);
                try (MockedStatic<PDKIntegration> mb = Mockito
                        .mockStatic(PDKIntegration.class)) {
                    mb.when(()->PDKIntegration.refreshJars(anyString())).thenAnswer(invocationOnMock -> null);
                    String dir = System.getProperty("user.dir") + File.separator + "dist";
                    File file = new File(dir+"/mysql__67890__.jar");
                    file.createNewFile();
                    fileMd5Map = new ConcurrentHashMap();
                    fileMd5Map.put(dir+"/mysql__67890__.jar", "1234567890123456");
                    ReflectionTestUtils.setField(PdkUtil.class,"fileMd5Map",fileMd5Map);
                    PdkUtil.downloadPdkFileIfNeed(httpClientMongoOperator,pdkHash,fileName,resourceId,callback);
                    verify(httpClientMongoOperator,new Times(1)).downloadFile(anyMap(),anyString(),anyString(),anyBoolean(),any());
                    file.delete();
                }
            }
        }
    }
    @Nested
    @DisplayName("getFileMD5 method test")
    class getFileMD5Test{
        @Test
        @DisplayName("getFileMD5 method test when isFile return false")
        void testGetFileMD5WithNotFile(){
            File file = mock(File.class);
            String fileMD5 = PdkUtil.getFileMD5(file);
            assertEquals(null,fileMD5);
        }
        @Test
        @SneakyThrows
        @DisplayName("getFileMD5 method test when isFile return true")
        void testGetFileMD5WithFile(){
            File file = new File("mysql.jar");
            file.createNewFile();
            String fileMD5 = PdkUtil.getFileMD5(file);
            file.delete();
            assertNotEquals(null,fileMD5);
        }
        @Test
        @SneakyThrows
        @DisplayName("getFileMD5 method test with exception")
        void testGetFileMD5WithEx(){
            try (MockedStatic<MessageDigest> mb = Mockito
                    .mockStatic(MessageDigest.class)) {
                mb.when(()->MessageDigest.getInstance("MD5")).thenThrow(new NoSuchAlgorithmException("mock NoSuchAlgorithmException"));
                File file = new File("mysql.jar");
                file.createNewFile();
                String fileMD5 = PdkUtil.getFileMD5(file);
                file.delete();
                assertEquals(null,fileMD5);}
        }
    }
}
