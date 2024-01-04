package io.tapdata.callback;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.MapUtil;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.tm.commons.metrics.ConnectorRecordDto;
import io.tapdata.callback.impl.DownloadCallbackImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import org.mockito.Mockito;
import java.util.HashMap;

import static org.mockito.ArgumentMatchers.any;

public class TestDownloadCallback {
    private HttpClientMongoOperator clientMongoOperator;
    DownloadCallbackImpl downloadCallback;
    private String connectionId;
    private String pdkHash;
    @BeforeEach
    void beforeEach(){
        clientMongoOperator = Mockito.mock(HttpClientMongoOperator.class);
        connectionId = "123";
        pdkHash = "4335aaa005ec1a74a4e2166bded2962e939ad50239f48b023b884f35b54129a5";
        DatabaseTypeEnum.DatabaseType databaseType = new DatabaseTypeEnum.DatabaseType();
        databaseType.setPdkHash(pdkHash);
        databaseType.setJarFile("mongodb-connector-v1.0-SNAPSHOT.jar");
        databaseType.setJarRid("658bd476be560938470cafa8");
        downloadCallback = Mockito.spy(DownloadCallbackImpl.class);
        downloadCallback.setConnectionId(connectionId);
        downloadCallback.setClientMongoOperator(clientMongoOperator);
        downloadCallback.setDatabaseDefinition(databaseType);
    }
    @Test
    void testNeedDownloadPdkFile() throws Exception {
        Mockito.doAnswer(invocationOnMock -> {
            ConnectorRecordDto connectorRecordDto = invocationOnMock.getArgument(0);
            assertEquals(connectionId,connectorRecordDto.getConnectionId());
            assertEquals(false,connectorRecordDto.getFlag());
            assertEquals(pdkHash,connectorRecordDto.getPdkHash());
            return null;
        }).when(downloadCallback).upsertConnectorRecord(any());
        downloadCallback.needDownloadPdkFile(false);
    }
    @Test
    void testOnProgress() throws Exception {
        Long fileSize=15626269L;
        Long progress=100L;
        Mockito.doAnswer(invocationOnMock -> {
            ConnectorRecordDto connectorRecordDto = invocationOnMock.getArgument(0);
            assertEquals(connectionId,connectorRecordDto.getConnectionId());
            assertEquals(true,connectorRecordDto.getFlag());
            assertEquals(pdkHash,connectorRecordDto.getPdkHash());
            assertEquals(fileSize.longValue(),connectorRecordDto.getFileSize());
            assertEquals(progress.longValue(),connectorRecordDto.getProgress());
            assertEquals(ConnectorRecordDto.StatusEnum.DOWNLOADING.getStatus(),connectorRecordDto.getStatus());
            return null;
        }).when(downloadCallback).upsertConnectorRecord(any());
        downloadCallback.onProgress(fileSize,progress);
    }
    @Test
    void testOnFinish() throws Exception{
        String downloadSpeed="47539.03kb/s";
        Mockito.doAnswer(invocationOnMock -> {
            ConnectorRecordDto connectorRecordDto = invocationOnMock.getArgument(0);
            assertEquals(connectionId,connectorRecordDto.getConnectionId());
            assertEquals(false,connectorRecordDto.getFlag());
            assertEquals(pdkHash,connectorRecordDto.getPdkHash());
//            assertEquals(downloadSpeed,connectorRecordDto.getDownloadSpeed());
            return null;
        }).when(downloadCallback).upsertConnectorRecord(any());
        downloadCallback.onFinish(downloadSpeed);
    }
    @Test
    void testOnError() throws Exception {
        Mockito.doAnswer(invocationOnMock -> {
            ConnectorRecordDto connectorRecordDto = invocationOnMock.getArgument(0);
            assertEquals(connectionId,connectorRecordDto.getConnectionId());
            assertEquals(true,connectorRecordDto.getFlag());
            assertEquals("Read File Faild",connectorRecordDto.getDownFiledMessage());
            return null;
        }).when(downloadCallback).upsertConnectorRecord(any());
        assertThrows(RuntimeException.class,()->{downloadCallback.onError(new Exception("Read File Faild"));});
    }
    @Test
    void testUpsertConnectorRecord() throws IllegalAccessException {
        ConnectorRecordDto connectorRecordDto = new ConnectorRecordDto();
        HashMap<String, Object> queryMap = new HashMap<>();
        queryMap.put("connectionId", connectorRecordDto.getConnectionId());
        downloadCallback.upsertConnectorRecord(connectorRecordDto);
        Mockito.verify(clientMongoOperator, Mockito.times(1)).upsert(queryMap, MapUtil.obj2Map(connectorRecordDto), ConnectorConstant.CONNECTORRECORD_COLLECTION);
    }
    @Test
    void testUpsertConnectorRecordException(){
        ConnectorRecordDto connectorRecordDto = new ConnectorRecordDto();
        HashMap<String, Object> queryMap = new HashMap<>();
        queryMap.put("connectionId", connectorRecordDto.getConnectionId());
        Mockito.doAnswer(invocationOnMock -> {
            throw new IllegalAccessException("can not cast Map");
        }).when(clientMongoOperator).upsert(any(),any(),any());
        downloadCallback.upsertConnectorRecord(connectorRecordDto);
    }

}
