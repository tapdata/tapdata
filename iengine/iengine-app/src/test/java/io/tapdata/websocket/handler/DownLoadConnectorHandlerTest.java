package io.tapdata.websocket.handler;


import base.BaseTest;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.tm.commons.metrics.ConnectorRecordDto;
import io.tapdata.aspect.supervisor.AspectRunnableUtil;
import io.tapdata.callback.DownloadCallback;
import io.tapdata.callback.impl.DownloadCallbackImpl;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import jnr.constants.platform.PRIO;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class DownLoadConnectorHandlerTest extends BaseTest {

    @Nested
    class TestHandle {
        private DownLoadConnectorHandler downLoadConnectorHandler;
        private HttpClientMongoOperator httpClientMongoOperator;
        private DownloadCallback callback;

        @BeforeEach
        void beforeEach() {
            downLoadConnectorHandler = spy(DownLoadConnectorHandler.class);
            httpClientMongoOperator = mock(HttpClientMongoOperator.class);
            callback = mock(DownloadCallbackImpl.class);
            downLoadConnectorHandler.clientMongoOperator = httpClientMongoOperator;
        }

        @Test
        void testHandle() {
            try(MockedStatic<AspectRunnableUtil> aspectRunnableUtilMockedStatic = mockStatic(AspectRunnableUtil.class)) {
                aspectRunnableUtilMockedStatic.when(()->AspectRunnableUtil.aspectRunnable(any(),any())).thenReturn(new Runnable() {
                    @Override
                    public void run() {

                    }
                });
                Map event = new HashMap<>();
                String pdkHash = "4335aaa005ec1a74a4e2166bded2962e939ad50239f48b023b884f35b54129a5";
                String connectionId = "658a8e3316adf05ab853f381";
                event.put("pdkHash", pdkHash);
                event.put("name", "SourceMongo");
                event.put("id", connectionId);
                event.put("type", "downLoadConnector");
                event.put("pdkType", "pdk");
                event.put("schemaVersion", "bedd269f-31c1-4f81-a86a-62863b5e14fe");
                event.put("database_type", "MongoDB");
                DatabaseTypeEnum.DatabaseType databaseType = new DatabaseTypeEnum.DatabaseType();
                databaseType.setPdkHash(pdkHash);
                databaseType.setJarFile("mongodb-connector-v1.0-SNAPSHOT.jar");
                databaseType.setJarRid("658bd476be560938470cafa8");
                Object thead = downLoadConnectorHandler.handle(event, null);
                assertNotNull(thead);
                assertEquals(Thread.class, thead.getClass());
                assertDoesNotThrow(() -> ((Thread) thead).join());
            }
        }
        @Test
        void testDownloadPdkFileIfNeedPrivate(){
            String connectionId="123";
            String connName="abc";
            try(MockedStatic<PdkUtil> pdkUtilMockedStatic = mockStatic(PdkUtil.class)){
                DatabaseTypeEnum.DatabaseType databaseDefinition = new DatabaseTypeEnum.DatabaseType();
                pdkUtilMockedStatic.when(()->{PdkUtil.downloadPdkFileIfNeed(any(),any(),any(),any(),any());}).thenThrow(new RuntimeException("downloadFaild"));
                doAnswer(invocationOnMock -> {
                    ConnectorRecordDto connectorRecordDto=invocationOnMock.getArgument(0);
                    assertEquals(connectionId,connectorRecordDto.getConnectionId());
                    assertEquals(ConnectorRecordDto.StatusEnum.FAIL.getStatus(),connectorRecordDto.getStatus());
                    assertEquals("downloadFaild",connectorRecordDto.getDownFiledMessage());
                    assertTrue(connectorRecordDto.getFlag());
                    return null;
                }).when((DownloadCallbackImpl)callback).upsertConnectorRecord(any());
                downLoadConnectorHandler.downloadPdkFileIfNeedPrivate(null,connName,connectionId,databaseDefinition,callback);
            };
        }
    }
}
