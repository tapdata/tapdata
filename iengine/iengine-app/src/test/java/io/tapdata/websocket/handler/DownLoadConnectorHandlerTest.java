package io.tapdata.websocket.handler;


import base.BaseTest;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.MapUtil;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.mongo.RestTemplateOperator;
import com.tapdata.tm.commons.metrics.ConnectorRecordDto;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.modules.api.pdk.PDKUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.*;

public class DownLoadConnectorHandlerTest extends BaseTest {

    @Test
    void testUpsertConnectorRecord() throws IllegalAccessException {
        DownLoadConnectorHandler downLoadConnectorHandler = spy(DownLoadConnectorHandler.class);
        HttpClientMongoOperator clientMongoOperator = Mockito.mock(HttpClientMongoOperator.class);
        downLoadConnectorHandler.initialize(clientMongoOperator,null);
        ConnectorRecordDto connectorRecordDto = new ConnectorRecordDto();
        downLoadConnectorHandler.upsertConnectorRecord(connectorRecordDto);
        HashMap<String, Object> queryMap = new HashMap<>();
        queryMap.put("connectionId", connectorRecordDto.getConnectionId());
        Mockito.verify(clientMongoOperator,Mockito.times(1)).upsert(queryMap, MapUtil.obj2Map(connectorRecordDto), ConnectorConstant.CONNECTORRECORD_COLLECTION);
    }
    @Nested
    class TestHandle{
        private DownLoadConnectorHandler downLoadConnectorHandler;
        private HttpClientMongoOperator httpClientMongoOperator;

        @BeforeEach
        void beforeEach(){
            downLoadConnectorHandler = spy(DownLoadConnectorHandler.class);
            httpClientMongoOperator=mock(HttpClientMongoOperator.class);
            downLoadConnectorHandler.clientMongoOperator = httpClientMongoOperator;
        }
        @Test
        void testHandle(){
            Map event=new HashMap<>();
            String pdkHash="4335aaa005ec1a74a4e2166bded2962e939ad50239f48b023b884f35b54129a5";
            String connectionId="658a8e3316adf05ab853f381";
            event.put("pdkHash",pdkHash);
            event.put("name","SourceMongo");
            event.put("id",connectionId);
            event.put("type","downLoadConnector");
            event.put("pdkType","pdk");
            event.put("schemaVersion","bedd269f-31c1-4f81-a86a-62863b5e14fe");
            event.put("database_type","MongoDB");
            DatabaseTypeEnum.DatabaseType databaseType=new DatabaseTypeEnum.DatabaseType();
            databaseType.setPdkHash(pdkHash);
            databaseType.setJarFile("mongodb-connector-v1.0-SNAPSHOT.jar");
            databaseType.setJarRid("658bd476be560938470cafa8");
            when(httpClientMongoOperator.findOne(anyMap(),any(),any())).thenReturn(databaseType);
            Object thead = downLoadConnectorHandler.handle(event, null);
            assertNotNull(thead);
            assertEquals(Thread.class, thead.getClass());
            assertDoesNotThrow(() -> ((Thread) thead).join());

        }
    }

}
