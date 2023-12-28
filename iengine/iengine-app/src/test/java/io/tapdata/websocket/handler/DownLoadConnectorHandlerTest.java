package io.tapdata.websocket.handler;


import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.MapUtil;
import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.tm.commons.metrics.ConnectorRecordDto;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;

public class DownLoadConnectorHandlerTest {

    @Nested
    class CallBackTest {

        @BeforeEach
        void beforeEach(){

        }
    }
    @Test
    void upsertConnectorRecord()  {
        DownLoadConnectorHandler downLoadConnectorHandler = Mockito.spy(DownLoadConnectorHandler.class);
        HttpClientMongoOperator clientMongoOperator = Mockito.mock(HttpClientMongoOperator.class);
        downLoadConnectorHandler.initialize(clientMongoOperator,null);
        ConnectorRecordDto connectorRecordDto = new ConnectorRecordDto();
        try{
            downLoadConnectorHandler.upsertConnectorRecord(connectorRecordDto);
            HashMap<String, Object> queryMap = new HashMap<>();
            queryMap.put("connectionId", connectorRecordDto.getConnectionId());
            Mockito.verify(clientMongoOperator,Mockito.times(1)).upsert(queryMap, MapUtil.obj2Map(connectorRecordDto), ConnectorConstant.CONNECTORRECORD_COLLECTION);
        }catch (Exception e){

        }
    }

}
