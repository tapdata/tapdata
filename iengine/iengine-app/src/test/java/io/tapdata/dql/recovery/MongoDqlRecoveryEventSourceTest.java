package io.tapdata.dql.recovery;

import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.tm.dql.vo.DqlRecoveryPayloadVo;
import io.tapdata.dql.model.DqlPayloadSnapshot;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.query.Query;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MongoDqlRecoveryEventSourceTest {

    @Test
    void loadsPayloadThroughTheDedicatedTmResource() {
        HttpClientMongoOperator operator = mock(HttpClientMongoOperator.class);
        DqlRecoveryPayloadVo payload = new DqlRecoveryPayloadVo();
        payload.setPayloadFormat("tap-record-event-json-v1");
        payload.setPayloadData(Map.of("after", Map.of("id", 1001)));
        payload.setPayloadHash("sha256:payload");
        payload.setPayloadSize(128L);
        payload.setPayloadComplete(true);
        when(operator.findOne(any(Query.class), eq("dql-events/DQL-1/recovery-payload"),
                eq(DqlRecoveryPayloadVo.class))).thenReturn(payload);

        DqlPayloadSnapshot snapshot = new MongoDqlRecoveryEventSource(operator).load("DQL-1");

        assertEquals(payload.getPayloadFormat(), snapshot.getPayloadFormat());
        assertEquals(payload.getPayloadData(), snapshot.getPayloadData());
        assertEquals(payload.getPayloadHash(), snapshot.getPayloadHash());
        assertEquals(payload.getPayloadSize(), snapshot.getPayloadSize());
        assertEquals(payload.getPayloadComplete(), snapshot.getPayloadComplete());
        verify(operator).findOne(any(Query.class), eq("dql-events/DQL-1/recovery-payload"),
                eq(DqlRecoveryPayloadVo.class));
    }
}
