package com.tapdata.tm.dql.dto;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class DqlRecoveryMessageDtoTest {

    @Test
    void buildsIndependentRecoveryMessagePayloadFromBatch() {
        DqlRecoveryBatchDto batch = new DqlRecoveryBatchDto();
        batch.setTaskId("task-1");
        batch.setBatchId("DQLB-1");
        batch.setTaskVersion(8L);
        batch.setOrderedEventIds(List.of("DQL-1", "DQL-2"));
        batch.setOperatorId("user-1");
        batch.setOperatorName("admin");

        DqlRecoveryMessageDto message = DqlRecoveryMessageDto.fromBatch(batch);
        Map<String, Object> payload = message.toPayload();

        assertEquals(DqlRecoveryMessageDto.TYPE, payload.get("type"));
        assertEquals("task-1", payload.get("taskId"));
        assertEquals("DQLB-1", payload.get("batchId"));
        assertEquals(8L, payload.get("taskVersion"));
        assertEquals(List.of("DQL-1", "DQL-2"), payload.get("orderedEventIds"));
        assertEquals("user-1", payload.get("operatorId"));
        assertEquals("admin", payload.get("operatorName"));
        assertEquals(DqlRecoveryMessageDto.MODE_AUTO, payload.get("mode"));
        assertFalse(payload.containsKey("eventIds"));
        assertFalse(payload.containsKey("opType"));
    }

    @Test
    void preservesBatchModeWhenBuildingRecoveryMessage() {
        DqlRecoveryBatchDto batch = new DqlRecoveryBatchDto();
        batch.setMode("AUTO-CHECKED");

        assertEquals("AUTO-CHECKED", DqlRecoveryMessageDto.fromBatch(batch).getMode());
    }
}
