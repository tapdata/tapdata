package com.tapdata.tm.dql.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tapdata.tm.dql.DqlEventStatusEnum;
import com.tapdata.tm.dql.dto.DqlEventDto;
import com.tapdata.tm.dql.dto.DqlRecoveryAttemptDto;
import com.tapdata.tm.dql.vo.DqlEventDetailVo;
import com.tapdata.tm.dql.vo.DqlEventListVo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DqlEventWebMapperTest {
    private final DqlEventWebMapper mapper = new DqlEventWebMapper();

    @Test
    @DisplayName("list mapping contains only the public summary fields")
    void listMappingOmitsPersistenceOnlyFields() throws Exception {
        DqlEventDto event = event();
        event.setPayloadData(Map.of("after", Map.of("id", 1001)));
        event.setEventIdentity("engine-event");
        event.setRecordIdentity("key:orders:id=1001");
        event.setErrorDetails("internal details");

        DqlEventListVo result = mapper.toList(event);
        String json = new ObjectMapper().writeValueAsString(result);

        assertEquals(event.getEventId(), result.getEventId());
        assertEquals(event.getStatus(), result.getStatus());
        assertFalse(json.contains("payloadData"));
        assertFalse(json.contains("eventIdentity"));
        assertFalse(json.contains("recordIdentity"));
        assertFalse(json.contains("errorDetails"));
    }

    @Test
    @DisplayName("detail mapping masks nested preview values and keeps only recent attempts")
    void detailMappingSanitizesPreviewAndAttempts() throws Exception {
        DqlEventDto event = event();
        event.setEventKey(new LinkedHashMap<>(Map.of("id", 1001, "password", "secret")));
        Map<String, Object> preview = new LinkedHashMap<>();
        preview.put("token", "token-secret");
        preview.put("longText", "x".repeat(513));
        preview.put("nested", Map.of("api-key", "api-secret", "safe", "visible"));
        event.setPayloadPreview(preview);
        List<DqlRecoveryAttemptDto> attempts = new ArrayList<>();
        for (int index = 0; index < 21; index++) {
            DqlRecoveryAttemptDto attempt = new DqlRecoveryAttemptDto();
            attempt.setAttemptId("A-" + index);
            attempt.setErrorDetails("error-" + index);
            attempts.add(attempt);
        }
        event.setRecoveryAttempts(attempts);

        DqlEventDetailVo result = mapper.toDetail(event);

        assertEquals(event.getStatus(), result.getStatus());
        assertEquals("source-node-1", result.getSourceNodeId());
        assertEquals("source-node-name", result.getSourceNodeName());
        assertEquals("target-node-1", result.getTargetNodeId());
        assertEquals("target-node-name", result.getTargetNodeName());
        assertEquals("failed-node-1", result.getFailedNodeId());
        assertEquals("failed-node-name", result.getFailedNodeName());
        String json = new ObjectMapper().writeValueAsString(result);
        assertTrue(json.contains("\"sourceNodeId\":\"source-node-1\""));
        assertTrue(json.contains("\"sourceNodeName\":\"source-node-name\""));
        assertTrue(json.contains("\"targetNodeId\":\"target-node-1\""));
        assertTrue(json.contains("\"targetNodeName\":\"target-node-name\""));
        assertTrue(json.contains("\"failedNodeId\":\"failed-node-1\""));
        assertTrue(json.contains("\"failedNodeName\":\"failed-node-name\""));
        assertEquals("{\"id\":1001,\"password\":\"******\"}", result.getEventKey());
        assertEquals("******", result.getPayloadPreview().get("token"));
        assertEquals(512, ((String) result.getPayloadPreview().get("longText")).length());
        assertEquals("******", ((Map<?, ?>) result.getPayloadPreview().get("nested")).get("api-key"));
        assertEquals("visible", ((Map<?, ?>) result.getPayloadPreview().get("nested")).get("safe"));
        assertTrue(result.getPayloadPreviewTruncated());
        assertEquals(20, result.getRecoveryAttempts().size());
        assertEquals("A-20", result.getRecoveryAttempts().get(0).getAttemptId());
        assertEquals("error-20", result.getRecoveryAttempts().get(0).getErrorMessage());
        assertEquals("A-1", result.getRecoveryAttempts().get(19).getAttemptId());
    }

    @Test
    @DisplayName("detail mapping keeps empty attempt history and null payload preview distinguishable")
    void detailMappingPreservesEmptyOptionalCollections() {
        DqlEventDto event = event();
        event.setPayloadPreview(null);
        event.setRecoveryAttempts(List.of());

        DqlEventDetailVo result = mapper.toDetail(event);

        assertNull(result.getPayloadPreview());
        assertFalse(result.getPayloadPreviewTruncated());
        assertTrue(result.getRecoveryAttempts().isEmpty());
    }

    private DqlEventDto event() {
        DqlEventDto event = new DqlEventDto();
        event.setId("64f000000000000000000001");
        event.setEventId("DQL-1");
        event.setTaskId("64f000000000000000000002");
        event.setTaskName("sync_order");
        event.setSourceTable("orders");
        event.setTargetTable("orders_sink");
        event.setDmlType("U");
        event.setErrorType("TRANSFORM_ERROR");
        event.setErrorCode("JS_PROCESS_FAILED");
        event.setEventTime(new Date(1000L));
        event.setFailedAt(new Date(2000L));
        event.setCaptureSeq(3L);
        event.setStatus(DqlEventStatusEnum.PENDING.name());
        event.setRecoveryCount(0);
        event.setPayloadComplete(true);
        event.setSourceNodeId("source-node-1");
        event.setSourceNodeName("source-node-name");
        event.setTargetNodeId("target-node-1");
        event.setTargetNodeName("target-node-name");
        event.setFailedNodeId("failed-node-1");
        event.setFailedNodeName("failed-node-name");
        return event;
    }
}
