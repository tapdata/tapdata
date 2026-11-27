package com.tapdata.tm.dql.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tapdata.tm.dql.DqlRecordIdentityTypeEnum;
import com.tapdata.tm.dql.vo.DqlEventReportVo;
import com.tapdata.tm.dql.vo.DqlRecordSuccessReportVo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DqlEventIdentityServiceTest {
    private static final String TASK_ID = "64f000000000000000000001";
    private final DqlEventIdentityService service = new DqlEventIdentityService(new ObjectMapper());

    @Test
    @DisplayName("Engine supplied identities remain authoritative")
    void preservesEngineSuppliedIdentities() {
        DqlEventReportVo report = report();
        report.setRecordIdentity("engine-record-identity");
        report.setRecordIdentityType(DqlRecordIdentityTypeEnum.UNIQUE_INDEX.name());
        report.setRecordIdentityFields(List.of("tenant_id", "order_no"));
        report.setEventIdentity("engine-event-identity");

        service.fillIdentities(TASK_ID, report);

        assertEquals("engine-record-identity", report.getRecordIdentity());
        assertEquals(DqlRecordIdentityTypeEnum.UNIQUE_INDEX.name(), report.getRecordIdentityType());
        assertEquals(List.of("tenant_id", "order_no"), report.getRecordIdentityFields());
        assertEquals("engine-event-identity", report.getEventIdentity());
    }

    @Test
    @DisplayName("exactly once id has precedence over source offset and key fallbacks")
    void prefersExactlyOnceId() {
        DqlEventReportVo report = report();
        report.setPayloadData(Map.of(
                "exactlyOnceId", "eo-1001",
                "info", Map.of("sourceOffset", Map.of("file", "mysql-bin.000001", "position", 42))
        ));

        service.fillIdentities(TASK_ID, report);

        assertEquals("eo:eo-1001", report.getEventIdentity());
    }

    @Test
    @DisplayName("source offset identity is stable for equivalent maps with different insertion order")
    void canonicalizesSourceOffset() {
        Map<String, Object> firstOffset = new LinkedHashMap<>();
        firstOffset.put("file", "mysql-bin.000001");
        firstOffset.put("position", 42);
        Map<String, Object> secondOffset = new LinkedHashMap<>();
        secondOffset.put("position", 42);
        secondOffset.put("file", "mysql-bin.000001");
        DqlEventReportVo first = report();
        first.setPayloadData(Map.of("info", Map.of("sourceOffset", firstOffset)));
        DqlEventReportVo second = report();
        second.setPayloadData(Map.of("info", Map.of("sourceOffset", secondOffset)));

        service.fillIdentities(TASK_ID, first);
        service.fillIdentities(TASK_ID, second);

        assertTrue(first.getEventIdentity().startsWith("offset:sha256:"));
        assertEquals(first.getEventIdentity(), second.getEventIdentity());
    }

    @Test
    @DisplayName("key and payload fallbacks are canonical and independent from map insertion order")
    void canonicalizesKeyAndPayloadFallbacks() {
        Map<String, Object> firstKey = new LinkedHashMap<>();
        firstKey.put("tenant_id", "tenant-1");
        firstKey.put("id", 1001);
        Map<String, Object> secondKey = new LinkedHashMap<>();
        secondKey.put("id", 1001);
        secondKey.put("tenant_id", "tenant-1");
        Map<String, Object> firstPayload = new LinkedHashMap<>();
        firstPayload.put("before", Map.of("status", "NEW"));
        firstPayload.put("after", Map.of("status", "PAID"));
        Map<String, Object> secondPayload = new LinkedHashMap<>();
        secondPayload.put("after", Map.of("status", "PAID"));
        secondPayload.put("before", Map.of("status", "NEW"));
        DqlEventReportVo first = report();
        first.setEventKey(firstKey);
        first.setPayloadData(firstPayload);
        DqlEventReportVo second = report();
        second.setEventKey(secondKey);
        second.setPayloadData(secondPayload);

        service.fillIdentities(TASK_ID, first);
        service.fillIdentities(TASK_ID, second);

        assertEquals(first.getPayloadHash(), second.getPayloadHash());
        assertTrue(first.getPayloadHash().startsWith("sha256:"));
        assertEquals(first.getRecordIdentity(), second.getRecordIdentity());
        assertTrue(first.getRecordIdentity().startsWith("key:orders:sha256:"));
        assertEquals(List.of("id", "tenant_id"), first.getRecordIdentityFields());
        assertEquals(DqlRecordIdentityTypeEnum.PRIMARY_KEY.name(), first.getRecordIdentityType());
        assertEquals(first.getEventIdentity(), second.getEventIdentity());
        assertTrue(first.getEventIdentity().startsWith("key:record-1:orders:U:1787580000000:sha256:"));
    }

    @Test
    @DisplayName("payload fallback changes when the canonical payload changes")
    void generatesPayloadFallback() {
        DqlEventReportVo first = report();
        first.setEventKey(null);
        first.setPayloadData(Map.of("after", Map.of("id", 1001)));
        DqlEventReportVo second = report();
        second.setEventKey(null);
        second.setPayloadData(Map.of("after", Map.of("id", 1002)));

        service.fillIdentities(TASK_ID, first);
        service.fillIdentities(TASK_ID, second);

        assertTrue(first.getRecordIdentity().startsWith("hash:orders:sha256:"));
        assertEquals(DqlRecordIdentityTypeEnum.FULL_FIELD_HASH.name(), first.getRecordIdentityType());
        assertTrue(first.getEventIdentity().startsWith("payload:record-1:orders:U:1787580000000:sha256:"));
        assertNotEquals(first.getEventIdentity(), second.getEventIdentity());
    }

    @Test
    @DisplayName("normal success callback uses the same record identity fallback as DLQ reporting")
    void reusesRecordIdentityForSuccessCallback() {
        Map<String, Object> eventKey = Map.of("tenant_id", "tenant-1", "id", 1001);
        DqlEventReportVo eventReport = report();
        eventReport.setEventKey(eventKey);
        DqlRecordSuccessReportVo successReport = new DqlRecordSuccessReportVo();
        successReport.setTableId("orders");
        successReport.setEventKey(eventKey);

        service.fillIdentities(TASK_ID, eventReport);
        service.fillRecordIdentity(successReport);

        assertEquals(eventReport.getRecordIdentity(), successReport.getRecordIdentity());
        assertEquals(eventReport.getRecordIdentityType(), successReport.getRecordIdentityType());
        assertEquals(eventReport.getRecordIdentityFields(), successReport.getRecordIdentityFields());
    }

    @Test
    @DisplayName("update-condition identity type is preserved by TM fallback")
    void preservesUpdateConditionIdentityTypeWhenEngineIdentityIsIncomplete() {
        DqlEventReportVo report = report();
        report.setRecordIdentity(null);
        report.setRecordIdentityType(DqlRecordIdentityTypeEnum.UPDATE_CONDITION.name());
        report.setRecordIdentityFields(List.of("external_id"));
        report.setEventKey(Map.of("external_id", "EXT-1"));
        report.setEventIdentity(null);

        service.fillIdentities(TASK_ID, report);

        assertEquals(DqlRecordIdentityTypeEnum.UPDATE_CONDITION.name(), report.getRecordIdentityType());
        assertEquals(List.of("external_id"), report.getRecordIdentityFields());
        assertTrue(report.getEventIdentity().startsWith("key:record-1:orders:U:"));
    }

    private DqlEventReportVo report() {
        DqlEventReportVo report = new DqlEventReportVo();
        report.setTaskRecordId("record-1");
        report.setTableId("orders");
        report.setSourceTable("orders");
        report.setDmlType("U");
        report.setEventTime(1787580000000L);
        report.setFailedNodeId("js-node");
        report.setEventKey(Map.of("id", 1001));
        report.setPayloadData(Map.of("after", Map.of("id", 1001)));
        return report;
    }
}
