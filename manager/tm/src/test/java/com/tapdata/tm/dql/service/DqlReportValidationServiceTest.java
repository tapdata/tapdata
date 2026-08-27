package com.tapdata.tm.dql.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.dql.vo.DqlEventReportVo;
import com.tapdata.tm.task.repository.TaskRepository;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DqlReportValidationServiceTest {
    private static final String TASK_ID = "64f000000000000000000001";
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    @DisplayName("report validation rejects a task id that does not exist")
    void rejectsMissingTask() {
        TaskRepository taskRepository = mock(TaskRepository.class);
        DqlReportValidationService service = new DqlReportValidationService(taskRepository, objectMapper);
        when(taskRepository.existsById(new ObjectId(TASK_ID))).thenReturn(false);

        BizException exception = assertThrows(BizException.class,
                () -> service.validateAndSecure(TASK_ID, report(Map.of("id", 1))));

        assertEquals("Task.NotFound", exception.getErrorCode());
        verify(taskRepository).existsById(new ObjectId(TASK_ID));
    }

    @Test
    @DisplayName("report validation rejects blank and malformed task ids before repository lookup")
    void rejectsInvalidTaskIds() {
        DqlReportValidationService service = serviceWithoutTaskLookup();

        BizException blank = assertThrows(BizException.class,
                () -> service.validateAndSecure(" ", report(Map.of("id", 1))));
        BizException malformed = assertThrows(BizException.class,
                () -> service.validateAndSecure("not-an-object-id", report(Map.of("id", 1))));

        assertEquals("IllegalArgument", blank.getErrorCode());
        assertEquals("IllegalArgument", malformed.getErrorCode());
    }

    @Test
    @DisplayName("report validation normalizes omitted route metadata and rejects explicit non DLQ routes")
    void validatesRouteMetadata() {
        DqlReportValidationService service = serviceWithoutTaskLookup();
        DqlEventReportVo defaulted = report(Map.of("id", 1));

        service.validateAndSecure(TASK_ID, defaulted);

        assertEquals("RECORD", defaulted.getExceptionScope());
        assertEquals("RECORD_DLQ", defaulted.getRouteDecision());

        DqlEventReportVo invalidScope = report(Map.of("id", 1));
        invalidScope.setExceptionScope("TASK_SHARED");
        invalidScope.setRouteDecision("RECORD_DLQ");
        BizException scopeException = assertThrows(BizException.class,
                () -> service.validateAndSecure(TASK_ID, invalidScope));
        assertEquals("DqlEvent.InvalidRouteDecision", scopeException.getErrorCode());

        DqlEventReportVo invalidDecision = report(Map.of("id", 1));
        invalidDecision.setExceptionScope("RECORD");
        invalidDecision.setRouteDecision("TASK_RETRY");
        BizException decisionException = assertThrows(BizException.class,
                () -> service.validateAndSecure(TASK_ID, invalidDecision));
        assertEquals("DqlEvent.InvalidRouteDecision", decisionException.getErrorCode());
    }

    @Test
    @DisplayName("report validation truncates error details at the frozen B06 limit")
    void truncatesErrorDetails() {
        DqlReportValidationService service = serviceWithoutTaskLookup();
        DqlEventReportVo report = report(Map.of("id", 1));
        report.setErrorDetails("x".repeat(4001));

        DqlReportValidationService.ValidationResult result = service.validateAndSecure(TASK_ID, report);

        assertEquals(4000, report.getErrorDetails().length());
        assertTrue(result.errorDetailsTruncated());
    }

    @Test
    @DisplayName("report validation masks sensitive values in error details before persistence")
    void masksSensitiveErrorDetails() {
        DqlReportValidationService service = serviceWithoutTaskLookup();
        DqlEventReportVo report = report(Map.of("id", 1));
        report.setErrorDetails("{\"password\":\"json-secret\"}; token=plain-secret\nAuthorization: Bearer bearer-secret");

        DqlReportValidationService.ValidationResult result = service.validateAndSecure(TASK_ID, report);

        assertFalse(result.errorDetailsTruncated());
        assertEquals("{\"password\":\"******\"}; token=******\nAuthorization: ******", report.getErrorDetails());
        assertFalse(report.getErrorDetails().contains("json-secret"));
        assertFalse(report.getErrorDetails().contains("plain-secret"));
        assertFalse(report.getErrorDetails().contains("bearer-secret"));
    }

    @Test
    @DisplayName("report validation masks complete unquoted sensitive values through the end of the line")
    void masksCompleteUnquotedSensitiveErrorValues() {
        DqlReportValidationService service = serviceWithoutTaskLookup();
        DqlEventReportVo report = report(Map.of("id", 1));
        report.setErrorDetails("Authorization: Digest username=\"alice\", response=\"digest-secret\"\n"
                + "token=part1,part2;part3-must-not-leak");

        service.validateAndSecure(TASK_ID, report);

        assertEquals("Authorization: ******\ntoken=******", report.getErrorDetails());
        assertFalse(report.getErrorDetails().contains("digest-secret"));
        assertFalse(report.getErrorDetails().contains("part2"));
        assertFalse(report.getErrorDetails().contains("part3-must-not-leak"));
    }

    @Test
    @DisplayName("report validation masks structured and malformed sensitive values conservatively")
    void masksStructuredAndMalformedSensitiveErrorDetails() {
        DqlReportValidationService service = serviceWithoutTaskLookup();
        DqlEventReportVo structured = report(Map.of("id", 1));
        structured.setErrorDetails("{\"credential\":{\"user\":\"a\",\"privateKey\":\"object-secret\"},"
                + "\"token\":[\"array-secret\",{\"nested\":\"still-secret\"}],\"safe\":\"visible\"}");
        DqlEventReportVo malformed = report(Map.of("id", 1));
        malformed.setErrorDetails("token=\"part1,part2-must-not-leak");
        DqlEventReportVo malformedStructure = report(Map.of("id", 1));
        malformedStructure.setErrorDetails("credential={\"privateKey\":\"structure-secret\",\"nested\":[1,2]");

        service.validateAndSecure(TASK_ID, structured);
        service.validateAndSecure(TASK_ID, malformed);
        service.validateAndSecure(TASK_ID, malformedStructure);

        assertEquals("{\"credential\":******,\"token\":******,\"safe\":\"visible\"}", structured.getErrorDetails());
        assertFalse(structured.getErrorDetails().contains("object-secret"));
        assertFalse(structured.getErrorDetails().contains("array-secret"));
        assertFalse(structured.getErrorDetails().contains("still-secret"));
        assertEquals("token=\"******", malformed.getErrorDetails());
        assertFalse(malformed.getErrorDetails().contains("part2-must-not-leak"));
        assertEquals("credential=******", malformedStructure.getErrorDetails());
        assertFalse(malformedStructure.getErrorDetails().contains("structure-secret"));
    }

    @Test
    @DisplayName("report validation measures payload using serialized UTF-8 bytes instead of trusting reported size")
    void measuresPayloadUtf8Bytes() throws Exception {
        DqlReportValidationService service = serviceWithoutTaskLookup();
        Map<String, Object> payload = Map.of("message", "中文");
        DqlEventReportVo report = report(payload);
        report.setPayloadSize(1L);

        service.validateAndSecure(TASK_ID, report);

        assertEquals((long) objectMapper.writeValueAsBytes(payload).length, report.getPayloadSize());
        assertEquals(payload, report.getPayloadData());
        assertTrue(report.getPayloadComplete());
    }

    @Test
    @DisplayName("report validation stores only a safe summary when payload exceeds one MiB")
    void removesOversizedPayload() {
        DqlReportValidationService service = serviceWithoutTaskLookup();
        DqlEventReportVo report = report(Map.of("message", "中".repeat(400_000)));
        report.setPayloadSize(1L);

        service.validateAndSecure(TASK_ID, report);

        assertTrue(report.getPayloadSize() > 1_048_576L);
        assertNull(report.getPayloadData());
        assertFalse(report.getPayloadComplete());
        assertEquals(Map.of("id", 1), report.getPayloadPreview());
    }

    @Test
    @DisplayName("report validation fingerprints a payload before removing an oversized body")
    void fingerprintsOversizedPayloadBeforeRemoval() {
        DqlReportValidationService service = serviceWithoutTaskLookup();
        DqlEventReportVo report = report(Map.of("message", "中".repeat(400_000)));
        report.setPayloadHash(null);

        service.validateAndSecure(TASK_ID, report);

        assertNull(report.getPayloadData());
        assertTrue(report.getPayloadHash().startsWith("sha256:"));
    }

    @Test
    @DisplayName("report validation recursively masks sensitive preview fields")
    void masksSensitivePreviewFields() {
        DqlReportValidationService service = serviceWithoutTaskLookup();
        DqlEventReportVo report = report(Map.of("id", 1));
        Map<String, Object> sensitiveValues = new LinkedHashMap<>();
        List<String> sensitiveFields = List.of(
                "password", "passwd", "secret", "token", "access_token",
                "authorization", "credential", "APIKey"
        );
        sensitiveFields.forEach(field -> sensitiveValues.put(field, field + "-must-not-leak"));
        sensitiveValues.put("safe", "visible");
        report.setPayloadPreview(Map.of("nested", sensitiveValues));

        service.validateAndSecure(TASK_ID, report);

        Map<?, ?> nested = (Map<?, ?>) report.getPayloadPreview().get("nested");
        sensitiveFields.forEach(field -> assertEquals("******", nested.get(field)));
        assertEquals("visible", nested.get("safe"));
        assertFalse(report.getPayloadPreviewTruncated());
    }

    @Test
    @DisplayName("report validation limits preview strings, collection items, and nesting depth")
    void limitsPreviewShape() {
        DqlReportValidationService service = serviceWithoutTaskLookup();
        DqlEventReportVo report = report(Map.of("id", 1));
        Map<String, Object> preview = new LinkedHashMap<>();
        preview.put("longText", "x".repeat(513));
        preview.put("items", new ArrayList<>(List.copyOf(java.util.stream.IntStream.range(0, 55).boxed().toList())));
        Map<String, Object> largeMap = new LinkedHashMap<>();
        java.util.stream.IntStream.range(0, 55).forEach(index -> largeMap.put("field" + index, index));
        preview.put("largeMap", largeMap);
        preview.put("level1", Map.of("level2", Map.of("level3", Map.of("level4", Map.of(
                "level5", Map.of("password", "must-not-leak")
        )))));
        report.setPayloadPreview(preview);

        service.validateAndSecure(TASK_ID, report);

        assertEquals(512, ((String) report.getPayloadPreview().get("longText")).length());
        assertEquals(50, ((List<?>) report.getPayloadPreview().get("items")).size());
        assertEquals(50, ((Map<?, ?>) report.getPayloadPreview().get("largeMap")).size());
        Map<?, ?> level1 = (Map<?, ?>) report.getPayloadPreview().get("level1");
        Map<?, ?> level2 = (Map<?, ?>) level1.get("level2");
        Map<?, ?> level3 = (Map<?, ?>) level2.get("level3");
        Map<?, ?> level4 = (Map<?, ?>) level3.get("level4");
        assertEquals("...", level4.get("level5"));
        assertTrue(report.getPayloadPreviewTruncated());
        assertFalse(report.getPayloadPreview().toString().contains("must-not-leak"));
    }

    private DqlReportValidationService serviceWithoutTaskLookup() {
        return new DqlReportValidationService(null, objectMapper);
    }

    private DqlEventReportVo report(Object payload) {
        DqlEventReportVo report = new DqlEventReportVo();
        report.setPayloadData(payload);
        report.setPayloadSize(128L);
        report.setPayloadComplete(true);
        report.setPayloadPreview(new LinkedHashMap<>(Map.of("id", 1)));
        report.setErrorDetails("script failed");
        return report;
    }
}
