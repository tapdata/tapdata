package com.tapdata.tm.dql.config;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DqlRuntimeConfigTest {
    @AfterEach
    void clearOverrides() {
        System.clearProperty(DqlRuntimeConfig.EVENT_ENABLED);
    }

    @Test
    void usesFrozenDefaults() {
        DqlRuntimeConfig config = DqlRuntimeConfig.fromMap(Map.of());

        assertTrue(config.isEventEnabled());
        assertEquals(4_000, config.getErrorDetailsMaxLength());
        assertEquals(1_048_576L, config.getPayloadMaxBytes());
        assertEquals(512, config.getPreviewFieldMaxLength());
        assertEquals(4, config.getPreviewMaxDepth());
        assertEquals(50, config.getPreviewMaxItems());
        assertEquals(200, config.getRecoveryBatchMaxSize());
        assertEquals(60L, config.getRecoveryEventTimeoutSeconds());
        assertEquals(1_800L, config.getRecoveryBatchTimeoutSeconds());
        assertEquals(60L, config.getRecoveryDispatchTimeoutSeconds());
        assertEquals(5L, config.getRecoveryHeartbeatIntervalSeconds());
        assertEquals(60L, config.getRecoveryHeartbeatTimeoutSeconds());
        assertTrue(config.isRecoveryContinueOnEventFailure());
        assertEquals(60L, config.getUnknownGuardWindowSeconds());
        assertEquals(20L, config.getUnknownGuardMaxEvents());
        assertEquals(0.2d, config.getUnknownGuardMaxBatchRatio());
        assertEquals("TASK_RETRY", config.getUnknownGuardDecision());
    }

    @Test
    void readsAllSupportedOverrides() {
        DqlRuntimeConfig config = DqlRuntimeConfig.fromMap(Map.ofEntries(
                Map.entry(DqlRuntimeConfig.EVENT_ENABLED, "false"),
                Map.entry(DqlRuntimeConfig.ERROR_DETAILS_MAX_LENGTH, "8000"),
                Map.entry(DqlRuntimeConfig.PAYLOAD_MAX_BYTES, "2097152"),
                Map.entry(DqlRuntimeConfig.PREVIEW_FIELD_MAX_LENGTH, "1024"),
                Map.entry(DqlRuntimeConfig.PREVIEW_MAX_DEPTH, "8"),
                Map.entry(DqlRuntimeConfig.PREVIEW_MAX_ITEMS, "100"),
                Map.entry(DqlRuntimeConfig.RECOVERY_BATCH_MAX_SIZE, "500"),
                Map.entry(DqlRuntimeConfig.RECOVERY_EVENT_TIMEOUT_SECONDS, "90"),
                Map.entry(DqlRuntimeConfig.RECOVERY_BATCH_TIMEOUT_SECONDS, "3600"),
                Map.entry(DqlRuntimeConfig.RECOVERY_DISPATCH_TIMEOUT_SECONDS, "75"),
                Map.entry(DqlRuntimeConfig.RECOVERY_HEARTBEAT_INTERVAL_SECONDS, "10"),
                Map.entry(DqlRuntimeConfig.RECOVERY_HEARTBEAT_TIMEOUT_SECONDS, "120"),
                Map.entry(DqlRuntimeConfig.RECOVERY_CONTINUE_ON_EVENT_FAILURE, "false"),
                Map.entry(DqlRuntimeConfig.UNKNOWN_GUARD_WINDOW_SECONDS, "120"),
                Map.entry(DqlRuntimeConfig.UNKNOWN_GUARD_MAX_EVENTS, "40"),
                Map.entry(DqlRuntimeConfig.UNKNOWN_GUARD_MAX_BATCH_RATIO, "0.5"),
                Map.entry(DqlRuntimeConfig.UNKNOWN_GUARD_DECISION, "task_error")));

        assertFalse(config.isEventEnabled());
        assertEquals(8_000, config.getErrorDetailsMaxLength());
        assertEquals(2_097_152L, config.getPayloadMaxBytes());
        assertEquals(1_024, config.getPreviewFieldMaxLength());
        assertEquals(8, config.getPreviewMaxDepth());
        assertEquals(100, config.getPreviewMaxItems());
        assertEquals(500, config.getRecoveryBatchMaxSize());
        assertEquals(90L, config.getRecoveryEventTimeoutSeconds());
        assertEquals(3_600L, config.getRecoveryBatchTimeoutSeconds());
        assertEquals(75L, config.getRecoveryDispatchTimeoutSeconds());
        assertEquals(10L, config.getRecoveryHeartbeatIntervalSeconds());
        assertEquals(120L, config.getRecoveryHeartbeatTimeoutSeconds());
        assertFalse(config.isRecoveryContinueOnEventFailure());
        assertEquals(120L, config.getUnknownGuardWindowSeconds());
        assertEquals(40L, config.getUnknownGuardMaxEvents());
        assertEquals(0.5d, config.getUnknownGuardMaxBatchRatio());
        assertEquals("TASK_ERROR", config.getUnknownGuardDecision());
    }

    @Test
    void rejectsUnsafeOverridesIndividually() {
        DqlRuntimeConfig config = DqlRuntimeConfig.fromMap(Map.ofEntries(
                Map.entry(DqlRuntimeConfig.EVENT_ENABLED, "not-a-boolean"),
                Map.entry(DqlRuntimeConfig.ERROR_DETAILS_MAX_LENGTH, "0"),
                Map.entry(DqlRuntimeConfig.PAYLOAD_MAX_BYTES, "-1"),
                Map.entry(DqlRuntimeConfig.PREVIEW_FIELD_MAX_LENGTH, "-1"),
                Map.entry(DqlRuntimeConfig.PREVIEW_MAX_DEPTH, "33"),
                Map.entry(DqlRuntimeConfig.PREVIEW_MAX_ITEMS, "0"),
                Map.entry(DqlRuntimeConfig.RECOVERY_BATCH_MAX_SIZE, "10001"),
                Map.entry(DqlRuntimeConfig.RECOVERY_EVENT_TIMEOUT_SECONDS, "0"),
                Map.entry(DqlRuntimeConfig.RECOVERY_BATCH_TIMEOUT_SECONDS, "-1"),
                Map.entry(DqlRuntimeConfig.RECOVERY_DISPATCH_TIMEOUT_SECONDS, "0"),
                Map.entry(DqlRuntimeConfig.RECOVERY_HEARTBEAT_INTERVAL_SECONDS, "-1"),
                Map.entry(DqlRuntimeConfig.RECOVERY_HEARTBEAT_TIMEOUT_SECONDS, "0"),
                Map.entry(DqlRuntimeConfig.RECOVERY_CONTINUE_ON_EVENT_FAILURE, "maybe"),
                Map.entry(DqlRuntimeConfig.UNKNOWN_GUARD_WINDOW_SECONDS, "0"),
                Map.entry(DqlRuntimeConfig.UNKNOWN_GUARD_MAX_EVENTS, "0"),
                Map.entry(DqlRuntimeConfig.UNKNOWN_GUARD_MAX_BATCH_RATIO, "1.1"),
                Map.entry(DqlRuntimeConfig.UNKNOWN_GUARD_DECISION, "RECORD_DLQ")));

        assertTrue(config.isEventEnabled());
        assertEquals(DqlRuntimeConfig.defaults().getErrorDetailsMaxLength(), config.getErrorDetailsMaxLength());
        assertEquals(DqlRuntimeConfig.defaults().getPayloadMaxBytes(), config.getPayloadMaxBytes());
        assertEquals(DqlRuntimeConfig.defaults().getPreviewFieldMaxLength(), config.getPreviewFieldMaxLength());
        assertEquals(DqlRuntimeConfig.defaults().getPreviewMaxDepth(), config.getPreviewMaxDepth());
        assertEquals(DqlRuntimeConfig.defaults().getPreviewMaxItems(), config.getPreviewMaxItems());
        assertEquals(DqlRuntimeConfig.defaults().getRecoveryBatchMaxSize(), config.getRecoveryBatchMaxSize());
        assertEquals(DqlRuntimeConfig.defaults().getRecoveryEventTimeoutSeconds(), config.getRecoveryEventTimeoutSeconds());
        assertEquals(DqlRuntimeConfig.defaults().getRecoveryBatchTimeoutSeconds(), config.getRecoveryBatchTimeoutSeconds());
        assertEquals(DqlRuntimeConfig.defaults().getRecoveryDispatchTimeoutSeconds(), config.getRecoveryDispatchTimeoutSeconds());
        assertEquals(DqlRuntimeConfig.defaults().getRecoveryHeartbeatIntervalSeconds(), config.getRecoveryHeartbeatIntervalSeconds());
        assertEquals(DqlRuntimeConfig.defaults().getRecoveryHeartbeatTimeoutSeconds(), config.getRecoveryHeartbeatTimeoutSeconds());
        assertTrue(config.isRecoveryContinueOnEventFailure());
        assertEquals(DqlRuntimeConfig.defaults().getUnknownGuardWindowSeconds(), config.getUnknownGuardWindowSeconds());
        assertEquals(DqlRuntimeConfig.defaults().getUnknownGuardMaxEvents(), config.getUnknownGuardMaxEvents());
        assertEquals(DqlRuntimeConfig.defaults().getUnknownGuardMaxBatchRatio(), config.getUnknownGuardMaxBatchRatio());
        assertEquals("TASK_RETRY", config.getUnknownGuardDecision());
    }

    @Test
    void systemPropertyWinsOverPersistedSetting() {
        System.setProperty(DqlRuntimeConfig.EVENT_ENABLED, "false");

        DqlRuntimeConfig config = DqlRuntimeConfig.fromMap(Map.of(
                DqlRuntimeConfig.EVENT_ENABLED, "true"));

        assertFalse(config.isEventEnabled());
    }
}
