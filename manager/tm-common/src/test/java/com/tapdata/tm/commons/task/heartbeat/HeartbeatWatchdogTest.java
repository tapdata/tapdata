package com.tapdata.tm.commons.task.heartbeat;

import com.tapdata.tm.commons.task.dto.TaskDto;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class HeartbeatWatchdogTest {
    private TaskDto task() {
        TaskDto task = new TaskDto();
        task.setId(new ObjectId());
        task.setStatus(TaskDto.STATUS_RUNNING);
        task.setAgentId("agent");
        task.setSyncType(TaskDto.SYNC_TYPE_SYNC);
        task.setAttrs(new HashMap<>());
        task.getAttrs().put(HeartbeatWatchdog.CONFIG, new HashMap<>(Map.of("enabled", true,
                "units", Arrays.asList("a", "b"), "timeoutMs", 60_000L, "graceMs", 90_000L)));
        return task;
    }
    private String progress(int offset) {
        return "{\"syncStage\":\"CDC\",\"streamOffset\":\"" + offset + "\",\"sourceTime\":" + offset + "}";
    }

    @Test void disabledWithoutExplicitHeartbeatContract() {
        TaskDto task = task();
        assertTrue(HeartbeatWatchdog.enabled(task));
        task.getAttrs().clear();
        assertFalse(HeartbeatWatchdog.enabled(task));
    }

    @Test void noDmlButHeartbeatsAdvanceNeverStalls() {
        TaskDto task = task();
        HeartbeatWatchdog.Detector detector = new HeartbeatWatchdog.Detector(0);
        for (int i = 0; i < 100; i++) {
            assertTrue(detector.inspect(task, Map.of("a", progress(i)), i * 30_000L).isEmpty());
        }
    }

    @Test void respectsGraceAndDoesNotLetAnotherEdgeMaskStall() {
        TaskDto task = task();
        HeartbeatWatchdog.Detector detector = new HeartbeatWatchdog.Detector(0);
        assertTrue(detector.inspect(task, Map.of("a", progress(1), "b", progress(1)), 0).isEmpty());
        assertTrue(detector.inspect(task, Map.of("a", progress(1), "b", progress(2)), 60_000).isEmpty());
        assertEquals(Set.of("a"), detector.inspect(task, Map.of("a", progress(1), "b", progress(3)), 90_000).keySet());
    }

    @Test void snapshotAndMalformedProgressAreUnknownNotStalled() {
        TaskDto task = task();
        HeartbeatWatchdog.Detector detector = new HeartbeatWatchdog.Detector(0);
        Map<String, String> progress = Map.of("a", "bad-json", "b", "{\"syncStage\":\"INITIAL_SYNC\"}");
        detector.inspect(task, progress, 0);
        assertTrue(detector.inspect(task, progress, 500_000).isEmpty());
    }

    @Test void reenteringCdcAndNewRunsGetFreshObservationWindow() {
        TaskDto task = task();
        HeartbeatWatchdog.Detector detector = new HeartbeatWatchdog.Detector(0);
        detector.inspect(task, Map.of("a", progress(1)), 0);
        detector.inspect(task, Map.of(), 300_000);
        assertTrue(detector.inspect(task, Map.of("a", progress(1)), 400_000).isEmpty());
        assertTrue(new HeartbeatWatchdog.Detector(500_000).inspect(task, Map.of("a", progress(1)), 500_000).isEmpty());
    }

    @Test void reuploadingIdenticalCheckpointDoesNotRefreshProgress() {
        TaskDto task = task();
        HeartbeatWatchdog.Detector detector = new HeartbeatWatchdog.Detector(0);
        detector.inspect(task, Map.of("a", progress(1)), 0);
        assertEquals(1, detector.inspect(task, Map.of("a", progress(1)), 90_000).size());
    }

    @Test void jsonAndMapFormsProduceTheSameFingerprint() {
        assertEquals(HeartbeatWatchdog.fingerprint(progress(1)), HeartbeatWatchdog.fingerprint(
                Map.of("syncStage", "CDC", "streamOffset", "1", "sourceTime", 1L)));
    }

    @Test void heartbeatTimestampsWithOldResumeOffsetCannotMaskStall() {
        TaskDto task = task();
        HeartbeatWatchdog.Detector detector = new HeartbeatWatchdog.Detector(0);
        detector.inspect(task, Map.of("a", progress(1)), 0);
        String onlyTimeChanged = "{\"syncStage\":\"CDC\",\"streamOffset\":\"1\",\"sourceTime\":999,\"eventTime\":999}";
        assertEquals(Set.of("a"), detector.inspect(task, Map.of("a", onlyTimeChanged), 90_000).keySet());
    }

    @Test void signaturesAreBoundedAndExcludeUnrelatedMetadata() {
        String large = "{\"syncStage\":\"CDC\",\"streamOffset\":\"" + "x".repeat(100_000) + "\"}";
        assertEquals(64, HeartbeatWatchdog.fingerprint(large).length());
        assertEquals(HeartbeatWatchdog.fingerprint(progress(1)), HeartbeatWatchdog.fingerprint(progress(1).replace("}", ",\"uploadedAt\":9}")));
    }

    @Test void budgetSurvivesProgressAndExpiresAsRollingWindow() {
        TaskDto task = task();
        task.getAttrs().put(HeartbeatWatchdog.RECOVERY, Map.of("state", "RECOVERED", "attempts", Arrays.asList(100L, 200L, 300L)));
        assertNull(HeartbeatWatchdog.request(task, Map.of("a", "digest"), "ENGINE", 1000));
        assertNotNull(HeartbeatWatchdog.request(task, Map.of("a", "digest"), "TM", HeartbeatWatchdog.WINDOW_MS + 301));
    }

    @Test void inFlightAndUnconfirmedStopsCannotBeRetried() {
        TaskDto task = task();
        for (String state : Arrays.asList("REQUESTED", "STOPPING", "VERIFYING", "BLOCKED")) {
            task.getAttrs().put(HeartbeatWatchdog.RECOVERY, Map.of("state", state));
            assertNull(HeartbeatWatchdog.request(task, Map.of("a", "digest"), "TM", 1000));
        }
    }

    @Test void verificationRequiresAllAffectedEdgesToAdvance() {
        TaskDto task = task();
        Map<String, Object> request = Map.of("fingerprints", Map.of("a", HeartbeatWatchdog.fingerprint(progress(1)), "b", HeartbeatWatchdog.fingerprint(progress(1))));
        task.getAttrs().put("syncProgress", Map.of("a", progress(2), "b", progress(1)));
        assertFalse(HeartbeatWatchdog.advanced(task, request));
        task.getAttrs().put("syncProgress", Map.of("a", progress(2), "b", progress(2)));
        assertTrue(HeartbeatWatchdog.advanced(task, request));
    }

    @Test void compareAndSetScopesStatusAgentRecordConfigAndPreviousRecovery() {
        TaskDto task = task();
        Map<String, Object> old = Map.of("id", "request-1", "state", "REQUESTED");
        var query = HeartbeatRecoveryProtocol.compare(task, old).getQueryObject();
        assertEquals(TaskDto.STATUS_RUNNING, query.get("status"));
        assertEquals("agent", query.get("agentId"));
        assertTrue(query.containsKey("taskRecordId"));
        assertEquals("request-1", query.get(HeartbeatWatchdog.RECOVERY_PATH + ".id"));
        assertEquals("REQUESTED", query.get(HeartbeatWatchdog.RECOVERY_PATH + ".state"));
        assertTrue(query.containsKey("attrs." + HeartbeatWatchdog.CONFIG + ".units"));
    }
}
