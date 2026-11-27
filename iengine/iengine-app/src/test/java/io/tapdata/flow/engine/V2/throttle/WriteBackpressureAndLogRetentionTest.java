package io.tapdata.flow.engine.V2.throttle;

import com.tapdata.tm.commons.throttle.LogPositionStatus;
import com.tapdata.tm.commons.throttle.WaitReason;
import io.tapdata.observable.metric.throttle.MonotonicClock;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class WriteBackpressureAndLogRetentionTest {

    @Test
    void writeBackpressureTargetsOnlyDependentSourcesAndResumesOnClear() {
        AgentMemoryBudgetPool pool = new AgentMemoryBudgetPool(100_000L, 1_000L, 100L, 0L, MonotonicClock.SYSTEM);
        InFlightBudget budget = new InFlightBudget(new TaskRunKey("t1", "sync", "task-bp", 1L),
                pool, 50_000L, 500L, 50L, 0L);
        WriteBackpressureSignal signal = new WriteBackpressureSignal(budget);

        signal.registerDependency("src-1", "tgt-1");
        signal.registerDependency("src-2", "tgt-2");

        // Stalling tgt-1 on RATE_WAIT pauses src-1 but leaves independent branch src-2 running
        signal.markTargetWaiting("tgt-1", WaitReason.RATE_WAIT);
        assertTrue(signal.shouldPauseSourcePull("src-1"));
        assertFalse(signal.shouldPauseSourcePull("src-2"));
        assertEquals(WaitReason.RATE_WAIT, signal.dominantUpstreamReason("src-1"));

        // Stalling on WRITE_IO_WAIT dominates as IO fault signal
        signal.markTargetWaiting("tgt-1", WaitReason.WRITE_IO_WAIT);
        assertEquals(WaitReason.WRITE_IO_WAIT, signal.dominantUpstreamReason("src-1"));

        // Clearing target waiting unblocks dependent source immediately
        signal.clearTargetWaiting("tgt-1");
        assertFalse(signal.shouldPauseSourcePull("src-1"));
        assertEquals(WaitReason.NONE, signal.dominantUpstreamReason("src-1"));
    }

    @Test
    void logRetentionSeparatesKnownThresholdsCatchUpRiskUnknownAndConfirmedExpiration() {
        LogRetentionMonitor monitor = new LogRetentionMonitor();

        // 50% used with healthy response buffer and C > P -> NORMAL with ETA
        LogRetentionMonitor.PartitionAssessment normal = monitor.assessKnownWindow(
                "p0", "binlog.001:100", 0.50d, 3600L, 600L, 200.0d, 100.0d, 10_000L);
        assertEquals(LogPositionStatus.NORMAL, normal.status());
        assertEquals(100.0d, normal.estimatedCatchUpSeconds(), 0.01d);

        // 75% used -> WARN_70
        LogRetentionMonitor.PartitionAssessment warn70 = monitor.assessKnownWindow(
                "p1", "binlog.001:200", 0.75d, 1800L, 600L, 200.0d, 100.0d, 10_000L);
        assertEquals(LogPositionStatus.WARN_70, warn70.status());
        assertEquals(LogPositionStatus.WARN_70, monitor.worstStatus());

        // Early warning when C <= P and backlog exists even if usage < 70%
        LogRetentionMonitor.PartitionAssessment stallRisk = monitor.assessKnownWindow(
                "p2", "binlog.001:300", 0.30d, 5000L, 600L, 50.0d, 100.0d, 500L);
        assertEquals(LogPositionStatus.WARN_70, stallRisk.status());
        assertNull(stallRisk.estimatedCatchUpSeconds());

        // 92% used -> CRITICAL_90
        LogRetentionMonitor.PartitionAssessment crit90 = monitor.assessKnownWindow(
                "p3", "binlog.001:400", 0.92d, 200L, 600L, 200.0d, 100.0d, 100L);
        assertEquals(LogPositionStatus.CRITICAL_90, crit90.status());
        assertEquals(LogPositionStatus.CRITICAL_90, monitor.worstStatus());

        // Dynamic unknown retention or transient network disconnect NEVER marks EXPIRED
        LogRetentionMonitor.PartitionAssessment transientErr = monitor.handleReadFailure(
                "p4", "binlog.001:500", false, "Temporary socket timeout");
        assertEquals(LogPositionStatus.UNKNOWN, transientErr.status());
        assertFalse(monitor.shouldStopAutoRetry());

        // Explicitly verified position expiration sets EXPIRED and blocks automatic retry/fallback
        LogRetentionMonitor.PartitionAssessment expired = monitor.handleReadFailure(
                "p4", "binlog.001:500", true, "Purged binary log before offset");
        assertEquals(LogPositionStatus.EXPIRED, expired.status());
        assertEquals(LogPositionStatus.EXPIRED, monitor.worstStatus());
        assertTrue(monitor.shouldStopAutoRetry());
        assertEquals("binlog.001:500", monitor.getExpiredPositionRef());
    }
}
