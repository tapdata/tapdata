package io.tapdata.flow.engine.V2.node.duckdb;

import com.tapdata.entity.SyncStage;
import io.tapdata.observable.logging.ObsLogger;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class SyncStageTrackerTest {
    @Test
    void updateTableStage_callsCallbackOnlyAfterInitCdcAndStageChanged() {
        SyncStageTracker tracker = new SyncStageTracker();
        ObsLogger logger = mock(ObsLogger.class);
        tracker.setLogger(logger);

        AtomicInteger callbackCount = new AtomicInteger();
        tracker.setOnAllTablesCdcCallback(v -> callbackCount.incrementAndGet());

        tracker.updateTableStage("t", SyncStage.INITIAL_SYNC);
        tracker.updateTableStage("t", SyncStage.CDC);
        assertEquals(0, callbackCount.get());

        tracker.initCdc();
        tracker.updateTableStage("t", SyncStage.CDC);
        tracker.updateTableStage("t", SyncStage.CDC);
        assertEquals(0, callbackCount.get());

        tracker.updateTableStage("t", SyncStage.INITIAL_SYNC);
        assertEquals(1, callbackCount.get());
    }

    @Test
    void isTableInInitialSync_defaultTrueUntilCdc() {
        SyncStageTracker tracker = new SyncStageTracker();
        assertTrue(tracker.isTableInInitialSync("t"));
        tracker.updateTableStageFromEvent("t", SyncStage.CDC);
        assertFalse(tracker.isTableInInitialSync("t"));
        tracker.reset();
        assertTrue(tracker.isTableInInitialSync("t"));
    }
}
