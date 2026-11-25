package io.tapdata.observable.metric.throttle;

import com.tapdata.tm.commons.throttle.DmlType;
import com.tapdata.tm.commons.throttle.SampleQuality;
import com.tapdata.tm.commons.throttle.ThrottleDirection;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.control.HeartbeatEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests covering AC-33 & AC-30 for {@link CdcTableDmlSampleCache} and {@link ContinuousBatchMeter}.
 */
class CdcTableDmlSampleCacheTest {

    private final EventCostMeter costMeter = new EventCostMeter();

    private static final class MutableClock implements MonotonicClock {
        private long monoNanos = 1_000_000_000L;
        private long utcMillis = 1_700_000_000_000L;

        void advanceSeconds(long seconds) {
            monoNanos += TimeUnit.SECONDS.toNanos(seconds);
            utcMillis += TimeUnit.SECONDS.toMillis(seconds);
        }

        @Override
        public long nanoTime() {
            return monoNanos;
        }

        @Override
        public long currentTimeMillis() {
            return utcMillis;
        }
    }

    @Test
    void mixedBatchCalculationMatchesDesignVectorAndTransitionsAccumulatingToValid() {
        MutableClock clock = new MutableClock();
        CdcTableDmlSampleCache cache = new CdcTableDmlSampleCache(
                "task-1", 1L, ThrottleDirection.INPUT, clock, costMeter, CdcTableDmlSampleCache.CacheConfig.defaults());
        ContinuousBatchMeter meter = new ContinuousBatchMeter("task-1", 1L, ThrottleDirection.INPUT, costMeter, cache);

        // Build mixed batch matching §3.1.2:
        // 40 x 1,000B orders INSERT + 30 x 3,000B orders UPDATE + 3 x 20,000B attachments INSERT + 10 x 200B customers DELETE = 83 events / 192,000 B
        List<TapEvent> batch1 = new ArrayList<>();
        for (int i = 0; i < 40; i++) batch1.add(insertWithExactBytes("orders", 1_000));
        for (int i = 0; i < 30; i++) batch1.add(updateWithExactBytes("orders", 3_000));
        for (int i = 0; i < 3; i++) batch1.add(insertWithExactBytes("attachments", 20_000));
        for (int i = 0; i < 10; i++) batch1.add(deleteWithExactBytes("customers", 200));

        BatchMeteringResult res1 = meter.meterCdcBatch("src-1", batch1, null, false);
        assertEquals(83L, res1.getDmlCount());
        assertEquals(43L, res1.getInsertCount());
        assertEquals(30L, res1.getUpdateCount());
        assertEquals(10L, res1.getDeleteCount());
        assertEquals(192_000L, res1.getEstimatedBytes());
        assertEquals(4L, res1.getSampleCount());
        assertEquals(SampleQuality.ACCUMULATING, res1.getSampleQuality());

        // Second batch advances each group to >= 2 samples -> VALID
        clock.advanceSeconds(1);
        BatchMeteringResult res2 = meter.meterCdcBatch("src-1", batch1, null, false);
        assertEquals(192_000L, res2.getEstimatedBytes());
        assertEquals(SampleQuality.VALID, res2.getSampleQuality());
    }

    @Test
    void ddlInSameBatchSplitsSchemaGenerationAndAvoidsStaleSampleReuse() {
        MutableClock clock = new MutableClock();
        CdcTableDmlSampleCache cache = new CdcTableDmlSampleCache(
                "task-2", 1L, ThrottleDirection.INPUT, clock, costMeter, CdcTableDmlSampleCache.CacheConfig.defaults());
        ContinuousBatchMeter meter = new ContinuousBatchMeter("task-2", 1L, ThrottleDirection.INPUT, costMeter, cache);

        TapInsertRecordEvent beforeDdl1 = insertWithExactBytes("orders", 500);
        TapInsertRecordEvent beforeDdl2 = insertWithExactBytes("orders", 500);
        meter.meterCdcBatch("src-1", List.of(beforeDdl1), null, false);
        meter.meterCdcBatch("src-1", List.of(beforeDdl2), null, false);

        TapCreateTableEvent ddl = new TapCreateTableEvent();
        ddl.setTableId("orders");
        ddl.setReferenceTime(0L);
        TapInsertRecordEvent afterDdl = insertWithExactBytes("orders", 2_000);

        // Batch containing old DML + DDL + new DML preserves order across schema generations
        BatchMeteringResult mixed = meter.meterCdcBatch("src-1", List.of(beforeDdl1, ddl, afterDdl), null, false);
        assertEquals(2L, mixed.getDmlCount());
        assertEquals(1L, mixed.getControlCount());
        assertEquals(2_500L, mixed.getEstimatedBytes());
        assertEquals(SampleQuality.ACCUMULATING, mixed.getSampleQuality());

        // Next batch after DDL isolates new schema group and turns VALID
        BatchMeteringResult postDdl = meter.meterCdcBatch("src-1", List.of(afterDdl, afterDdl), null, false);
        assertEquals(4_000L, postDdl.getEstimatedBytes());
        assertEquals(SampleQuality.VALID, postDdl.getSampleQuality());
    }

    @Test
    void eventWeightingFourSampleCapTwoXEvictionAndStaleMissingStates() {
        MutableClock clock = new MutableClock();
        CdcTableDmlSampleCache.CacheConfig config = CdcTableDmlSampleCache.CacheConfig.builder()
                .refreshIntervalSeconds(10)
                .normalSamplesPerBatch(1)
                .coldStartSamplesPerBatch(1)
                .build();
        CdcTableDmlSampleCache cache = new CdcTableDmlSampleCache(
                "task-3", 1L, ThrottleDirection.INPUT, clock, costMeter, config);
        ContinuousBatchMeter meter = new ContinuousBatchMeter("task-3", 1L, ThrottleDirection.INPUT, costMeter, cache);

        // Sample 1 at t=0: weight 10 events * 1000B
        List<TapEvent> batch10 = new ArrayList<>();
        for (int i = 0; i < 10; i++) batch10.add(insertWithExactBytes("t", 1_000));
        BatchMeteringResult r1 = meter.meterCdcBatch("src", batch10, null, false);
        assertEquals(10_000L, r1.getEstimatedBytes());
        assertEquals(SampleQuality.ACCUMULATING, r1.getSampleQuality());

        // Sample 2 at t=1: weight 30 events * 1500B (within [0.5x, 2x] normal drift range) -> weighted mean = (10*1000 + 30*1500)/40 = 1375
        clock.advanceSeconds(1);
        List<TapEvent> batch30 = new ArrayList<>();
        for (int i = 0; i < 30; i++) batch30.add(insertWithExactBytes("t", 1_500));
        BatchMeteringResult r2 = meter.meterCdcBatch("src", batch30, null, false);
        assertEquals(30L * 1375L, r2.getEstimatedBytes());
        assertEquals(SampleQuality.VALID, r2.getSampleQuality());

        // Add another group "u" also due at t=12s to force budget contention (normalSamplesPerBatch = 1)
        meter.meterCdcBatch("src", List.of(insertWithExactBytes("u", 800)), null, false);
        clock.advanceSeconds(1);
        meter.meterCdcBatch("src", List.of(insertWithExactBytes("u", 800)), null, false);

        // Advance to t=13s -> both "t" and "u" > 10s interval; only 1 group refreshes per normal batch -> the unrefreshed one reports STALE_REUSED!
        clock.advanceSeconds(11);
        BatchMeteringResult rStale = meter.meterCdcBatch("src", List.of(
                insertWithExactBytes("t", 1_500),
                insertWithExactBytes("u", 800)), null, false);
        assertNotNull(rStale.getEstimatedBytes());
        assertEquals(SampleQuality.STALE_REUSED, rStale.getSampleQuality());

        // Advance past 2 * 10s = 20s without any events, then hit budget limit so remaining expired group becomes MISSING/BUDGET_LIMITED
        clock.advanceSeconds(25);
        BatchMeteringResult rExpiredOne = meter.meterCdcBatch("src", List.of(
                insertWithExactBytes("t", 1_500),
                insertWithExactBytes("u", 800)), null, false);
        // At least one group had all >2x samples evicted and exceeded coldStartSamplesPerBatch=1 -> estimatedBytes null
        assertEquals(2L, rExpiredOne.getDmlCount());
        assertNull(rExpiredOne.getEstimatedBytes());
        assertFalse(rExpiredOne.getMissingGroups().isEmpty());
        assertTrue(rExpiredOne.getSampleQuality() == SampleQuality.MISSING
                || rExpiredOne.getSampleQuality() == SampleQuality.BUDGET_LIMITED);
    }

    @Test
    void hotUpdateIntervalRejectsInvalidAndPreservesExistingCacheWithoutClearing() {
        MutableClock clock = new MutableClock();
        CdcTableDmlSampleCache cache = new CdcTableDmlSampleCache(
                "task-4", 1L, ThrottleDirection.OUTPUT, clock, costMeter, CdcTableDmlSampleCache.CacheConfig.defaults());
        ContinuousBatchMeter meter = new ContinuousBatchMeter("task-4", 1L, ThrottleDirection.OUTPUT, costMeter, cache);

        IllegalArgumentException low = assertThrows(IllegalArgumentException.class, () -> cache.updateRefreshInterval(4, 2L));
        assertTrue(low.getMessage().contains("INVALID_RATE"));
        IllegalArgumentException high = assertThrows(IllegalArgumentException.class, () -> cache.updateRefreshInterval(301, 2L));
        assertTrue(high.getMessage().contains("INVALID_RATE"));

        meter.meterCdcBatch("tgt", List.of(insertWithExactBytes("t1", 1_000)), null, false);
        clock.advanceSeconds(1);
        meter.meterCdcBatch("tgt", List.of(insertWithExactBytes("t1", 1_000)), null, false);

        // Hot update from 30s -> 5s at t=2s: cache preserved, not due at t=4s, due at t=7s
        clock.advanceSeconds(1);
        cache.updateRefreshInterval(5, 2L);
        assertEquals(1, cache.getGroupCount());

        clock.advanceSeconds(2); // age = 3s < 5s -> cache hit, 0 samples taken
        BatchMeteringResult beforeDue = meter.meterCdcBatch("tgt", List.of(insertWithExactBytes("t1", 1_000)), null, false);
        assertEquals(2L, beforeDue.getSampleConfigVersion());
        assertEquals(5, beforeDue.getCdcSampleRefreshIntervalSeconds());
        assertEquals(0L, beforeDue.getSampleCount());
        assertEquals(1_000L, beforeDue.getEstimatedBytes());

        clock.advanceSeconds(3); // age = 6s >= 5s -> eligible for refresh
        BatchMeteringResult afterDue = meter.meterCdcBatch("tgt", List.of(insertWithExactBytes("t1", 1_000)), null, false);
        assertEquals(1L, afterDue.getSampleCount());
        assertEquals(SampleQuality.VALID, afterDue.getSampleQuality());
    }

    @Test
    void distributionShiftProbesRebuildsBaselineWithinFifteenSeconds() {
        MutableClock clock = new MutableClock();
        CdcTableDmlSampleCache cache = new CdcTableDmlSampleCache(
                "task-5", 1L, ThrottleDirection.INPUT, clock, costMeter, CdcTableDmlSampleCache.CacheConfig.defaults());
        ContinuousBatchMeter meter = new ContinuousBatchMeter("task-5", 1L, ThrottleDirection.INPUT, costMeter, cache);

        // Establish 2-sample baseline at 1,000B with 30s default refresh interval
        meter.meterCdcBatch("src", List.of(insertWithExactBytes("items", 1_000)), null, false);
        clock.advanceSeconds(1);
        meter.meterCdcBatch("src", List.of(insertWithExactBytes("items", 1_000)), null, false);

        // t+5s: distribution shifts 1KB -> 10KB while rateWaiting=true (probe #1 records pending shift)
        clock.advanceSeconds(5);
        BatchMeteringResult spike1 = meter.meterCdcBatch("src", List.of(insertWithExactBytes("items", 10_000)), null, true);
        assertEquals(1L, spike1.getSampleCount());
        assertEquals(1_000L, spike1.getEstimatedBytes()); // keeps baseline on single probe

        // t+10s (within 15s SLA): probe #2 confirms 10KB (> 2x old mean) -> rebuilds mean immediately to 10KB
        clock.advanceSeconds(5);
        BatchMeteringResult spike2 = meter.meterCdcBatch("src", List.of(insertWithExactBytes("items", 10_000)), null, true);
        assertEquals(1L, spike2.getSampleCount());
        assertEquals(10_000L, spike2.getEstimatedBytes());
        assertEquals(SampleQuality.VALID, spike2.getSampleQuality());
    }

    @Test
    void enforcesWorkBudgetLimitsEmptyControlsAndRotatesSamplePositions() {
        MutableClock clock = new MutableClock();
        CdcTableDmlSampleCache.CacheConfig tinyWorkBudget = CdcTableDmlSampleCache.CacheConfig.builder()
                .maxWorkUnitsPerBatch(5)
                .build();
        CdcTableDmlSampleCache cache = new CdcTableDmlSampleCache(
                "task-6", 1L, ThrottleDirection.INPUT, clock, costMeter, tinyWorkBudget);
        ContinuousBatchMeter meter = new ContinuousBatchMeter("task-6", 1L, ThrottleDirection.INPUT, costMeter, cache);

        // Pure heartbeat/empty batch produces valid 0 bytes
        BatchMeteringResult emptyRes = meter.meterCdcBatch("src", List.of(new HeartbeatEvent()), null, false);
        assertEquals(0L, emptyRes.getDmlCount());
        assertEquals(1L, emptyRes.getControlCount());
        assertEquals(0L, emptyRes.getEstimatedBytes());
        assertEquals(SampleQuality.VALID, emptyRes.getSampleQuality());

        // Event with > 5 fields hits maxWorkUnitsPerBatch=5 -> BUDGET_LIMITED and null estimatedBytes while keeping exact dmlCount
        Map<String, Object> wideMap = new HashMap<>();
        for (int i = 0; i < 20; i++) wideMap.put("k" + i, i);
        TapInsertRecordEvent wide = new TapInsertRecordEvent().table("wide").referenceTime(0L).after(wideMap);
        BatchMeteringResult budgetExceeded = meter.meterCdcBatch("src", List.of(wide, wide), null, false);
        assertEquals(2L, budgetExceeded.getDmlCount());
        assertNull(budgetExceeded.getEstimatedBytes());
        assertTrue(budgetExceeded.isBudgetHit());
        assertEquals(SampleQuality.BUDGET_LIMITED, budgetExceeded.getSampleQuality());

        // Verify in-group sample rotation visits distinct positions over refreshes
        MutableClock rotClock = new MutableClock();
        Set<String> observedTags = new HashSet<>();
        CdcTableDmlSampleCache rotCache = new CdcTableDmlSampleCache(
                "rot", 1L, ThrottleDirection.INPUT, rotClock, new EventCostMeter(),
                CdcTableDmlSampleCache.CacheConfig.builder().refreshIntervalSeconds(5).build());
        ContinuousBatchMeter rotMeter = new ContinuousBatchMeter("rot", 1L, ThrottleDirection.INPUT, new EventCostMeter(), rotCache);
        List<TapEvent> twoDiffSizeEvents = List.of(
                insertWithExactBytes("t", 100),
                insertWithExactBytes("t", 140));
        for (int round = 0; round < 3; round++) {
            BatchMeteringResult r = rotMeter.meterCdcBatch("src", twoDiffSizeEvents, null, false);
            observedTags.add(String.valueOf(r.getEstimatedBytes()));
            rotClock.advanceSeconds(6);
        }
        assertTrue(observedTags.size() >= 2, "Rotation should sample distinct positions within group");
    }

    private TapInsertRecordEvent insertWithExactBytes(String table, int targetTotalBytes) {
        TapInsertRecordEvent base = new TapInsertRecordEvent().table(table).referenceTime(0L);
        Map<String, Object> after = new HashMap<>();
        after.put("p", "");
        base.after(after);
        long overhead = costMeter.measure(EventV1View.fromTapRecordEvent(base, table));
        int paddingLen = Math.max(0, targetTotalBytes - (int) overhead);
        after.put("p", "a".repeat(paddingLen));
        return base;
    }

    private TapUpdateRecordEvent updateWithExactBytes(String table, int targetTotalBytes) {
        TapUpdateRecordEvent base = new TapUpdateRecordEvent().table(table).referenceTime(0L);
        Map<String, Object> before = new HashMap<>();
        Map<String, Object> after = new HashMap<>();
        before.put("p", "");
        after.put("p", "");
        base.before(before);
        base.after(after);
        long overhead = costMeter.measure(EventV1View.fromTapRecordEvent(base, table));
        int paddingLen = Math.max(0, targetTotalBytes - (int) overhead);
        after.put("p", "u".repeat(paddingLen));
        return base;
    }

    private TapDeleteRecordEvent deleteWithExactBytes(String table, int targetTotalBytes) {
        TapDeleteRecordEvent base = new TapDeleteRecordEvent().table(table).referenceTime(0L);
        Map<String, Object> before = new HashMap<>();
        before.put("p", "");
        base.before(before);
        long overhead = costMeter.measure(EventV1View.fromTapRecordEvent(base, table));
        int paddingLen = Math.max(0, targetTotalBytes - (int) overhead);
        before.put("p", "d".repeat(paddingLen));
        return base;
    }
}
