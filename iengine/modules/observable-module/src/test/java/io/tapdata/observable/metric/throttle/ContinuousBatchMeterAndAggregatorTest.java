package io.tapdata.observable.metric.throttle;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.tm.commons.metrics.MetricCons;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.throttle.LogPositionStatus;
import com.tapdata.tm.commons.throttle.SampleQuality;
import com.tapdata.tm.commons.throttle.TaskThrottleTelemetrySample;
import com.tapdata.tm.commons.throttle.ThrottleDimension;
import com.tapdata.tm.commons.throttle.ThrottleDirection;
import com.tapdata.tm.commons.throttle.ThrottlePhase;
import com.tapdata.tm.commons.throttle.TimeQuality;
import com.tapdata.tm.commons.throttle.WaitReason;
import io.tapdata.common.sample.Sampler;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.control.HeartbeatEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.observable.metric.TaskSampleRetriever;
import io.tapdata.observable.metric.handler.TaskSampleHandler;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests covering FULL multi-table metering, monotonic-UTC windowing, clock drift detection,
 * zero vs missing metrics, cancelled-after-debit counters, and TAP-12023 handler preservation.
 */
class ContinuousBatchMeterAndAggregatorTest {

    private static final class DriftClock implements MonotonicClock {
        private long monoNanos = 10_000_000_000L;
        private long utcMillis = 1_700_000_000_000L;

        void stepNormalSeconds(long seconds) {
            monoNanos += TimeUnit.SECONDS.toNanos(seconds);
            utcMillis += TimeUnit.SECONDS.toMillis(seconds);
        }

        void jumpUtcMillis(long deltaMillis) {
            utcMillis += deltaMillis;
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
    void fullMultiTableBatchPreservesOriginalOrderAndHandlesBudgetFailure() {
        EventCostMeter costMeter = new EventCostMeter();
        CdcTableDmlSampleCache cache = new CdcTableDmlSampleCache(
                "task-full", 2L, ThrottleDirection.INPUT, MonotonicClock.SYSTEM, costMeter, CdcTableDmlSampleCache.CacheConfig.defaults());
        ContinuousBatchMeter meter = new ContinuousBatchMeter("task-full", 2L, ThrottleDirection.INPUT, costMeter, cache);

        TapInsertRecordEvent a1 = new TapInsertRecordEvent().table("A").referenceTime(0L).after(Map.of("id", 1));
        HeartbeatEvent hb = new HeartbeatEvent();
        TapInsertRecordEvent b1 = new TapInsertRecordEvent().table("B").referenceTime(0L).after(Map.of("name", "bob"));
        TapInsertRecordEvent a2 = new TapInsertRecordEvent().table("A").referenceTime(0L).after(Map.of("id", 99999));

        List<TapEvent> original = List.of(a1, hb, b1, a2);
        BatchMeteringResult result = meter.meterFullBatch("ep1", original, null);

        // Original event order untouched
        assertSame(a1, original.get(0));
        assertSame(hb, original.get(1));
        assertSame(b1, original.get(2));
        assertSame(a2, original.get(3));

        long aFirstBytes = costMeter.measure(EventV1View.fromTapRecordEvent(a1, "A"));
        long bFirstBytes = costMeter.measure(EventV1View.fromTapRecordEvent(b1, "B"));
        assertEquals(3L, result.getDmlCount());
        assertEquals(1L, result.getControlCount());
        assertEquals(2L, result.getSampleCount());
        assertEquals(2L * aFirstBytes + bFirstBytes, result.getEstimatedBytes());
        assertEquals(SampleQuality.VALID, result.getSampleQuality());

        // Single-table cyclic payload -> BUDGET_LIMITED with null bytes but accurate DML count
        Map<String, Object> cyclic = new HashMap<>();
        cyclic.put("self", cyclic);
        TapInsertRecordEvent bad = new TapInsertRecordEvent().table("C").referenceTime(0L).after(cyclic);
        BatchMeteringResult failed = meter.meterFullBatch("ep1", List.of(bad, a1), null);
        assertEquals(2L, failed.getDmlCount());
        assertNull(failed.getEstimatedBytes());
        assertEquals(SampleQuality.BUDGET_LIMITED, failed.getSampleQuality());
    }

    @Test
    void aggregatorDistinguishesValidZeroFromMissingDetectsUtcDriftAndRenewsWaitState() {
        DriftClock clock = new DriftClock();
        ThrottleMetricsWindowAggregator agg = new ThrottleMetricsWindowAggregator(7L, clock);

        // Window 1: valid batch admission + wait state + cancelled-after-debit
        BatchMeteringResult admitted = BatchMeteringResult.builder()
                .taskId("t").runEpoch(7L).phase(ThrottlePhase.FULL).direction(ThrottleDirection.INPUT)
                .dmlCount(10L).estimatedBytes(5_000L).sampleCount(1L)
                .sampleQuality(SampleQuality.VALID).sampleConfigVersion(1L).cdcSampleRefreshIntervalSeconds(30)
                .build();
        agg.recordAdmitted(admitted, 3L);
        agg.recordCancelledAfterDebit(ThrottlePhase.FULL, ThrottleDirection.INPUT, 2L, 1_000L);
        agg.updateWaitState(ThrottlePhase.FULL, ThrottleDirection.INPUT, WaitReason.RATE_WAIT,
                EnumSet.of(ThrottleDimension.BYTES, ThrottleDimension.QPS), 0.75d, LogPositionStatus.WARN_70);

        clock.stepNormalSeconds(5);
        List<TaskThrottleTelemetrySample> w1 = agg.snapshotAndRollWindow();
        assertEquals(1, w1.size());
        TaskThrottleTelemetrySample s1 = w1.get(0);
        assertEquals(1L, s1.getSampleSeq());
        assertEquals(TimeQuality.ALIGNED, s1.getTimeQuality());
        assertEquals(10L, s1.getAdmittedEvents());
        assertEquals(5_000L, s1.getEstimatedAdmittedBytes());
        assertEquals(2L, s1.getCancelledAfterDebitEvents());
        assertEquals(1_000L, s1.getCancelledEstimatedBytes());
        assertEquals(WaitReason.RATE_WAIT, s1.getWaitReason());
        assertEquals(EnumSet.of(ThrottleDimension.BYTES, ThrottleDimension.QPS), s1.getWaitingDimensions());
        assertEquals(1_000.0d, agg.latestNumericMetrics().get(MetricCons.SS.VS.F_ESTIMATED_INPUT_BYTES_RATE).doubleValue(), 0.01d);

        // Window 2: idle while still waiting + UTC backward jump > 2s -> DRIFT_DETECTED, monotonic windowStartUtc, valid 0 bytes, renewed wait
        clock.stepNormalSeconds(5);
        clock.jumpUtcMillis(-10_000L);
        List<TaskThrottleTelemetrySample> w2 = agg.snapshotAndRollWindow();
        assertEquals(1, w2.size());
        TaskThrottleTelemetrySample s2 = w2.get(0);
        assertEquals(2L, s2.getSampleSeq());
        assertEquals(TimeQuality.DRIFT_DETECTED, s2.getTimeQuality());
        assertTrue(s2.getWindowStartUtc() >= s1.getWindowEndUtc());
        assertTrue(s2.getWindowEndUtc() >= s2.getWindowStartUtc());
        assertEquals(0L, s2.getAdmittedEvents());
        assertEquals(0L, s2.getEstimatedAdmittedBytes());
        assertEquals(WaitReason.RATE_WAIT, s2.getWaitReason());

        // Window 3: missing byte batch -> null estimatedAdmittedBytes and null numeric rate
        BatchMeteringResult missing = BatchMeteringResult.builder()
                .taskId("t").runEpoch(7L).phase(ThrottlePhase.FULL).direction(ThrottleDirection.INPUT)
                .dmlCount(4L).estimatedBytes(null).sampleCount(0L)
                .sampleQuality(SampleQuality.MISSING).sampleConfigVersion(1L).cdcSampleRefreshIntervalSeconds(30)
                .build();
        agg.recordAdmitted(missing, 3L);
        clock.stepNormalSeconds(5);
        List<TaskThrottleTelemetrySample> w3 = agg.snapshotAndRollWindow();
        assertEquals(1, w3.size());
        assertEquals(4L, w3.get(0).getAdmittedEvents());
        assertNull(w3.get(0).getEstimatedAdmittedBytes());
        assertEquals(SampleQuality.MISSING, w3.get(0).getSampleQuality());
        assertNull(agg.latestNumericMetrics().get(MetricCons.SS.VS.F_ESTIMATED_INPUT_BYTES_RATE));
    }

    @Test
    void attachingAggregatorToTaskSampleHandlerPreservesRecoveryTagsAndCoreFields() {
        TaskSampleRetriever retriever = TaskSampleRetriever.getInstance();
        Object previousOperator = ReflectionTestUtils.getField(retriever, "operator");
        try {
            HttpClientMongoOperator operator = mock(HttpClientMongoOperator.class);
            ReflectionTestUtils.setField(retriever, "operator", operator);
            TaskSampleRetriever.Samples emptySamples = new TaskSampleRetriever.Samples();
            emptySamples.setRetriever(new ArrayList<>());
            TaskSampleRetriever.SampleResponse response = new TaskSampleRetriever.SampleResponse();
            response.setSamples(emptySamples);
            when(operator.postOne(anyMap(), eq(ConnectorConstant.SAMPLE_STATISTIC_COLLECTION + "/query/v2?taskCounterRecovery=true"),
                    eq(TaskSampleRetriever.SampleResponse.class))).thenReturn(response);

            TaskDto task = mock(TaskDto.class);
            when(task.getId()).thenReturn(new ObjectId("0123456789abcdef01234567"));
            when(task.getTaskRecordId()).thenReturn("record-99");
            when(task.getStartTime()).thenReturn(new Date(100_000L));
            TaskSampleHandler handler = new TaskSampleHandler(task);

            DriftClock clock = new DriftClock();
            ThrottleMetricsWindowAggregator agg = new ThrottleMetricsWindowAggregator(9L, clock);
            agg.updateWaitState(ThrottlePhase.CDC, ThrottleDirection.OUTPUT, WaitReason.RATE_WAIT,
                    EnumSet.of(ThrottleDimension.QPS), 0.50d, LogPositionStatus.NORMAL);
            clock.stepNormalSeconds(5);
            agg.snapshotAndRollWindow();

            handler.init();
            handler.attachThrottleAggregator(agg);

            assertTrue(handler.retrieve().isEmpty());
            @SuppressWarnings("unchecked")
            Map<String, String> recoveryTags = (Map<String, String>) ReflectionTestUtils.invokeMethod(handler, "recoveryTags");
            assertNotNull(recoveryTags);
            assertFalse(recoveryTags.containsKey("physicalNodeId"));
            assertFalse(recoveryTags.containsKey("engineId"));
            assertFalse(handler.getCollector().tags().isEmpty());
            @SuppressWarnings("unchecked")
            Map<String, Sampler> samplerMap = (Map<String, Sampler>) ReflectionTestUtils.getField(handler.getCollector(), "idSamplerMap");
            assertNotNull(samplerMap);
            assertEquals(9L, samplerMap.get(MetricCons.SS.VS.F_THROTTLE_RUN_EPOCH).value().longValue());
            assertEquals(WaitReason.RATE_WAIT.code(), samplerMap.get(MetricCons.SS.VS.F_THROTTLE_WAIT_REASON_CODE).value().intValue());
            assertEquals(ThrottleDimension.QPS.bitMask(), samplerMap.get(MetricCons.SS.VS.F_THROTTLE_WAITING_DIM_MASK).value().intValue());
            assertEquals(0.0d, samplerMap.get(MetricCons.SS.VS.F_ESTIMATED_OUTPUT_BYTES_RATE).value().doubleValue(), 0.001d);
        } finally {
            ReflectionTestUtils.setField(retriever, "operator", previousOperator);
        }
    }
}
