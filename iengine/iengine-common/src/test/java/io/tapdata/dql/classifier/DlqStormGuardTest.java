package io.tapdata.dql.classifier;

import io.tapdata.dql.model.DqlClassificationConfidence;
import io.tapdata.dql.model.DqlClassificationResult;
import io.tapdata.dql.model.DqlErrorType;
import io.tapdata.dql.model.DqlExceptionScope;
import io.tapdata.dql.model.DqlRouteDecision;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DlqStormGuardTest {
    private static final String TASK_ID = "task-1";
    private static final String NODE_ID = "node-1";
    private static final String TABLE_ID = "orders";

    @Test
    @DisplayName("default guard configuration follows the frozen A05 contract")
    void usesFrozenDefaults() {
        DqlStormGuardConfig config = DqlStormGuardConfig.defaults();

        assertEquals(60, config.getWindowSeconds());
        assertEquals(20, config.getMaxEvents());
        assertEquals(0.2d, config.getMaxBatchRatio());
        assertEquals(DqlRouteDecision.TASK_RETRY, config.getDecision());
    }

    @Test
    @DisplayName("unknown single records are allowed through the inclusive count threshold")
    void allowsUnknownRecordsThroughThreshold() {
        DlqStormGuard guard = new DlqStormGuard(config(2, 0.2d, DqlRouteDecision.TASK_RETRY), () -> 1_000L);
        DqlStormGuardContext context = singleContext("connection refused: request 123");

        DqlStormGuardDecision first = guard.evaluate(unknownResult(), context);
        DqlStormGuardDecision second = guard.evaluate(unknownResult(), context);

        assertEquals(DqlRouteDecision.RECORD_DLQ, first.getClassificationResult().getRouteDecision());
        assertEquals(DqlClassificationConfidence.UNKNOWN_SINGLE,
                first.getClassificationResult().getClassificationConfidence());
        assertEquals(DqlErrorType.UNKNOWN_RECORD_ERROR, first.getClassificationResult().getErrorType());
        assertEquals(1, first.getWindowCount());
        assertEquals(2, second.getWindowCount());
        assertEquals(2, second.getMaxEvents());
        assertEquals(0.2d, second.getMaxBatchRatio());
        assertFalse(first.isGuardTriggered());
        assertFalse(second.isGuardTriggered());
    }

    @Test
    @DisplayName("the event over the count threshold is suppressed and routed to task retry")
    void triggersAfterCountThreshold() {
        DlqStormGuard guard = new DlqStormGuard(config(2, 0.2d, DqlRouteDecision.TASK_RETRY), () -> 1_000L);
        DqlStormGuardContext context = singleContext("unknown failure");

        guard.evaluate(unknownResult(), context);
        guard.evaluate(unknownResult(), context);
        DqlStormGuardDecision third = guard.evaluate(unknownResult(), context);
        DqlStormGuardDecision fourth = guard.evaluate(unknownResult(), context);

        assertEquals(DqlRouteDecision.TASK_RETRY, third.getClassificationResult().getRouteDecision());
        assertEquals(DqlRouteDecision.TASK_RETRY, fourth.getClassificationResult().getRouteDecision());
        assertEquals(DqlExceptionScope.UNKNOWN, third.getClassificationResult().getExceptionScope());
        assertEquals(null, third.getClassificationResult().getErrorType());
        assertEquals(DqlClassificationConfidence.RULE,
                third.getClassificationResult().getClassificationConfidence());
        assertTrue(third.isGuardTriggered());
        assertEquals(3, third.getWindowCount());
        assertEquals(1, third.getSuppressedCount());
        assertEquals(2, fourth.getSuppressedCount());
    }

    @Test
    @DisplayName("a batch unknown ratio above the threshold triggers before count exhaustion")
    void triggersAfterBatchRatioThreshold() {
        DlqStormGuard guard = new DlqStormGuard(config(100, 0.2d, DqlRouteDecision.TASK_RETRY), () -> 1_000L);
        DqlStormGuardContext context = new DqlStormGuardContext(
                TASK_ID, NODE_ID, TABLE_ID, "E-UNKNOWN", "same failure",
                new DqlBatchContext(true, 10, 1, 3), true);

        DqlStormGuardDecision decision = guard.evaluate(unknownResult(), context);

        assertTrue(decision.isGuardTriggered());
        assertEquals(DqlRouteDecision.TASK_RETRY, decision.getClassificationResult().getRouteDecision());
        assertEquals(0.3d, decision.getBatchRatio());
        assertTrue(decision.getClassificationResult().getClassificationReason().contains("batchRatio"));
    }

    @Test
    @DisplayName("a single record does not trigger the batch ratio rule")
    void doesNotApplyBatchRatioToSingleRecord() {
        DlqStormGuard guard = new DlqStormGuard(config(1, 0.2d, DqlRouteDecision.TASK_RETRY), () -> 1_000L);

        DqlStormGuardDecision decision = guard.evaluate(unknownResult(), singleContext("unknown failure"));

        assertFalse(decision.isGuardTriggered());
        assertEquals(DqlRouteDecision.RECORD_DLQ, decision.getClassificationResult().getRouteDecision());
        assertEquals(-1d, decision.getBatchRatio());
    }

    @Test
    @DisplayName("window expiry resets the count at the exact expiry boundary")
    void resetsAfterWindowExpiry() {
        AtomicLong now = new AtomicLong(10_000L);
        DlqStormGuard guard = new DlqStormGuard(config(1, 0.2d, DqlRouteDecision.TASK_RETRY), now::get);
        DqlStormGuardContext context = singleContext("unknown failure");

        guard.evaluate(unknownResult(), context);
        now.set(70_000L);
        DqlStormGuardDecision decision = guard.evaluate(unknownResult(), context);

        assertFalse(decision.isGuardTriggered());
        assertEquals(1, decision.getWindowCount());
        assertEquals(70_000L, decision.getWindowStartMillis());
    }

    @Test
    @DisplayName("expired windows can be explicitly removed")
    void clearsExpiredWindows() {
        AtomicLong now = new AtomicLong(10_000L);
        DlqStormGuard guard = new DlqStormGuard(config(20, 0.2d, DqlRouteDecision.TASK_RETRY), now::get);

        guard.evaluate(unknownResult(), singleContext("first"));
        guard.evaluate(unknownResult(), new DqlStormGuardContext(
                TASK_ID, NODE_ID, "customers", "E-UNKNOWN", "second",
                DqlBatchContext.singleRecord(), true));
        now.set(70_000L);

        assertEquals(2, guard.clearExpired());
        assertEquals(0, guard.getActiveWindowCount());
    }

    @Test
    @DisplayName("different guard dimensions do not share a window")
    void isolatesGuardKeys() {
        DlqStormGuard guard = new DlqStormGuard(config(1, 0.2d, DqlRouteDecision.TASK_RETRY), () -> 1_000L);
        DqlStormGuardContext firstContext = singleContext("unknown failure");
        DqlStormGuardContext secondContext = new DqlStormGuardContext(
                TASK_ID, "node-2", TABLE_ID, "E-UNKNOWN", "unknown failure",
                DqlBatchContext.singleRecord(), true);

        guard.evaluate(unknownResult(), firstContext);
        DqlStormGuardDecision second = guard.evaluate(unknownResult(), secondContext);

        assertFalse(second.isGuardTriggered());
        assertEquals(1, second.getWindowCount());
        assertNotEquals(firstContext.getGuardKey(), second.getGuardKey());
    }

    @Test
    @DisplayName("volatile message details normalize to the same guard key")
    void normalizesVolatileMessageDetails() {
        DqlStormGuardKey first = DqlStormGuardKey.of(
                TASK_ID, NODE_ID, TABLE_ID, "E-UNKNOWN", "Connection request 123 timed out");
        DqlStormGuardKey second = DqlStormGuardKey.of(
                TASK_ID, NODE_ID, TABLE_ID, "E-UNKNOWN", " connection   request 456   timed out ");

        assertEquals(first, second);
        assertEquals("connection request <number> timed out", first.getNormalizedErrorMessage());
    }

    @Test
    @DisplayName("safe guard identifiers do not expose normalized error text")
    void createsSafeIdentifier() {
        DqlStormGuardKey key = DqlStormGuardKey.of(
                TASK_ID, NODE_ID, TABLE_ID, "E-UNKNOWN", "password=secret-value");

        assertTrue(key.getSafeIdentifier().startsWith("sha256:"));
        assertFalse(key.getSafeIdentifier().contains("password"));
        assertFalse(key.getSafeIdentifier().contains("secret-value"));
        assertEquals(key.getSafeIdentifier(), DqlStormGuardKey.of(
                TASK_ID, NODE_ID, TABLE_ID, "E-UNKNOWN", "password=secret-value").getSafeIdentifier());
    }

    @Test
    @DisplayName("a non-locatable unknown event cannot be promoted to a record DLQ")
    void rejectsNonLocatableUnknownEvent() {
        DlqStormGuard guard = new DlqStormGuard(config(20, 0.2d, DqlRouteDecision.TASK_ERROR), () -> 1_000L);
        DqlStormGuardContext context = new DqlStormGuardContext(
                TASK_ID, NODE_ID, TABLE_ID, "E-UNKNOWN", "unknown failure",
                DqlBatchContext.batchFailure(10, 10), false);

        DqlStormGuardDecision decision = guard.evaluate(unknownResult(), context);

        assertTrue(decision.isGuardTriggered());
        assertEquals(DqlRouteDecision.TASK_ERROR, decision.getClassificationResult().getRouteDecision());
        assertEquals(null, decision.getClassificationResult().getErrorType());
        assertEquals(0, guard.getActiveWindowCount());
    }

    @Test
    @DisplayName("known classifications bypass Storm Guard without opening a window")
    void bypassesKnownClassification() {
        DlqStormGuard guard = new DlqStormGuard(config(1, 0.2d, DqlRouteDecision.TASK_RETRY), () -> 1_000L);
        DqlClassificationResult known = new DqlClassificationResult();
        known.setExceptionScope(DqlExceptionScope.RECORD);
        known.setRouteDecision(DqlRouteDecision.RECORD_DLQ);
        known.setErrorType(DqlErrorType.TARGET_WRITE_ERROR);
        known.setClassificationReason("known target failure");
        known.setClassificationConfidence(DqlClassificationConfidence.EXACT);

        DqlStormGuardDecision decision = guard.evaluate(known, singleContext("known failure"));

        assertFalse(decision.isGuardTriggered());
        assertEquals(known, decision.getClassificationResult());
        assertEquals(0, guard.getActiveWindowCount());
    }

    @Test
    @DisplayName("guard evaluation does not mutate the classifier result")
    void doesNotMutateClassifierResult() {
        DlqStormGuard guard = new DlqStormGuard(config(20, 0.2d, DqlRouteDecision.TASK_RETRY), () -> 1_000L);
        DqlClassificationResult original = unknownResult();

        DqlStormGuardDecision decision = guard.evaluate(original, singleContext("unknown failure"));

        assertNotSame(original, decision.getClassificationResult());
        assertEquals(DqlRouteDecision.TASK_RETRY, original.getRouteDecision());
        assertEquals(DqlClassificationConfidence.RULE, original.getClassificationConfidence());
    }

    @Test
    @DisplayName("per-key counting remains exact under concurrent evaluations")
    void countsConcurrently() throws Exception {
        int calls = 8 * 100;
        DlqStormGuard guard = new DlqStormGuard(config(calls + 1L, 0.2d, DqlRouteDecision.TASK_RETRY), () -> 1_000L);
        DqlStormGuardContext context = singleContext("unknown failure");
        ExecutorService executor = Executors.newFixedThreadPool(8);
        try {
            List<Callable<DqlStormGuardDecision>> tasks = new ArrayList<>();
            IntStream.range(0, calls).forEach(index -> tasks.add(() -> guard.evaluate(unknownResult(), context)));
            List<Future<DqlStormGuardDecision>> futures = executor.invokeAll(tasks);
            DqlStormGuardDecision last = futures.get(futures.size() - 1).get();

            assertEquals(calls + 1L, guard.evaluate(unknownResult(), context).getWindowCount());
            assertFalse(last.isGuardTriggered());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    @DisplayName("configuration rejects unsafe thresholds")
    void validatesConfiguration() {
        assertThrows(IllegalArgumentException.class,
                () -> new DqlStormGuardConfig(0, 20, 0.2d, DqlRouteDecision.TASK_RETRY));
        assertThrows(IllegalArgumentException.class,
                () -> new DqlStormGuardConfig(60, 0, 0.2d, DqlRouteDecision.TASK_RETRY));
        assertThrows(IllegalArgumentException.class,
                () -> new DqlStormGuardConfig(60, 20, 1.1d, DqlRouteDecision.TASK_RETRY));
        assertThrows(IllegalArgumentException.class,
                () -> new DqlStormGuardConfig(60, 20, 0.2d, DqlRouteDecision.RECORD_DLQ));
    }

    private static DqlStormGuardConfig config(long maxEvents,
                                              double maxBatchRatio,
                                              DqlRouteDecision decision) {
        return new DqlStormGuardConfig(60, maxEvents, maxBatchRatio, decision);
    }

    private static DqlStormGuardContext singleContext(String message) {
        return DqlStormGuardContext.singleRecord(
                TASK_ID, NODE_ID, TABLE_ID, "E-UNKNOWN", message);
    }

    private static DqlClassificationResult unknownResult() {
        DqlClassificationResult result = new DqlClassificationResult();
        result.setExceptionScope(DqlExceptionScope.UNKNOWN);
        result.setRouteDecision(DqlRouteDecision.TASK_RETRY);
        result.setErrorType(DqlErrorType.UNKNOWN_RECORD_ERROR);
        result.setClassificationReason("unknown failure requires Storm Guard");
        result.setClassificationConfidence(DqlClassificationConfidence.RULE);
        return result;
    }
}
