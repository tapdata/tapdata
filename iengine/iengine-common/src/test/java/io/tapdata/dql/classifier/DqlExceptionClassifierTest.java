package io.tapdata.dql.classifier;

import com.tapdata.processor.error.ScriptProcessorExCode_30;
import io.tapdata.PDKExCode_10;
import io.tapdata.dql.model.DqlClassificationConfidence;
import io.tapdata.dql.model.DqlClassificationResult;
import io.tapdata.dql.model.DqlErrorType;
import io.tapdata.dql.model.DqlExceptionScope;
import io.tapdata.dql.model.DqlRouteDecision;
import io.tapdata.error.TaskProcessorExCode_11;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.exception.TapCodeException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.script.ScriptException;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DqlExceptionClassifierTest {
    private static final TapInsertRecordEvent EVENT = TapInsertRecordEvent.create()
            .table("orders")
            .after(Map.of("id", 1001, "status", "new"));

    private final DqlExceptionClassifier classifier = new DqlExceptionClassifier();

    @Test
    @DisplayName("known target data errors are routed to a record DLQ")
    void routesKnownTargetDataErrorToRecordDlq() {
        DqlClassificationResult result = classifier.classify(
                new TapCodeException(PDKExCode_10.WRITE_TYPE), targetContext(EVENT));

        assertEquals(DqlExceptionScope.RECORD, result.getExceptionScope());
        assertEquals(DqlRouteDecision.RECORD_DLQ, result.getRouteDecision());
        assertEquals(DqlErrorType.TARGET_WRITE_ERROR, result.getErrorType());
        assertEquals(DqlClassificationConfidence.EXACT, result.getClassificationConfidence());
        assertTrue(result.getClassificationReason().contains(PDKExCode_10.WRITE_TYPE));
    }

    @Test
    @DisplayName("script failure for one processor event is a transform error")
    void routesSingleScriptFailureToRecordDlq() {
        DqlClassificationResult result = classifier.classify(
                new TapCodeException(ScriptProcessorExCode_30.JAVA_SCRIPT_PROCESS_FAILED),
                processorContext(EVENT));

        assertEquals(DqlExceptionScope.RECORD, result.getExceptionScope());
        assertEquals(DqlRouteDecision.RECORD_DLQ, result.getRouteDecision());
        assertEquals(DqlErrorType.TRANSFORM_ERROR, result.getErrorType());
        assertEquals(DqlClassificationConfidence.EXACT, result.getClassificationConfidence());
    }

    @Test
    @DisplayName("network timeout is task retryable even when an event is available")
    void routesNetworkTimeoutToTaskRetry() {
        DqlClassificationResult result = classifier.classify(
                new SocketTimeoutException("target read timed out"), targetContext(EVENT));

        assertEquals(DqlExceptionScope.TASK_SHARED, result.getExceptionScope());
        assertEquals(DqlRouteDecision.TASK_RETRY, result.getRouteDecision());
        assertEquals(DqlClassificationConfidence.RULE, result.getClassificationConfidence());
    }

    @Test
    @DisplayName("retryable shared failure becomes task error after retry exhaustion")
    void routesExhaustedSharedFailureToTaskError() {
        DqlClassificationResult result = classifier.classify(
                new SocketTimeoutException("target read timed out"),
                targetContext(EVENT, DqlTaskContext.runningSync(true)));

        assertEquals(DqlExceptionScope.TASK_SHARED, result.getExceptionScope());
        assertEquals(DqlRouteDecision.TASK_ERROR, result.getRouteDecision());
    }

    @Test
    @DisplayName("a retryable code nested in the exception chain is task retryable")
    void routesNestedRetryableCodeToTaskRetry() {
        DqlClassificationResult result = classifier.classify(
                new TapCodeException(PDKExCode_10.WRITE_TYPE,
                        new TapCodeException(PDKExCode_10.RETRYABLE_ERROR)), targetContext(EVENT));

        assertEquals(DqlExceptionScope.TASK_SHARED, result.getExceptionScope());
        assertEquals(DqlRouteDecision.TASK_RETRY, result.getRouteDecision());
        assertTrue(result.getClassificationReason().contains(PDKExCode_10.WRITE_TYPE));
    }

    @Test
    @DisplayName("permission failures are system failures")
    void routesPermissionFailureToTaskError() {
        DqlClassificationResult result = classifier.classify(
                new TapCodeException(PDKExCode_10.WRITE_MISSING_PRIVILEGES), targetContext(EVENT));

        assertEquals(DqlExceptionScope.SYSTEM, result.getExceptionScope());
        assertEquals(DqlRouteDecision.TASK_ERROR, result.getRouteDecision());
    }

    @Test
    @DisplayName("configuration errors are system failures")
    void routesConfigurationErrorToTaskError() {
        DqlClassificationResult result = classifier.classify(
                new TapCodeException(PDKExCode_10.CONFIG_ERROR), targetContext(EVENT));

        assertEquals(DqlExceptionScope.SYSTEM, result.getExceptionScope());
        assertEquals(DqlRouteDecision.TASK_ERROR, result.getRouteDecision());
    }

    @Test
    @DisplayName("system failures never enter a record DLQ")
    void routesSystemFailureToTaskError() {
        DqlClassificationResult result = classifier.classify(
                new OutOfMemoryError("heap exhausted"), targetContext(EVENT));

        assertEquals(DqlExceptionScope.SYSTEM, result.getExceptionScope());
        assertEquals(DqlRouteDecision.TASK_ERROR, result.getRouteDecision());
        assertEquals(DqlClassificationConfidence.EXACT, result.getClassificationConfidence());
    }

    @Test
    @DisplayName("a task callback failure is system level")
    void routesTmCallbackFailureToTaskError() {
        DqlClassificationResult result = classifier.classify(
                new IllegalStateException("TM unavailable"),
                new DqlClassificationContext(DqlFailedStage.TM_CALLBACK, DqlNodeType.TM_CALLBACK,
                        EVENT, DqlBatchContext.singleRecord(), DqlTaskContext.runningSync()));

        assertEquals(DqlExceptionScope.SYSTEM, result.getExceptionScope());
        assertEquals(DqlRouteDecision.TASK_ERROR, result.getRouteDecision());
    }

    @Test
    @DisplayName("unknown single-record failures stay conservative until Storm Guard evaluates them")
    void keepsUnknownFailureOutOfRecordDlq() {
        DqlClassificationResult result = classifier.classify(
                new IllegalStateException("connector returned an undocumented error"), targetContext(EVENT));

        assertEquals(DqlExceptionScope.UNKNOWN, result.getExceptionScope());
        assertEquals(DqlRouteDecision.TASK_RETRY, result.getRouteDecision());
        assertEquals(DqlErrorType.UNKNOWN_RECORD_ERROR, result.getErrorType());
        assertEquals(DqlClassificationConfidence.RULE, result.getClassificationConfidence());
        assertTrue(result.getClassificationReason().contains("Storm Guard"));
    }

    @Test
    @DisplayName("known record error without a TapRecordEvent cannot enter DQL")
    void rejectsRecordDlqWithoutEvent() {
        DqlClassificationResult result = classifier.classify(
                new TapCodeException(PDKExCode_10.WRITE_TYPE),
                new DqlClassificationContext(DqlFailedStage.TARGET_WRITE, DqlNodeType.TARGET,
                        null, DqlBatchContext.singleRecord(), DqlTaskContext.runningSync()));

        assertEquals(DqlExceptionScope.SYSTEM, result.getExceptionScope());
        assertEquals(DqlRouteDecision.TASK_ERROR, result.getRouteDecision());
    }

    @Test
    @DisplayName("script syntax errors during initialization are not record transforms")
    void doesNotClassifyScriptInitializationAsRecordError() {
        DqlClassificationResult result = classifier.classify(
                new ScriptException("syntax error"),
                new DqlClassificationContext(DqlFailedStage.PROCESSOR, DqlNodeType.PROCESSOR,
                        EVENT, DqlBatchContext.singleRecord(), DqlTaskContext.runningSync()));

        assertEquals(DqlExceptionScope.SYSTEM, result.getExceptionScope());
        assertEquals(DqlRouteDecision.TASK_ERROR, result.getRouteDecision());
    }

    @Test
    @DisplayName("script initialization remains system level when its cause is an I/O error")
    void keepsScriptInitializationAheadOfSharedCause() {
        ScriptException scriptException = new ScriptException("syntax error");
        scriptException.initCause(new IOException("script resource unavailable"));

        DqlClassificationResult result = classifier.classify(scriptException,
                new DqlClassificationContext(DqlFailedStage.PROCESSOR, DqlNodeType.PROCESSOR,
                        EVENT, DqlBatchContext.singleRecord(), DqlTaskContext.runningSync()));

        assertEquals(DqlExceptionScope.SYSTEM, result.getExceptionScope());
        assertEquals(DqlRouteDecision.TASK_ERROR, result.getRouteDecision());
    }

    @Test
    @DisplayName("script invocation failures for one event are transform errors")
    void routesScriptInvocationFailureToRecordDlq() {
        DqlClassificationResult result = classifier.classify(
                new TapCodeException(ScriptProcessorExCode_30.INVOKE_SCRIPT_FAILED),
                processorContext(EVENT));

        assertEquals(DqlExceptionScope.RECORD, result.getExceptionScope());
        assertEquals(DqlRouteDecision.RECORD_DLQ, result.getRouteDecision());
        assertEquals(DqlErrorType.TRANSFORM_ERROR, result.getErrorType());
    }

    @Test
    @DisplayName("a nested script exception with one processor event is a transform error")
    void routesNestedScriptExceptionToRecordDlq() {
        ScriptException scriptFailure = new ScriptException("undefined variable");
        RuntimeException executionFailure = new RuntimeException(
                "Execute script error, record: {id=1001}", scriptFailure);

        DqlClassificationResult result = classifier.classify(
                new TapCodeException(TaskProcessorExCode_11.UNKNOWN_ERROR, executionFailure),
                processorContext(EVENT));

        assertEquals(DqlExceptionScope.RECORD, result.getExceptionScope());
        assertEquals(DqlRouteDecision.RECORD_DLQ, result.getRouteDecision());
        assertEquals(DqlErrorType.TRANSFORM_ERROR, result.getErrorType());
        assertEquals(DqlClassificationConfidence.RULE, result.getClassificationConfidence());
    }

    @Test
    @DisplayName("a direct script exception remains an initialization failure")
    void keepsDirectScriptExceptionAtTaskLevel() {
        DqlClassificationResult result = classifier.classify(
                new ScriptException("syntax error"), processorContext(EVENT));

        assertEquals(DqlExceptionScope.SYSTEM, result.getExceptionScope());
        assertEquals(DqlRouteDecision.TASK_ERROR, result.getRouteDecision());
    }

    @Test
    @DisplayName("record conversion failures are malformed records")
    void routesRecordConversionFailureToRecordDlq() {
        DqlClassificationResult result = classifier.classify(
                new NumberFormatException("invalid number"), targetContext(EVENT));

        assertEquals(DqlExceptionScope.RECORD, result.getExceptionScope());
        assertEquals(DqlRouteDecision.RECORD_DLQ, result.getRouteDecision());
        assertEquals(DqlErrorType.MALFORMED_RECORD, result.getErrorType());
        assertEquals(DqlClassificationConfidence.RULE, result.getClassificationConfidence());
    }

    @Test
    @DisplayName("an unresolved batch without a single event stays task retryable")
    void routesUnresolvedBatchFailureToTaskRetry() {
        DqlClassificationResult result = classifier.classify(
                new IllegalStateException("batch write failed"),
                new DqlClassificationContext(DqlFailedStage.TARGET_WRITE, DqlNodeType.TARGET,
                        null, DqlBatchContext.batchFailure(10, 10), DqlTaskContext.runningSync()));

        assertEquals(DqlExceptionScope.TASK_SHARED, result.getExceptionScope());
        assertEquals(DqlRouteDecision.TASK_RETRY, result.getRouteDecision());
    }

    @Test
    @DisplayName("an unresolved batch becomes a task error after retry exhaustion")
    void routesExhaustedUnresolvedBatchToTaskError() {
        DqlClassificationResult result = classifier.classify(
                new IllegalStateException("batch write failed"),
                new DqlClassificationContext(DqlFailedStage.TARGET_WRITE, DqlNodeType.TARGET,
                        null, DqlBatchContext.batchFailure(10, 10), DqlTaskContext.runningSync(true)));

        assertEquals(DqlExceptionScope.TASK_SHARED, result.getExceptionScope());
        assertEquals(DqlRouteDecision.TASK_ERROR, result.getRouteDecision());
    }

    @Test
    @DisplayName("classifier validates required inputs")
    void validatesInputs() {
        assertThrows(IllegalArgumentException.class,
                () -> classifier.classify(null, targetContext(EVENT)));
        assertThrows(IllegalArgumentException.class,
                () -> classifier.classify(new IllegalStateException("error"), null));
    }

    private static DqlClassificationContext targetContext(TapInsertRecordEvent event) {
        return targetContext(event, DqlTaskContext.runningSync());
    }

    private static DqlClassificationContext targetContext(TapInsertRecordEvent event,
                                                           DqlTaskContext taskContext) {
        return new DqlClassificationContext(DqlFailedStage.TARGET_WRITE, DqlNodeType.TARGET,
                event, DqlBatchContext.singleRecord(), taskContext);
    }

    private static DqlClassificationContext processorContext(TapInsertRecordEvent event) {
        return new DqlClassificationContext(DqlFailedStage.PROCESSOR, DqlNodeType.PROCESSOR,
                event, DqlBatchContext.singleRecord(), DqlTaskContext.runningSync());
    }
}
