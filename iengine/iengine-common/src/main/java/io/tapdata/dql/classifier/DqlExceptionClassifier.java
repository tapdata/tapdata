package io.tapdata.dql.classifier;

import com.tapdata.processor.error.FieldProcessException;
import com.tapdata.processor.error.ScriptProcessorExCode_30;
import io.tapdata.PDKExCode_10;
import io.tapdata.error.TaskProcessorExCode_11;
import io.tapdata.dql.model.DqlClassificationConfidence;
import io.tapdata.dql.model.DqlClassificationResult;
import io.tapdata.dql.model.DqlErrorType;
import io.tapdata.dql.model.DqlExceptionScope;
import io.tapdata.dql.model.DqlRouteDecision;
import io.tapdata.exception.TapCodeException;

import javax.script.ScriptException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.time.DateTimeException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeoutException;

/**
 * Conservatively classifies Engine failures before any DQL capture or skip decision.
 */
public class DqlExceptionClassifier {
    // TaskTargetProcessorExCode_15 is defined in iengine-app; keep the common recoverable code local.
    private static final String TARGET_WRITE_COMMON_FAILED = "15019";
    private static final Set<String> SHARED_ERROR_CODES = Set.of(
            TARGET_WRITE_COMMON_FAILED,
            PDKExCode_10.TERMINATE_BY_SERVER,
            PDKExCode_10.RETRYABLE_ERROR);
    private static final Set<String> SYSTEM_ERROR_CODES = Set.of(
            PDKExCode_10.USERNAME_PASSWORD_INVALID,
            PDKExCode_10.OFFSET_OUT_OF_LOG,
            PDKExCode_10.READ_MISSING_PRIVILEGES,
            PDKExCode_10.WRITE_MISSING_PRIVILEGES,
            PDKExCode_10.CONFIG_ERROR,
            TaskProcessorExCode_11.CREATE_PROCESSOR_FAILED,
            TaskProcessorExCode_11.CUSTOM_NODE_NOT_FOUND,
            TaskProcessorExCode_11.SCRIPT_INIT_FAILED,
            TaskProcessorExCode_11.INIT_DATA_FLOW_PROCESSOR_FAILED);
    private static final Set<String> TARGET_RECORD_ERROR_CODES = Set.of(
            PDKExCode_10.WRITE_TYPE,
            PDKExCode_10.WRITE_LENGTH_INVALID,
            PDKExCode_10.WRITE_VIOLATE_UNIQUE_CONSTRAINT,
            PDKExCode_10.WRITE_VIOLATE_NULLABLE_CONSTRAINT,
            PDKExCode_10.SKIPPABLE_DATA);
    private static final Set<String> TRANSFORM_ERROR_CODES = Set.of(
            ScriptProcessorExCode_30.INVOKE_SCRIPT_FAILED,
            ScriptProcessorExCode_30.JAVA_SCRIPT_PROCESS_FAILED,
            ScriptProcessorExCode_30.PYTHON_PROCESS_FAILED,
            TaskProcessorExCode_11.JAVA_SCRIPT_PROCESS_FAILED,
            TaskProcessorExCode_11.PYTHON_PROCESS_FAILED);
    private static final Set<String> SCRIPT_INITIALIZATION_ERROR_CODES = Set.of(
            ScriptProcessorExCode_30.GET_SCRIPT_ENGINE_ERROR,
            ScriptProcessorExCode_30.URL_CLASS_LOADER_ERROR,
            ScriptProcessorExCode_30.INIT_PYTHON_METHOD_ERROR,
            ScriptProcessorExCode_30.INIT_STANDARDIZATION_METHOD_FAILED,
            ScriptProcessorExCode_30.GET_SCRIPT_STANDARDIZATION_ENGINE_FAILED,
            ScriptProcessorExCode_30.INIT_BUILD_IN_METHOD_FAILED,
            ScriptProcessorExCode_30.EVAL_SOURCE_ERROR,
            ScriptProcessorExCode_30.INIT_SCRIPT_ENGINE_FAILED,
            ScriptProcessorExCode_30.APPLY_CLASS_LOADER_CONTEXT_FAILED,
            ScriptProcessorExCode_30.GET_PYTHON_ENGINE_FAILED,
            ScriptProcessorExCode_30.JAVA_SCRIPT_PROCESSOR_GET_SCRIPT_FAILED,
            ScriptProcessorExCode_30.CUSTOM_PROCESSOR_GET_SCRIPT_ENGINE_FAILED,
            ScriptProcessorExCode_30.INVOKE_SCRIPT_FAILED_ENGINE_NULL);

    public DqlClassificationResult classify(Throwable error, DqlClassificationContext context) {
        if (error == null) {
            throw new IllegalArgumentException("error must not be null");
        }
        if (context == null) {
            throw new IllegalArgumentException("context must not be null");
        }

        if (isSystemFailure(error, context)) {
            return result(DqlExceptionScope.SYSTEM, DqlRouteDecision.TASK_ERROR, null,
                    reason("system failure", error, context), DqlClassificationConfidence.EXACT);
        }

        if (isScriptInitializationFailure(error)) {
            return result(DqlExceptionScope.SYSTEM, DqlRouteDecision.TASK_ERROR, null,
                    reason("script initialization failure", error, context), DqlClassificationConfidence.EXACT);
        }

        if (isSharedFailure(error)) {
            DqlRouteDecision decision = context.getTaskContext().isRetryExhausted()
                    ? DqlRouteDecision.TASK_ERROR : DqlRouteDecision.TASK_RETRY;
            return result(DqlExceptionScope.TASK_SHARED, decision, null,
                    reason("shared failure", error, context), DqlClassificationConfidence.RULE);
        }

        if (isRecordTask(context) && context.getEvent() != null) {
            DqlClassificationResult recordResult = classifyKnownRecordFailure(error, context);
            if (recordResult != null) {
                return recordResult;
            }
        }

        if (context.getEvent() == null) {
            if (isUnresolvedBatchFailure(context)) {
                DqlRouteDecision decision = context.getTaskContext().isRetryExhausted()
                        ? DqlRouteDecision.TASK_ERROR : DqlRouteDecision.TASK_RETRY;
                return result(DqlExceptionScope.TASK_SHARED, decision, null,
                        reason("batch failure has no single record", error, context),
                        DqlClassificationConfidence.RULE);
            }
            return result(DqlExceptionScope.SYSTEM, DqlRouteDecision.TASK_ERROR, null,
                    reason("record event is unavailable", error, context), DqlClassificationConfidence.RULE);
        }

        return result(DqlExceptionScope.UNKNOWN, DqlRouteDecision.TASK_RETRY,
                DqlErrorType.UNKNOWN_RECORD_ERROR,
                reason("unknown failure requires Storm Guard", error, context),
                DqlClassificationConfidence.RULE);
    }

    private DqlClassificationResult classifyKnownRecordFailure(Throwable error,
                                                                DqlClassificationContext context) {
        if (context.getFailedStage() == DqlFailedStage.TARGET_WRITE
                && isTargetRecordCode(error)) {
            String targetErrorCode = firstMatchingCode(error, TARGET_RECORD_ERROR_CODES);
            DqlErrorType errorType = switch (targetErrorCode) {
                case PDKExCode_10.WRITE_TYPE, PDKExCode_10.WRITE_LENGTH_INVALID,
                        PDKExCode_10.WRITE_VIOLATE_NULLABLE_CONSTRAINT,
                        PDKExCode_10.WRITE_VIOLATE_UNIQUE_CONSTRAINT -> DqlErrorType.TARGET_WRITE_ERROR;
                case PDKExCode_10.SKIPPABLE_DATA -> DqlErrorType.POISON_RECORD;
                default -> DqlErrorType.TARGET_WRITE_ERROR;
            };
            return result(DqlExceptionScope.RECORD, DqlRouteDecision.RECORD_DLQ, errorType,
                    reason("known target record failure", error, context), DqlClassificationConfidence.EXACT);
        }

        if (context.getFailedStage() == DqlFailedStage.PROCESSOR
                && isTransformCode(error)) {
            return result(DqlExceptionScope.RECORD, DqlRouteDecision.RECORD_DLQ,
                    DqlErrorType.TRANSFORM_ERROR,
                    reason("known processor record failure", error, context),
                    DqlClassificationConfidence.EXACT);
        }

        if (isMalformedException(error, context)) {
            return result(DqlExceptionScope.RECORD, DqlRouteDecision.RECORD_DLQ,
                    DqlErrorType.MALFORMED_RECORD,
                    reason("record conversion failure", error, context),
                    DqlClassificationConfidence.RULE);
        }

        return null;
    }

    private boolean isSystemFailure(Throwable error, DqlClassificationContext context) {
        if (context.getFailedStage() == DqlFailedStage.TM_CALLBACK
                || context.getNodeType() == DqlNodeType.TM_CALLBACK) {
            return true;
        }
        DqlTaskContext taskContext = context.getTaskContext();
        if (!taskContext.isConfigurationValid() || !taskContext.isRecordDlqTask()
                || isStopped(taskContext.getTaskStatus())) {
            return true;
        }
        if (containsCode(error, SYSTEM_ERROR_CODES)) {
            return true;
        }
        for (Throwable current : chain(error)) {
            if (current instanceof VirtualMachineError
                    || current instanceof ThreadDeath
                    || current instanceof InterruptedException
                    || current instanceof CancellationException) {
                return true;
            }
        }
        return false;
    }

    private boolean isSharedFailure(Throwable error) {
        if (containsCode(error, SHARED_ERROR_CODES)) {
            return true;
        }
        for (Throwable current : chain(error)) {
            if (current instanceof SocketTimeoutException
                    || current instanceof ConnectException
                    || current instanceof SocketException
                    || current instanceof TimeoutException
                    || current instanceof IOException) {
                return true;
            }
        }
        return false;
    }

    private boolean isScriptInitializationFailure(Throwable error) {
        if (containsCode(error, SCRIPT_INITIALIZATION_ERROR_CODES)) {
            return true;
        }
        return errorCode(error) == null && containsType(error, ScriptException.class);
    }

    private boolean isTargetRecordCode(Throwable error) {
        return containsCode(error, TARGET_RECORD_ERROR_CODES);
    }

    private boolean isTransformCode(Throwable error) {
        return containsCode(error, TRANSFORM_ERROR_CODES);
    }

    private boolean isMalformedException(Throwable error, DqlClassificationContext context) {
        if (context.getFailedStage() != DqlFailedStage.TARGET_WRITE
                && context.getFailedStage() != DqlFailedStage.PROCESSOR) {
            return false;
        }
        return containsType(error, NumberFormatException.class)
                || containsType(error, DateTimeException.class)
                || containsType(error, FieldProcessException.class);
    }

    private boolean isRecordTask(DqlClassificationContext context) {
        DqlTaskContext taskContext = context.getTaskContext();
        return taskContext.isRecordDlqTask()
                && !isStopped(taskContext.getTaskStatus());
    }

    private boolean isUnresolvedBatchFailure(DqlClassificationContext context) {
        DqlBatchContext batchContext = context.getBatchContext();
        return batchContext.isBatchWriteFailed()
                && batchContext.getBatchSize() > 1
                && batchContext.getSplitEventCount() == 0;
    }

    private String errorCode(Throwable error) {
        for (Throwable current : chain(error)) {
            if (current instanceof TapCodeException tapCodeException
                    && tapCodeException.getCode() != null) {
                return tapCodeException.getCode();
            }
        }
        return null;
    }

    private boolean containsType(Throwable error, Class<? extends Throwable> type) {
        for (Throwable current : chain(error)) {
            if (type.isInstance(current)) {
                return true;
            }
        }
        return false;
    }

    private String firstMatchingCode(Throwable error, Set<String> codes) {
        for (Throwable current : chain(error)) {
            if (current instanceof TapCodeException tapCodeException
                    && codes.contains(tapCodeException.getCode())) {
                return tapCodeException.getCode();
            }
        }
        return null;
    }

    private boolean containsCode(Throwable error, Set<String> codes) {
        return firstMatchingCode(error, codes) != null;
    }

    private List<Throwable> chain(Throwable error) {
        List<Throwable> chain = new ArrayList<>();
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        Throwable current = error;
        while (current != null && visited.add(current)) {
            chain.add(current);
            current = current.getCause();
        }
        return chain;
    }

    private boolean isStopped(String taskStatus) {
        if (taskStatus == null) {
            return false;
        }
        return switch (taskStatus.toUpperCase()) {
            case "STOPPED", "STOPPING", "FAILED", "ERROR", "CLOSED", "CLOSING" -> true;
            default -> false;
        };
    }

    private String reason(String prefix, Throwable error, DqlClassificationContext context) {
        String code = errorCode(error);
        return prefix + ": code=" + (code == null ? "none" : code)
                + ", stage=" + context.getFailedStage()
                + ", node=" + context.getNodeType();
    }

    private DqlClassificationResult result(DqlExceptionScope scope,
                                           DqlRouteDecision decision,
                                           DqlErrorType errorType,
                                           String reason,
                                           DqlClassificationConfidence confidence) {
        DqlClassificationResult result = new DqlClassificationResult();
        result.setExceptionScope(scope);
        result.setRouteDecision(decision);
        result.setErrorType(errorType);
        result.setClassificationReason(reason);
        result.setClassificationConfidence(confidence);
        return result;
    }
}
