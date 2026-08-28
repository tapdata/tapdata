package io.tapdata.dql.recovery;

import com.tapdata.tm.dql.dto.DqlRecoveryMessageDto;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.Objects;

/**
 * Validates and accepts TM recovery commands exactly once per batch in this
 * Engine process.
 */
public class DqlRecoveryMessageHandler {
    private final DqlRecoveryCoordinator coordinator;
    private final DqlRecoveryReportSender reportSender;
    private final DqlRecoveryTaskContextProvider taskContextProvider;
    private final String currentAgentId;
    private final DqlRecoveryBatchRegistry batchRegistry;
    private final int maxEventCount;

    public DqlRecoveryMessageHandler(DqlRecoveryCoordinator coordinator,
                                     DqlRecoveryReportSender reportSender,
                                     DqlRecoveryTaskContextProvider taskContextProvider,
                                     String currentAgentId) {
        this(coordinator, reportSender, taskContextProvider, currentAgentId,
                new DqlRecoveryBatchRegistry(), DqlRecoveryMessageParser.MAX_EVENT_COUNT);
    }

    public DqlRecoveryMessageHandler(DqlRecoveryCoordinator coordinator,
                                     DqlRecoveryReportSender reportSender,
                                     DqlRecoveryTaskContextProvider taskContextProvider,
                                     String currentAgentId,
                                     DqlRecoveryBatchRegistry batchRegistry) {
        this(coordinator, reportSender, taskContextProvider, currentAgentId, batchRegistry,
                DqlRecoveryMessageParser.MAX_EVENT_COUNT);
    }

    public DqlRecoveryMessageHandler(DqlRecoveryCoordinator coordinator,
                                     DqlRecoveryReportSender reportSender,
                                     DqlRecoveryTaskContextProvider taskContextProvider,
                                     String currentAgentId,
                                     DqlRecoveryBatchRegistry batchRegistry,
                                     int maxEventCount) {
        this.coordinator = coordinator;
        this.reportSender = reportSender;
        this.taskContextProvider = taskContextProvider;
        this.currentAgentId = currentAgentId;
        this.batchRegistry = Objects.requireNonNull(batchRegistry, "batchRegistry must not be null");
        if (maxEventCount <= 0) {
            throw new IllegalArgumentException("maxEventCount must be greater than zero");
        }
        this.maxEventCount = maxEventCount;
    }

    public DqlRecoveryHandleResult handle(Map<?, ?> payload) {
        final DqlRecoveryMessageDto command;
        try {
            command = DqlRecoveryMessageParser.parse(payload, maxEventCount);
        } catch (DqlRecoveryMessageValidationException exception) {
            return DqlRecoveryHandleResult.rejected(exception.getMessage());
        }

        if (!batchRegistry.claim(command.getBatchId())) {
            return DqlRecoveryHandleResult.duplicate(command);
        }

        boolean batchStartedReported = false;
        try {
            validateTaskContext(command);
            if (coordinator == null) {
                String message = "DQL recovery cannot start because the Engine recovery coordinator is unavailable";
                // The batch is already DISPATCHED in TM. A direct terminal
                // callback is required here because this failure happens
                // before BATCH_STARTED can be published.
                reportBatchFailed(command, message);
                batchRegistry.release(command.getBatchId());
                return DqlRecoveryHandleResult.rejected(message);
            }
            if (reportSender == null) {
                throw new IllegalStateException("recovery report sender is unavailable");
            }
            // The coordinator starts work asynchronously and may report
            // EVENT_STARTED before start() returns. TM accepts that callback
            // only after the batch has transitioned from DISPATCHED to
            // RUNNING, so publish BATCH_STARTED first.
            reportSender.reportBatchStarted(command);
            batchStartedReported = true;
            coordinator.start(command);
            return DqlRecoveryHandleResult.accepted(command);
        } catch (RuntimeException exception) {
            // If TM accepted BATCH_STARTED, finish the failed lifecycle so
            // event locks are released immediately. If that callback itself
            // failed, its outcome is unknown; release the local claim and let
            // a redelivery retry the idempotent TM callback.
            if (batchStartedReported) {
                new DqlRecoveryFailureCompensator(
                        message -> reportBatchFailed(command, message)
                ).compensate(exception);
            } else {
                batchRegistry.release(command.getBatchId());
            }
            return DqlRecoveryHandleResult.rejected(safeMessage(exception));
        }
    }

    private void reportBatchFailed(DqlRecoveryMessageDto command, String message) {
        try {
            reportSender.reportBatchFailed(command, message, System.currentTimeMillis());
        } catch (RuntimeException ignored) {
            // TM callback failure is best effort; the accepted batch remains
            // claimed and can converge through the TM timeout scanner.
        }
    }

    private void validateTaskContext(DqlRecoveryMessageDto command) {
        if (taskContextProvider == null) {
            throw new IllegalStateException("recovery task context provider is unavailable");
        }
        if (StringUtils.isBlank(currentAgentId)) {
            throw new DqlRecoveryMessageValidationException("current agent id is unavailable");
        }
        DqlRecoveryTaskContext context = taskContextProvider.find(command.getTaskId());
        if (context == null) {
            throw new DqlRecoveryMessageValidationException("recovery task was not found");
        }
        if (!Objects.equals(command.getTaskId(), context.taskId())) {
            throw new DqlRecoveryMessageValidationException("recovery task id does not match");
        }
        if (!Objects.equals(command.getTaskVersion(), context.taskVersion())) {
            throw new DqlRecoveryMessageValidationException("recovery task version does not match");
        }
        if (!Objects.equals(currentAgentId, context.agentId())) {
            throw new DqlRecoveryMessageValidationException("recovery task agent does not match current agent");
        }
    }

    private String safeMessage(RuntimeException exception) {
        String message = exception.getMessage();
        return StringUtils.isBlank(message) ? "recovery message was rejected" : message;
    }
}
