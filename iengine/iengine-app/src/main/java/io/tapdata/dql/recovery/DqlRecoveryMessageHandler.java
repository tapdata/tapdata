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

    public DqlRecoveryMessageHandler(DqlRecoveryCoordinator coordinator,
                                     DqlRecoveryReportSender reportSender,
                                     DqlRecoveryTaskContextProvider taskContextProvider,
                                     String currentAgentId) {
        this(coordinator, reportSender, taskContextProvider, currentAgentId,
                new DqlRecoveryBatchRegistry());
    }

    public DqlRecoveryMessageHandler(DqlRecoveryCoordinator coordinator,
                                     DqlRecoveryReportSender reportSender,
                                     DqlRecoveryTaskContextProvider taskContextProvider,
                                     String currentAgentId,
                                     DqlRecoveryBatchRegistry batchRegistry) {
        this.coordinator = coordinator;
        this.reportSender = reportSender;
        this.taskContextProvider = taskContextProvider;
        this.currentAgentId = currentAgentId;
        this.batchRegistry = Objects.requireNonNull(batchRegistry, "batchRegistry must not be null");
    }

    public DqlRecoveryHandleResult handle(Map<?, ?> payload) {
        final DqlRecoveryMessageDto command;
        try {
            command = DqlRecoveryMessageParser.parse(payload);
        } catch (DqlRecoveryMessageValidationException exception) {
            return DqlRecoveryHandleResult.rejected(exception.getMessage());
        }

        if (!batchRegistry.claim(command.getBatchId())) {
            return DqlRecoveryHandleResult.duplicate(command);
        }

        boolean coordinatorStarted = false;
        try {
            validateTaskContext(command);
            if (coordinator == null) {
                throw new IllegalStateException("recovery coordinator is unavailable");
            }
            if (reportSender == null) {
                throw new IllegalStateException("recovery report sender is unavailable");
            }
            coordinator.start(command);
            coordinatorStarted = true;
            reportSender.reportBatchStarted(command);
            return DqlRecoveryHandleResult.accepted(command);
        } catch (RuntimeException exception) {
            // A startup failure means no coordinator has accepted the batch;
            // allow TM redelivery to retry initialization. Once start returns,
            // the batch remains claimed even if its callback later fails.
            if (!coordinatorStarted) {
                batchRegistry.release(command.getBatchId());
            }
            return DqlRecoveryHandleResult.rejected(safeMessage(exception));
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
