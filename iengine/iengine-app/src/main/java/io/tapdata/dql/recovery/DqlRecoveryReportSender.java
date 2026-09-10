package io.tapdata.dql.recovery;

import com.tapdata.tm.dql.dto.DqlRecoveryMessageDto;
import io.tapdata.dql.model.DqlRecoveryReport;
import io.tapdata.dql.model.DqlRecoveryNodeState;

import java.util.List;

/** Sends lifecycle callbacks for an accepted recovery batch. */
@FunctionalInterface
public interface DqlRecoveryReportSender {
    void send(DqlRecoveryMessageDto command, DqlRecoveryReport report);

    default void reportBatchStarted(DqlRecoveryMessageDto command) {
        send(command, DqlRecoveryReport.batchStarted(command.getBatchId(), System.currentTimeMillis()));
    }

    default void reportBatchHeartbeat(DqlRecoveryMessageDto command) {
        send(command, DqlRecoveryReport.batchHeartbeat(command.getBatchId(), System.currentTimeMillis()));
    }

    default void reportEventStarted(DqlRecoveryMessageDto command,
                                    String eventId,
                                    String attemptId,
                                    long startedAt) {
        send(command, DqlRecoveryReport.eventStarted(command.getBatchId(), eventId, attemptId, startedAt));
    }

    default void reportEventResult(DqlRecoveryMessageDto command,
                                   String eventId,
                                   String attemptId,
                                   String result,
                                   String message,
                                   long startedAt,
                                   long finishedAt) {
        reportEventResult(command, eventId, attemptId, result, message,
                null, null, startedAt, finishedAt);
    }

    default void reportEventResult(DqlRecoveryMessageDto command,
                                   String eventId,
                                   String attemptId,
                                   String result,
                                   String message,
                                   String errorCode,
                                   String errorDetails,
                                   long startedAt,
                                   long finishedAt) {
        DqlRecoveryReport report = DqlRecoveryReport.eventResult(
                command.getBatchId(), eventId, attemptId, result, message, startedAt, finishedAt);
        report.setErrorCode(errorCode);
        report.setErrorDetails(errorDetails);
        send(command, report);
    }

    default void reportBatchFinished(DqlRecoveryMessageDto command, String message, long finishedAt) {
        reportBatchFinished(command, message, finishedAt, List.of());
    }

    default void reportBatchFinished(DqlRecoveryMessageDto command,
                                     String message,
                                     long finishedAt,
                                     List<DqlRecoveryNodeState> nodeStates) {
        DqlRecoveryReport report = DqlRecoveryReport.batchFinished(command.getBatchId(), message, finishedAt);
        report.setNodeStates(nodeStates);
        send(command, report);
    }

    default void reportBatchFailed(DqlRecoveryMessageDto command, String message, long finishedAt) {
        reportBatchFailed(command, message, finishedAt, List.of());
    }

    default void reportBatchFailed(DqlRecoveryMessageDto command,
                                   String message,
                                   long finishedAt,
                                   List<DqlRecoveryNodeState> nodeStates) {
        DqlRecoveryReport report = DqlRecoveryReport.batchFailed(command.getBatchId(), message, finishedAt);
        report.setNodeStates(nodeStates);
        send(command, report);
    }
}
