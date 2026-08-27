package io.tapdata.dql.recovery;

import com.tapdata.tm.dql.dto.DqlRecoveryMessageDto;
import io.tapdata.dql.model.DqlRecoveryReport;

/** Sends lifecycle callbacks for an accepted recovery batch. */
@FunctionalInterface
public interface DqlRecoveryReportSender {
    void send(DqlRecoveryMessageDto command, DqlRecoveryReport report);

    default void reportBatchStarted(DqlRecoveryMessageDto command) {
        send(command, DqlRecoveryReport.batchStarted(command.getBatchId(), System.currentTimeMillis()));
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
        send(command, DqlRecoveryReport.eventResult(
                command.getBatchId(), eventId, attemptId, result, message, startedAt, finishedAt));
    }

    default void reportBatchFinished(DqlRecoveryMessageDto command, String message, long finishedAt) {
        send(command, DqlRecoveryReport.batchFinished(command.getBatchId(), message, finishedAt));
    }

    default void reportBatchFailed(DqlRecoveryMessageDto command, String message, long finishedAt) {
        send(command, DqlRecoveryReport.batchFailed(command.getBatchId(), message, finishedAt));
    }
}
