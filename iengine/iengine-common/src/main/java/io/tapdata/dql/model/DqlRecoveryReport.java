package io.tapdata.dql.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

import java.util.List;

/**
 * Engine request body for the TM DLQ recovery callback.
 *
 * <p>The callback is intentionally a small, stable contract. Event payloads
 * are never copied into a recovery report.</p>
 */
@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DqlRecoveryReport {
    public static final String BATCH_STARTED = "BATCH_STARTED";
    public static final String BATCH_HEARTBEAT = "BATCH_HEARTBEAT";
    public static final String EVENT_STARTED = "EVENT_STARTED";
    public static final String EVENT_RESULT = "EVENT_RESULT";
    public static final String BATCH_FINISHED = "BATCH_FINISHED";
    public static final String BATCH_FAILED = "BATCH_FAILED";

    private String batchId;
    private String eventId;
    private String attemptId;
    private String type;
    private String result;
    private String message;
    private String errorCode;
    private String errorDetails;
    private Long startedAt;
    private Long pingTime;
    private Long finishedAt;
    private List<DqlRecoveryNodeState> nodeStates;

    public static DqlRecoveryReport batchStarted(String batchId, long startedAt) {
        DqlRecoveryReport report = new DqlRecoveryReport();
        report.setBatchId(batchId);
        report.setType(BATCH_STARTED);
        report.setStartedAt(startedAt);
        return report;
    }

    public static DqlRecoveryReport batchHeartbeat(String batchId, long pingTime) {
        DqlRecoveryReport report = new DqlRecoveryReport();
        report.setBatchId(batchId);
        report.setType(BATCH_HEARTBEAT);
        report.setPingTime(pingTime);
        return report;
    }

    public static DqlRecoveryReport eventStarted(String batchId,
                                                 String eventId,
                                                 String attemptId,
                                                 long startedAt) {
        DqlRecoveryReport report = new DqlRecoveryReport();
        report.setBatchId(batchId);
        report.setEventId(eventId);
        report.setAttemptId(attemptId);
        report.setType(EVENT_STARTED);
        report.setStartedAt(startedAt);
        return report;
    }

    public static DqlRecoveryReport eventResult(String batchId,
                                                String eventId,
                                                String attemptId,
                                                String result,
                                                String message,
                                                long startedAt,
                                                long finishedAt) {
        DqlRecoveryReport report = eventStarted(batchId, eventId, attemptId, startedAt);
        report.setType(EVENT_RESULT);
        report.setResult(result);
        report.setMessage(message);
        report.setFinishedAt(finishedAt);
        return report;
    }

    public static DqlRecoveryReport batchFinished(String batchId,
                                                  String message,
                                                  long finishedAt) {
        DqlRecoveryReport report = new DqlRecoveryReport();
        report.setBatchId(batchId);
        report.setType(BATCH_FINISHED);
        report.setMessage(message);
        report.setFinishedAt(finishedAt);
        return report;
    }

    public static DqlRecoveryReport batchFailed(String batchId,
                                                String message,
                                                long finishedAt) {
        DqlRecoveryReport report = batchFinished(batchId, message, finishedAt);
        report.setType(BATCH_FAILED);
        return report;
    }
}
