package io.tapdata.dql.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

/**
 * Engine request body for the TM DQL recovery callback.
 *
 * <p>The callback is intentionally a small, stable contract. Event payloads
 * are never copied into a recovery report.</p>
 */
@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DqlRecoveryReport {
    public static final String BATCH_STARTED = "BATCH_STARTED";

    private String batchId;
    private String eventId;
    private String attemptId;
    private String type;
    private String result;
    private String message;
    private String errorCode;
    private String errorDetails;
    private Long startedAt;
    private Long finishedAt;

    public static DqlRecoveryReport batchStarted(String batchId, long startedAt) {
        DqlRecoveryReport report = new DqlRecoveryReport();
        report.setBatchId(batchId);
        report.setType(BATCH_STARTED);
        report.setStartedAt(startedAt);
        return report;
    }
}
