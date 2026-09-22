package io.tapdata.dql.model;

import lombok.Data;

/**
 * TM acknowledgement for the later-success overwrite-risk callback.
 */
@Data
public class DqlRecordSuccessReportResult {
    private boolean marked;
    private String eventId;
    private String recordIdentity;
    private String overwriteRiskMessage;
}
