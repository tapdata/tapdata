package io.tapdata.dql.model;

import lombok.Data;

/**
 * TM acknowledgement for an Engine DQL event report.
 */
@Data
public class DqlEventReportResult {
    private String eventId;
    private String status;
    private boolean duplicate;
}
