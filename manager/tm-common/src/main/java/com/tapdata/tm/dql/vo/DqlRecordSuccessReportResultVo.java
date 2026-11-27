package com.tapdata.tm.dql.vo;

import lombok.Data;

import java.io.Serializable;

/**
 * Result returned after TM tries to mark a previous DLQ event with later-success overwrite risk.
 */
@Data
public class DqlRecordSuccessReportResultVo implements Serializable {
    private boolean marked;
    private String eventId;
    private String recordIdentity;
    private String overwriteRiskMessage;
}
