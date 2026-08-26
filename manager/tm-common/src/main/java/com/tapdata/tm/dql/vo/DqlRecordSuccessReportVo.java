package com.tapdata.tm.dql.vo;

import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Engine callback payload for a successful normal record write after DLQ skip is enabled.
 */
@Data
public class DqlRecordSuccessReportVo implements Serializable {
    private String taskRecordId;
    private String sourceTable;
    private String targetTable;
    private String tableId;
    private Map<String, Object> eventKey;
    private String recordIdentity;
    private String recordIdentityType;
    private List<String> recordIdentityFields;
    private String dmlType;
    private Long eventTime;
    private Long captureSeq;
    private String payloadHash;
    private Long successAt;
}
