package io.tapdata.dql.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * Engine request body for the later-success overwrite-risk callback.
 */
@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DqlRecordSuccessReport {
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
