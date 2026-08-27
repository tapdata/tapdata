package com.tapdata.tm.userLog.param;

import com.tapdata.tm.userLog.constant.AuditEventType;
import com.tapdata.tm.userLog.constant.AuditOutcome;
import lombok.Data;

/**
 * Common audit event input. Sensitive values must never be passed in this object.
 */
@Data
public class AuditLogParam {
    private AuditEventType eventType;
    private AuditOutcome outcome;
    private String userId;
    private String customerId;
    private String username;
    private String action;
    private String objectName;
    private String failureReason;
    private String changeSummary;
    private String serviceNode;
    private String componentType;
    private String instanceName;
    private String loginMethod;
    private String parameter1;
}
