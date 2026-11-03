package com.tapdata.tm.ds.exception;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.ds.dto.ConnectionAgentConflictReport;
import lombok.Getter;

@Getter
public class ConnectionAgentConflictException extends BizException {
    public static final String CODE = "Datasource.AgentTaskConflict";
    private final ConnectionAgentConflictReport report;

    public ConnectionAgentConflictException(ConnectionAgentConflictReport report) {
        super(CODE, report);
        this.report = report;
    }
}
