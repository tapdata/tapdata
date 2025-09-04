package com.tapdata.tm.apiCalls.vo;

import lombok.Data;

@Data
public class ApiCallStats {
    private String apiId;
    private String workOid;
    private String processId;
    private long totalCount;
    private long okCount;
    private long notOkCount;
}