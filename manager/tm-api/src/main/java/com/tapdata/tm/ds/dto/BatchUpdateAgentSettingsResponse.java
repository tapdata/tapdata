package com.tapdata.tm.ds.dto;

import lombok.Data;

import java.util.List;

@Data
public class BatchUpdateAgentSettingsResponse {
    private String requestId;
    private int requestedCount;
    private long updatedCount;
    private List<String> updatedConnectionIds;
}
