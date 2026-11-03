package com.tapdata.tm.ds.dto;

import lombok.Data;

import java.util.List;

@Data
public class BatchUpdateAgentSettingsResponse {
    private String requestId;
    private long updatedCount;
    private List<String> updatedConnectionIds;
}
