package com.tapdata.tm.ds.dto;

import lombok.Data;

import java.util.List;

@Data
public class BatchUpdateAgentSettingsRequest {
    private String requestId;
    private List<String> connectionIds;
    private AgentSettings settings;

    @Data
    public static class AgentSettings {
        private String accessNodeType;
        private String accessNodeProcessId;
        private String priorityProcessId;
    }
}
