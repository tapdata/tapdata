package com.tapdata.tm.ds.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;
import java.util.List;

@Data
@EqualsAndHashCode(callSuper = true)
public class ConnectionTaskAgentRequest extends ConnectionAgentCheckRequest {
    private List<TaskEdit> taskEdits;
    private AgentSettings taskSettings;
    @Data
    public static class TaskEdit {
        private String id;
        private Long revision;
    }
}
