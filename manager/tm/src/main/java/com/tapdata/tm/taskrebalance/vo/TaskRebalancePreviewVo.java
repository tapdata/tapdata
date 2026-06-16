package com.tapdata.tm.taskrebalance.vo;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Data
public class TaskRebalancePreviewVo {
    private List<TaskPreview> tasks = new ArrayList<>();
    private Integer moveCount = 0;
    private String reason;

    @Data
    public static class TaskPreview {
        private String taskId;
        private String taskName;
        private String type;
        private String syncType;
        private String status;
        private String sourceAgentId;
        private String targetAgentId;
        private Boolean movable = false;
        private String schedulableStatus;
        private Boolean changed = false;
        private String reason;
        private Integer priorityScore = 0;
        private Map<String, Integer> priorityScoreItems;
        private Integer nodeCount = 0;
        private Long startTime;
    }
}
