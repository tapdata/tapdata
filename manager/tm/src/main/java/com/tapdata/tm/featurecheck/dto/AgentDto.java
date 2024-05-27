package com.tapdata.tm.featurecheck.dto;

import lombok.Data;

@Data
public class AgentDto {
    private String id;
    private String version;
    private int limitTasks;
    private int runningTasks;
}
