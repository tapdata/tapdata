package com.tapdata.tm.report.dto;

import lombok.Data;

@Data
public class TasksNumBatch {
    private String machineId;
    private String taskType;
    @Override
    public String toString() {
        return "{" +
                "\"machineId\": \"" + machineId + "\"," +
                "\"taskType\": \""+ taskType + "\""+
                "}";
    }
}
