package com.tapdata.tm.report.dto;

import lombok.Data;

@Data
public class RunDaysBatch {
    private String machineId;
    @Override
    public String toString() {
        return "{" +
                "\"machineId\": \"" + machineId + "\"" +
                "}";
    }
}
