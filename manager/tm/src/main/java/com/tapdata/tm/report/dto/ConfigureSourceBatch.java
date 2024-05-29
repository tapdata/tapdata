package com.tapdata.tm.report.dto;

import lombok.Data;

@Data
public class ConfigureSourceBatch {
    private String machineId;

    private String pdkId;
    @Override
    public String toString() {
        return "{" +
                "\"machineId\": \"" + machineId + "\"," +
                "\"pdkId\": \""+ pdkId + "\""+
                "}";
    }
}
