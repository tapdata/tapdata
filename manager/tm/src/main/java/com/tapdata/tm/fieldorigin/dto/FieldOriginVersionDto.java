package com.tapdata.tm.fieldorigin.dto;

import com.tapdata.tm.fieldorigin.builder.FieldOriginStats;
import lombok.Data;

import java.util.Date;

@Data
public class FieldOriginVersionDto {
    private String taskId;
    private Long version;
    private String configHash;
    private String status;
    private Boolean active;
    private FieldOriginStats stats;
    private String error;
    private String trigger;
    private Date buildStartAt;
    private Date buildEndAt;
}
