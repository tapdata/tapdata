package com.tapdata.tm.worker.dto;

import lombok.Data;

import java.io.Serializable;

@Data
public class WorkSchedule implements Serializable {
    private static final long serialVersionUID = 6223678319818710137L;
    private String processId;
    private Integer weight;
    private Integer taskRunNum;
    private Integer taskLimit;
}
