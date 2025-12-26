package com.tapdata.tm.task.bean;

import lombok.Data;

import java.util.Date;
import java.util.List;

@Data
public class TableStatusInfoDto {

    private String status;

    private Long  cdcDelayTime;

    private Date  lastDataChangeTime;

    private String taskId;

    private String taskName;

    private List<TableStatusInfoDto> upstreamTableStatus;

    private Boolean onDelayPath;
}
