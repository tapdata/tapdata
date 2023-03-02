package com.tapdata.tm.task.bean;

import lombok.Data;

import java.util.Date;

@Data
public class TableStatusInfoDto {

    private String status;

    private Long  cdcDelayTime;

    private Date  LastDataChangeTime;
}
