package com.tapdata.tm.dql.vo;

import lombok.Data;

import java.io.Serializable;
import java.util.Date;

@Data
public class DqlEventQueryVo implements Serializable {
    private String taskId;
    private String eventId;
    private String taskName;
    private String sourceTable;
    private String targetTable;
    private String keyword;
    private String dmlType;
    private String errorType;
    private String status;
    private String errorCode;
    private Date startTime;
    private Date endTime;
    private long skip;
    private int limit;
    private String order;
}
