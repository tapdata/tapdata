package com.tapdata.tm.task.vo;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.Date;

@Data
@ToString
@NoArgsConstructor
public class TaskRecordListVo {
    private String taskId;
    private String taskRecordId;
    private Date startDate;
    private Date endDate;
    private String operator;
    private String status;
    private Long syncNum;
    private Long diffNum;
}
