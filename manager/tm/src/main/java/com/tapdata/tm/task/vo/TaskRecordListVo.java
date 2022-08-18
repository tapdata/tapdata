package com.tapdata.tm.task.vo;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@ToString
@NoArgsConstructor
public class TaskRecordListVo {
    private String taskId;
    private String taskRecordId;
    private String startDate;
    private String endDate;
    private String operator;
    private String status;
    private Long inputTotal;
    private Long outputTotal;
}
