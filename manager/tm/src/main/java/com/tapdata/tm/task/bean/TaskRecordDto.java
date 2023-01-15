package com.tapdata.tm.task.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TaskRecordDto {
    private String taskId;
    private Integer page;
    private Integer size;
}
