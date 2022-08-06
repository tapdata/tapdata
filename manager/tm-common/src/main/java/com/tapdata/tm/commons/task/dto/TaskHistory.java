package com.tapdata.tm.commons.task.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Date;

@EqualsAndHashCode(callSuper = true)
@Data
public class TaskHistory extends TaskDto {
    private String taskId;
}
