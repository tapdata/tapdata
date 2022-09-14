package com.tapdata.tm.task.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * @author liujiaxin
 */
@Data
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class SyncTaskStatusDto {
    private String taskId;
    private String taskRecordId;
    private String taskStatus;
}
