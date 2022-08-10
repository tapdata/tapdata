package com.tapdata.tm.task.bean;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@ToString
@NoArgsConstructor
public class SyncTaskStatusDto {
    private String taskRecordId;
    private String taskStatus;
}
