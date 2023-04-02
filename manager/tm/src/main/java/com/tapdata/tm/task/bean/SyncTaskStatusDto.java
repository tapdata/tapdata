package com.tapdata.tm.task.bean;

import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import lombok.*;

/**
 * @author liujiaxin
 */
@Data
@ToString
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SyncTaskStatusDto {
    private String taskId;
    private String taskName;
    private String taskRecordId;
    private String taskStatus;
    private String updateBy;
    private String updatorName;
    private String agentId;
    private String syncType;
    private String userId;
    private TaskDto taskDto;
    private UserDetail userDetail;
}
