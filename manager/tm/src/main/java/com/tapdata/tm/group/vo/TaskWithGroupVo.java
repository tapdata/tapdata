package com.tapdata.tm.group.vo;

import com.tapdata.tm.commons.task.dto.TaskDto;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class TaskWithGroupVo extends TaskDto {
    private String groupId;
    private String groupName;
}
