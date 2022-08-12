package com.tapdata.tm.commons.task.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.bson.types.ObjectId;

/**
 * SubTask
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class TaskRunHistoryDto extends BaseDto {

    public static final String ACTION_RUN = "run";
    public static final String ACTION_STOP = "stop";

    /** 任务id */
    private ObjectId taskId;

    /** 任务名称 */
    private String taskName;
    /** 动作  run 启动 stop 停止 */
    private String action;
}
