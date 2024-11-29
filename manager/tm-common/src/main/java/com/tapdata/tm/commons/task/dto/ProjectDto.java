package com.tapdata.tm.commons.task.dto;

import java.util.Map;

import com.tapdata.tm.commons.base.IDataPermissionDto;
import com.tapdata.tm.commons.base.dto.SchedulableDto;

import lombok.Data;

@Data
public class ProjectDto extends SchedulableDto implements IDataPermissionDto {
    
    // project名称
    public String name;

    // project描述
    public String description;

    // 定时启动规则
    public String crontabExpression;

    // {taskName: TaskDto}, 任务名称和任务配置
    public Map<String, TaskDto> tasks;

    // {taskName: dependsOnInfo}, 任务名称和依赖信息
    public Map<String, String> taskDepends;

    /*
     * status 状态
     */
    public static final String STATUS_EDIT = "edit";
    public static final String STATUS_WAIT_RUN = "wait_run";
    public static final String STATUS_WAIT_START = "wait_start";
    public static final String STATUS_RUNNING = "running";
    public static final String STATUS_STOPPING = "stopping";
    public static final String STATUS_STOP = "stop";
    public static final String STATUS_ERROR = "error";
    public static final String STATUS_COMPLETE = "complete";
    public static final String STATUS_DELETE = "deleting";
    public static final String STATUS_DELETE_FAILED = "delete_failed";
    public static final String STATUS_RENEWING = "renewing";
    public static final String STATUS_RENEW_FAILED = "renew_failed";

    // status 状态
    private String status;

}
