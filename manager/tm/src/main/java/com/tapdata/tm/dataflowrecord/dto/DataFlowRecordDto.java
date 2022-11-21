package com.tapdata.tm.dataflowrecord.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;


/**
 * DataFlowRecord
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class DataFlowRecordDto extends BaseDto {

    public static final String STATUS_RUNNING = "running";
    public static final String STATUS_COMPLETED = "completed";
    public static final String STATUS_PAUSED = "paused";
    public static final String STATUS_ERROR = "error";

    private String dataFlowId; // 任务id

    private String dataFlowName;  // 任务名称

    private Object dataFlowStartTime;  // 任务开始时间

    private Object dataFlowEndTime;  // 任务结束时间

    private String startType;  // 任务启动方式, manual-手动运行， auto-自动运行

    private String dataFlowStatus;  // 任务状态 running-运行中， completed-已完成， paused-已停止， error-错误
}