package com.tapdata.tm.task.vo;

import com.tapdata.tm.commons.dag.NodeEnum;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;

@Data
public class RelationTaskRequest {
    public final static String type_logCollector = NodeEnum.logCollector.name();
    public final static String type_shareCache = NodeEnum.mem_cache.name();
    public final static String type_inspect = "inspect";
    public final static String type_task_by_collector = "task_by_collector";

    @Schema(description = "任务类型 logCollector mem_cache inspect")
    private String type;
    private String status;
    private String keyword;
    private String taskId;
    private String taskRecordId;
}
