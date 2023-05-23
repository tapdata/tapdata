package com.tapdata.tm.task.vo;

import com.tapdata.tm.commons.dag.NodeEnum;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;

import java.util.Map;
import java.util.Set;

@Data
public class RelationTaskRequest {
    public final static String type_logCollector = NodeEnum.logCollector.name();
    public final static String type_shareCache = NodeEnum.mem_cache.name();
    public final static String type_ConnHeartbeat = TaskDto.SYNC_TYPE_CONN_HEARTBEAT;
    public final static String type_inspect = "inspect";
    public final static String type_task_by_collector = "task_by_collector";
    public final static String type_task_by_collector_table = "task_by_collector_table";

    @Schema(description = "任务类型 logCollector mem_cache inspect connHeartbeat")
    private String type;
    private String status;
    private String keyword;
    private String taskId;
    private String taskRecordId;

    /**
     * connectionId, tableNames
     */
    private Map<String, Set<String>> tableNameMap;
}
