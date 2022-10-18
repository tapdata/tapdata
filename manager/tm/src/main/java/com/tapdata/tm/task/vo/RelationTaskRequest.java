package com.tapdata.tm.task.vo;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;

@Data
public class RelationTaskRequest {
    public final static String type_logCollector = "logCollector";
    public final static String type_shareCache = "shareCache";
    public final static String type_inspect = "inspect";

    @Schema(description = "任务类型 logCollector shareCache inspect")
    private String type;
    private String status;
    private String keyword;
    private String taskId;
    private String taskRecordId;
}
