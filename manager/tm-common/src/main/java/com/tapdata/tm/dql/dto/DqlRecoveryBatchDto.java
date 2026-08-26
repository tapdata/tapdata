package com.tapdata.tm.dql.dto;

import lombok.Data;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

@Data
public class DqlRecoveryBatchDto implements Serializable {
    public static final String FIELD_ID = "_id";
    public static final String FIELD_BATCH_ID = "batch_id";
    public static final String FIELD_TASK_ID = "task_id";
    public static final String FIELD_TASK_NAME = "task_name";
    public static final String FIELD_TASK_STATUS_BEFORE = "task_status_before";
    public static final String FIELD_TASK_VERSION = "task_version";
    public static final String FIELD_AGENT_ID = "agent_id";
    public static final String FIELD_EVENT_IDS = "event_ids";
    public static final String FIELD_ORDERED_EVENT_IDS = "ordered_event_ids";
    public static final String FIELD_OPERATOR_ID = "operator_id";
    public static final String FIELD_OPERATOR_NAME = "operator_name";
    public static final String FIELD_STATUS = "status";
    public static final String FIELD_SELECTED_COUNT = "selected_count";
    public static final String FIELD_SUCCESS_COUNT = "success_count";
    public static final String FIELD_FAILED_COUNT = "failed_count";
    public static final String FIELD_SKIPPED_COUNT = "skipped_count";
    public static final String FIELD_STARTED_AT = "started_at";
    public static final String FIELD_FINISHED_AT = "finished_at";
    public static final String FIELD_MESSAGE = "message";
    public static final String FIELD_CREATED = "created";
    public static final String FIELD_UPDATED = "updated";

    private String id;
    private String batchId;
    private String taskId;
    private String taskName;
    private String taskStatusBefore;
    private Long taskVersion;
    private String agentId;
    private List<String> eventIds;
    private List<String> orderedEventIds;
    private String operatorId;
    private String operatorName;
    private String status;
    private Integer selectedCount;
    private Integer successCount;
    private Integer failedCount;
    private Integer skippedCount;
    private Date startedAt;
    private Date finishedAt;
    private String message;
    private Date created;
    private Date updated;
}
