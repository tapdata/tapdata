package com.tapdata.tm.dql.entity;

import com.tapdata.tm.base.entity.Entity;
import com.tapdata.tm.dql.dto.DqlRecoveryBatchDto;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.util.Date;
import java.util.List;

@Data
@Document("dql_recovery_batches")
@EqualsAndHashCode(callSuper = true)
public class DqlRecoveryBatchEntity extends Entity {
    @Field(DqlRecoveryBatchDto.FIELD_BATCH_ID)
    private String batchId;
    @Field(DqlRecoveryBatchDto.FIELD_TASK_ID)
    private String taskId;
    @Field(DqlRecoveryBatchDto.FIELD_TASK_NAME)
    private String taskName;
    @Field(DqlRecoveryBatchDto.FIELD_TASK_STATUS_BEFORE)
    private String taskStatusBefore;
    @Field(DqlRecoveryBatchDto.FIELD_TASK_VERSION)
    private Long taskVersion;
    @Field(DqlRecoveryBatchDto.FIELD_AGENT_ID)
    private String agentId;
    @Field(DqlRecoveryBatchDto.FIELD_EVENT_IDS)
    private List<String> eventIds;
    @Field(DqlRecoveryBatchDto.FIELD_ORDERED_EVENT_IDS)
    private List<String> orderedEventIds;
    @Field(DqlRecoveryBatchDto.FIELD_OPERATOR_ID)
    private String operatorId;
    @Field(DqlRecoveryBatchDto.FIELD_OPERATOR_NAME)
    private String operatorName;
    private String status;
    @Field(DqlRecoveryBatchDto.FIELD_SELECTED_COUNT)
    private Integer selectedCount;
    @Field(DqlRecoveryBatchDto.FIELD_SUCCESS_COUNT)
    private Integer successCount;
    @Field(DqlRecoveryBatchDto.FIELD_FAILED_COUNT)
    private Integer failedCount;
    @Field(DqlRecoveryBatchDto.FIELD_SKIPPED_COUNT)
    private Integer skippedCount;
    @Field(DqlRecoveryBatchDto.FIELD_STARTED_AT)
    private Date startedAt;
    @Field(DqlRecoveryBatchDto.FIELD_FINISHED_AT)
    private Date finishedAt;
    private String message;
    private Date created;
    private Date updated;
}
