package com.tapdata.tm.dql.entity;

import com.tapdata.tm.base.entity.Entity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.util.Date;

@Data
@Document(DqlRecoveryTaskLockEntity.COLLECTION_NAME)
@EqualsAndHashCode(callSuper = true)
public class DqlRecoveryTaskLockEntity extends Entity {
    public static final String COLLECTION_NAME = "dql_recovery_locks";
    public static final String FIELD_TASK_ID = "task_id";
    public static final String FIELD_BATCH_ID = "batch_id";
    public static final String FIELD_OWNER = "owner";
    public static final String FIELD_EXPIRE_AT = "expire_at";
    public static final String FIELD_CREATED = "created";

    @Field(FIELD_TASK_ID)
    private String taskId;
    @Field(FIELD_BATCH_ID)
    private String batchId;
    @Field(FIELD_OWNER)
    private String owner;
    @Field(FIELD_EXPIRE_AT)
    private Date expireAt;
    @Field(FIELD_CREATED)
    private Date created;
}
