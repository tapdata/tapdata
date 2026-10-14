package com.tapdata.tm.fieldorigin.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import com.tapdata.tm.fieldorigin.builder.FieldOriginStats;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;

/**
 * One build of the field origins of a task. Only one SUCCESS version per task is active.
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document(FieldOriginVersionEntity.COLLECTION)
public class FieldOriginVersionEntity extends BaseEntity {
    public static final String COLLECTION = "FieldOriginVersion";
    public static final String STATUS_BUILDING = "BUILDING";
    public static final String STATUS_SUCCESS = "SUCCESS";
    public static final String STATUS_FAILED = "FAILED";
    private String taskId;
    private Long version;
    private String configHash;
    private String status;
    private Boolean active;
    private FieldOriginStats stats;
    private String error;
    private String trigger;
    private Date buildStartAt;
    private Date buildEndAt;
}
