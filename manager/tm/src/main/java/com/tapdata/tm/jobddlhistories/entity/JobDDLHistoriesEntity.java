package com.tapdata.tm.jobddlhistories.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;
import com.tapdata.tm.base.entity.BaseEntity;
import org.springframework.data.mongodb.core.mapping.Document;


/**
 * JobDDLHistories
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("JobDDLHistories")
public class JobDDLHistoriesEntity extends BaseEntity {

    private String jobId;

    private String databaseName;

    private String line;

    private Object source;

    private Object position;

    private String ddl;

    private String sourceConnId;
}