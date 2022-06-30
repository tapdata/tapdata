package com.tapdata.tm.jobddlhistories.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;
import com.tapdata.tm.commons.base.dto.BaseDto;


/**
 * JobDDLHistories
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class JobDDLHistoriesDto extends BaseDto {

    private String jobId;

    private String databaseName;

    private String line;

    private Object source;

    private Object position;

    private String ddl;

    private String sourceConnId;
}
