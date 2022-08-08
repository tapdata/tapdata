package com.tapdata.tm.monitoringlogs.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;
import com.tapdata.tm.base.entity.BaseEntity;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;


/**
 * MonitoringLogs
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("monitoringLogs")
public class MonitoringLogsEntity extends BaseEntity {
    private String level;
    private Long timestamp;
    private Date date;
    private String taskId;
    private Integer version;
    private String message;
    private String errorStack;

}