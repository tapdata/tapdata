package com.tapdata.tm.monitoringlogs.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;
import com.tapdata.tm.base.entity.BaseEntity;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;
import java.util.List;
import java.util.Map;


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
    private String taskRecordId;
    private String taskName;
    private String nodeId;
    private String nodeName;
    private Integer version;
    private String message;
    private String errorStack;
    private List<String> logTags;
    private List<Map<String, Object>> data;
    private String dataJson;
}