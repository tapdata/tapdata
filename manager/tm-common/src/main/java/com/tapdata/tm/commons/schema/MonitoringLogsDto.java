package com.tapdata.tm.commons.schema;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.*;
import lombok.extern.jackson.Jacksonized;

import java.util.Date;
import java.util.List;
import java.util.Map;


/**
 * MonitoringLogs
 */
@EqualsAndHashCode(callSuper = true)
@Jacksonized
@NoArgsConstructor
@AllArgsConstructor
@Data
@Builder
public class MonitoringLogsDto extends BaseDto {
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
    @Singular
    private List<String> logTags;
    @Singular("record")
    private List<Map<String, Object>> data;
}