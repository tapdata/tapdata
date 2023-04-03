package com.tapdata.tm.commons.schema;

import com.alibaba.fastjson.JSON;
import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.*;
import lombok.extern.jackson.Jacksonized;
import org.apache.commons.lang3.StringUtils;

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
    private String errorCode;
    private String fullErrorCode;

    public String formatMonitoringLog() {
        return "[" + level + "] " + date + " " + formatMonitoringLogMessage();
    }

    public String formatMonitoringLogMessage() {
        StringBuilder sb = new StringBuilder();
        sb.append('[').append(taskName).append(']');
        if (StringUtils.isNotBlank(nodeName)) {
            sb.append('[').append(nodeName).append(']');
        }

        if (null != logTags) {
            for (String tag : logTags) {
                sb.append("[").append(tag).append("]").append(" ");
            }
        }

        sb.append(" - ").append(message).append(" ");
        if (errorStack != null) {
            sb.append(errorStack);
        }
        if (null != data && data.size() > 0) {
            sb.append(JSON.toJSON(data));
        }

        return sb.toString();
    }
}