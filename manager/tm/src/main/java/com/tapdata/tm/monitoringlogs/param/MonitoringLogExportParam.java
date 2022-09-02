package com.tapdata.tm.monitoringlogs.param;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;

/**
 * @author Dexter
 */
@Data
public class MonitoringLogExportParam {

    private Long start;
    private Long end;
    private String taskId;
    private String taskRecordId;

    public boolean isStartEndValid() {
        return null != start && null != end && start < end;
    }

    public Long getStart() {
        if (isStartEndValid()) {
            return start;
        }

        throw new RuntimeException("Invalid value for start or end");
    }

    public Long getEnd() {
        if (isStartEndValid()) {
            return end;
        }

        throw new RuntimeException("Invalid value for start or end");
    }
}
