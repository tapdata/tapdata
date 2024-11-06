package com.tapdata.tm.monitoringlogs.param;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * @author Dexter
 */
@Data
public class MonitoringLogExportParam {

    private Long start;
    private Long end;
    private String taskId;
    private String taskRecordId;
    private List<String> logTags;

    public boolean isStartEndValid() {
        return null != start && null != end && start < end;
    }
}
