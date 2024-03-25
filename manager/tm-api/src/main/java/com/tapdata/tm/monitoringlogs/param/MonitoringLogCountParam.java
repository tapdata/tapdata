package com.tapdata.tm.monitoringlogs.param;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Dexter
 */
@Data
public class MonitoringLogCountParam {
    private String taskId;
    private String taskRecordId;
}
