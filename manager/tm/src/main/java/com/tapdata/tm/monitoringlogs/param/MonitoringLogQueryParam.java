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
public class MonitoringLogQueryParam {
    public static final String ORDER_ASC = "asc";
    public static final String ORDER_DESC = "desc";

    public static final String LEVEL_DEBUG = "DEBUG";
    public static final String LEVEL_INFO = "INFO";
    public static final String LEVEL_WARN = "WARN";
    public static final String LEVEL_ERROR = "ERROR";

    private Long start;
    private Long end;
    private Long page;
    private Long pageSize;
    private String order;
    private String taskId;
    private String nodeId;
    private String taskRecordId;
    private String search;
    private List<String> levels;

    public boolean isOrderValid() {
        return StringUtils.equalsAny(order, ORDER_ASC, ORDER_DESC);
    }

    public boolean isStartEndValid() {
        return null != start && null != end && start < end;
    }

    public boolean isPaginationValid() {
        return null != page && null != pageSize && page >= 1 & pageSize > 0;
    }

    public boolean isLevelsValid() {
        if (null == levels) {
            return true;
        }

        boolean valid = true;
        for (String level : levels) {
            if (!StringUtils.equalsAnyIgnoreCase(level, getFullLevels().toArray(new String[]{}))) {
                valid = false;
                break;
            }
        }

        return valid;
    }

    public String getOrder() {
        if (isOrderValid()) {
            return order;
        }

        throw new RuntimeException("Invalid value for order, only \"asc\" and \"desc\" are valid");
    }

    public Long getPage() {
        if (isPaginationValid()) {
            return page;
        }

        throw new RuntimeException("Invalid value for page or pageSize");
    }

    public Long getPageSize() {
        if (isPaginationValid()) {
            return pageSize;
        }

        throw new RuntimeException("Invalid value for page or pageSize");
    }

    public List<String> getLevels() {
        if (null == levels) {
            return levels;
        }

        if (isLevelsValid()) {
            return levels.stream().map(String::toUpperCase).collect(Collectors.toList());
        }

        throw new RuntimeException("Invalid value for levels, only \"debug\", \"info\", \"warn\" " +
                "and \"error\" are valid");
    }

    public List<String> getFullLevels() {
        return Arrays.asList(
                LEVEL_DEBUG, LEVEL_INFO, LEVEL_WARN, LEVEL_ERROR
        );
    }
}
