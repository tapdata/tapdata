package com.tapdata.tm.monitor.dto;

import lombok.Data;

import java.util.Date;
import java.util.Map;

@Data
public class StatisticVo {
    private Map<String, Number> values;
    private Date time;
}
