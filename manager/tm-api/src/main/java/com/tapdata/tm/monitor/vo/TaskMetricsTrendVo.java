package com.tapdata.tm.monitor.vo;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class TaskMetricsTrendVo {
    private List<Long> ts = new ArrayList<>();
    private List<Double> outputQps = new ArrayList<>();
    private List<Double> outputSizeQps = new ArrayList<>();
}
