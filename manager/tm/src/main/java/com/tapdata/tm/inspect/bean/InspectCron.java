package com.tapdata.tm.inspect.bean;

import lombok.Data;

@Data
public class InspectCron {
    int scheduleTimes;
    private Long intervals;  // 间隔时间
    private String intervalsUnit;  // second、minute、hour、day、week、month
}