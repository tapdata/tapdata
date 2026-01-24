package com.tapdata.tm.v2.api.common.main.dto;

import com.tapdata.tm.v2.api.monitor.main.enums.TimeGranularity;
import lombok.Data;

@Data
public class TimeRange {
    long start;
    long end;
    TimeGranularity unit;

    public TimeRange(long start, long end, TimeGranularity unit) {
        this.start = start;
        this.end = end;
        this.unit = unit;
    }
}