package com.tapdata.tm.monitor.param;

import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * @author Dexter
 */
@Data
public class MeasurementQueryParam {
    Long startAt;
    Long endAt;
    Map<String, MeasurementQuerySample> samples;

    private boolean isStartEndValid() {
        return null != startAt && null != endAt;
    }

    public Long getStartAt() {
        if (isStartEndValid()) {
            return startAt;
        }
        throw new RuntimeException("Invalid value for startAt or endAt");
    }

    public Long getEndAt() {
        if (isStartEndValid()) {
            return endAt;
        }
        throw new RuntimeException("Invalid value for startAt or endAt");
    }

    @Data
    public static class MeasurementQuerySample {
        public static final String MEASUREMENT_QUERY_SAMPLE_TYPE_INSTANT = "instant";
        public static final String MEASUREMENT_QUERY_SAMPLE_TYPE_DIFFERENCE = "difference";
        public static final String MEASUREMENT_QUERY_SAMPLE_TYPE_CONTINUOUS = "continuous";

        Map<String, String> tags;
        List<String> fields;
        String type;
        Long startAt;
        Long endAt;
    }
}
