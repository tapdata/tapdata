package com.tapdata.tm.monitor.entity;

import io.tapdata.common.sample.request.Sample;
import lombok.Data;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;
import java.util.List;
import java.util.Map;


@Document("AgentMeasurement")
@Data
public class AgentMeasurementEntity {
    private String id;
    private String granularity;

    private Date date;
    private Date first;
    private Date last;

    private Map<String, String> tags;

    private List<Sample> samples;
}
