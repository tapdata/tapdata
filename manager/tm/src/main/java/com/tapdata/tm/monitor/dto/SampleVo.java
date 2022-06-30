package com.tapdata.tm.monitor.dto;

import lombok.Data;
import org.bson.codecs.pojo.annotations.BsonProperty;
import org.springframework.data.mongodb.core.mapping.Field;

import java.util.Date;
import java.util.List;
import java.util.Map;

@Data
public class SampleVo {
    private List<Map<String, Number>> values;
    private List<Long> time;
}
