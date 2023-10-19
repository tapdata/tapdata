package com.tapdata.tm.monitor.entity;

import io.tapdata.common.sample.request.Sample;
import io.tapdata.common.utils.NumberUtils;
import lombok.Data;
import org.bson.codecs.pojo.annotations.BsonId;
import org.springframework.data.mongodb.core.mapping.Field;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class MeasurementEntity {
    public static final String COLLECTION_NAME = "AgentMeasurementV2";
    public static final String FIELD_ID = "_id";
    public static final String FIELD_GRANULARITY = "grnty";
    public static final String FIELD_DATE = "date";
    public static final String FIELD_SAMPLE_SIZE = "size";
    public static final String FIELD_FIRST = "first";
    public static final String FIELD_LAST = "last";
    public static final String FIELD_TAGS = "tags";
    public static final String FIELD_SAMPLES = "ss";

    @BsonId
    private String id;

    @Field(FIELD_GRANULARITY)
    private String granularity;
    @Field(FIELD_DATE)
    private Date date;
    @Field(FIELD_SAMPLE_SIZE)
    private Long sampleSize;
    @Field(FIELD_FIRST)
    private Date first;
    @Field(FIELD_LAST)
    private Date last;

    @Field(FIELD_TAGS)
    private Map<String, String> tags;
    @Field(FIELD_SAMPLES)
    private List<Sample> samples;

    public Map<String, Number> averageValues() {
        Map<String, Number> finalKeyValueMap = new HashMap<>();
        Map<String, Number> keyValueMap = new HashMap<>();
        Map<String, Integer> keyCounterMap = new HashMap<>();
        if(samples != null) {
            for(Sample sample : samples) {
                Map<String, Number> map = sample.getVs();
                for(Map.Entry<String, Number> entry : map.entrySet()) {
                    Number number = keyValueMap.get(entry.getKey());
                    if(number == null) {
                        number = entry.getValue();
                    } else {
                        if (null == entry.getValue()) {
                            continue;
                        }
                        number = NumberUtils.addNumbers(number, entry.getValue());
                    }
                    keyValueMap.put(entry.getKey(), number);

                    Integer counter = keyCounterMap.get(entry.getKey());
                    if(counter != null) {
                        counter++;
                    } else {
                        counter = 1;
                    }
                    keyCounterMap.put(entry.getKey(), counter);
                }
            }
            if(!keyValueMap.isEmpty()) {
                for(Map.Entry<String, Number> entry : keyValueMap.entrySet()) {
                    Integer counter = keyCounterMap.get(entry.getKey());
                    Number value = entry.getValue();
                    if(counter != null && value != null && counter > 0) {
                        finalKeyValueMap.put(entry.getKey(), NumberUtils.divideNumbers(entry.getValue(), counter));
                    } else {
                        finalKeyValueMap.put(entry.getKey(), null);
                    }
                }
            }
        }
        return finalKeyValueMap;
    }
}
