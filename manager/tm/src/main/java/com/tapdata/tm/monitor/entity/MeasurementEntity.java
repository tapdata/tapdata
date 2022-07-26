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
    @BsonId
    private String id;

    public static final String GRANULARITY_MINUTE = "minute";
    public static final String GRANULARITY_HOUR = "hour";
    //    public static final String GRANULARITY_SECOND = "second";
    public static final String GRANULARITY_DAY = "day";
    public static final String GRANULARITY_MONTH = "month";

    public static final String FIELD_GRANULARITY = "grnty";
    public static final String FIELD_DATE = "date";
    public static final String FIELD_SAMPLE_SIZE = "size";
    public static final String FIELD_FIRST = "first";
    public static final String FIELD_LAST = "last";
    public static final String FIELD_TAGS = "tags";
    public static final String FIELD_SAMPLES = "ss";
    public static final String FIELD_STATISTICS = "statistics";

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
    @Field(FIELD_STATISTICS)
    private Map<String, Number> statistics;

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
