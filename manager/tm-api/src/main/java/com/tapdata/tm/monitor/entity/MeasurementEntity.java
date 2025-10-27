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
    public static final String INPUT_QPS = "inputQps";
    public static final String OUTPUT_QPS = "outputQps";
    public static final String MAX_INPUT_QPS = "maxInputQps";
    public static final String MAX_OUTPUT_QPS = "maxOutputQps";
    public static final String INPUT_SIZE_QPS = "inputSizeQps";
    public static final String OUTPUT_SIZE_QPS = "outputSizeQps";
    public static final String MAX_INPUT_SIZE_QPS = "maxInputSizeQps";
    public static final String MAX_OUTPUT_SIZE_QPS = "maxOutputSizeQps";
    

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
        Number maxInputQps = 0;
        Number maxOutputQps = 0;
        Number maxInputSizeQps = 0;
        Number maxOutputSizeQps = 0;

        if(samples != null) {
            for(Sample sample : samples) {
                Map<String, Number> map = sample.getVs();
                for(Map.Entry<String, Number> entry : map.entrySet()) {
                    String key = entry.getKey();
                    Number value = entry.getValue();

                    // 跟踪最大输入和输出QPS
                    if (INPUT_QPS.equals(key) && value != null) {
                        if (compareNumbers(value, maxInputQps) > 0) {
                            maxInputQps = value;
                        }
                    } else if (OUTPUT_QPS.equals(key) && value != null) {
                        if (compareNumbers(value, maxOutputQps) > 0) {
                            maxOutputQps = value;
                        }
                    } else if (INPUT_SIZE_QPS.equals(key) && value != null) {
                        if (compareNumbers(value, maxInputSizeQps) > 0) {
                            maxInputSizeQps = value;
                        }
                    } else if (OUTPUT_SIZE_QPS.equals(key) && value != null) {
                        if (compareNumbers(value, maxOutputSizeQps) > 0) {
                            maxOutputSizeQps = value;
                        }
                    }

                    // 计算累计值用于平均值计算
                    Number number = keyValueMap.get(key);
                    if(number == null) {
                        number = value;
                    } else {
                        if (null == value) {
                            continue;
                        }
                        number = NumberUtils.addNumbers(number, value);
                    }
                    keyValueMap.put(key, number);

                    Integer counter = keyCounterMap.get(key);
                    if(counter != null) {
                        counter++;
                    } else {
                        counter = 1;
                    }
                    keyCounterMap.put(key, counter);
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

            finalKeyValueMap.put(MAX_INPUT_QPS, maxInputQps);
            finalKeyValueMap.put(MAX_OUTPUT_QPS, maxOutputQps);
            finalKeyValueMap.put(MAX_INPUT_SIZE_QPS, maxInputSizeQps);
            finalKeyValueMap.put(MAX_OUTPUT_SIZE_QPS, maxOutputSizeQps);
        }
        return finalKeyValueMap;
    }


    private int compareNumbers(Number a, Number b) {
        if (a == null && b == null) return 0;
        if (a == null) return -1;
        if (b == null) return 1;

        return Double.compare(a.doubleValue(), b.doubleValue());
    }
}
