package com.tapdata.tm.commons.metrics;

import lombok.Data;
import org.bson.codecs.pojo.annotations.BsonId;
import org.springframework.data.mongodb.core.mapping.Field;

import java.io.Serializable;
import java.util.Date;
import java.util.List;
import java.util.Map;


@Data
public class MeasurementEntity  implements Serializable {
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
    private List<SampleVO> samples;
}
