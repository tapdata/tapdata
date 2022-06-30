package com.tapdata.tm.insights.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.tapdata.tm.base.entity.BaseEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.util.Date;
import java.util.List;
import java.util.Map;


/**
 * Modules
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("Insights")
public class InsightsEntity extends BaseEntity {

    @Field("stats_category")
    private  String statsCategory;

    @Field("stats_name")
    private String statsName;

    @Field("aggregate_time")
    private Date aggregateTime;

    private Map<String,Object> data;

    @Field("stats_granularity")
    private String statsGranularity;

    @Field("stats_keys")
    private  Map<String,Object> statsKeys;


    @Field("stats_time")
    private String statsTime;

}