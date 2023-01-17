package com.tapdata.tm.log.entity;

import com.tapdata.tm.base.entity.elastic.ElasticBaseEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;
import org.springframework.data.elasticsearch.annotations.Setting;


/**
 * Logs
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document(indexName = "logs")
@Setting(shards = 6, replicas = 0, refreshInterval = "1s")
public class LogEntityElastic extends ElasticBaseEntity {

    @Field(type = FieldType.Keyword)
    private Object level;

    @Field(type = FieldType.Keyword)
    private Object loggerName;

    @Field(type = FieldType.Text)
    private Object message;

    @Field(type = FieldType.Date)
    private Object date;

    @Field(type = FieldType.Object)
    private Object thrown;

    @Field(type = FieldType.Keyword)
    private Object threadName;

    @Field(type = FieldType.Object)
    private Object contextMap;

    @Field(type = FieldType.Object)
    private Object contextStack;

    @Field(type = FieldType.Ip)
    private Object ip;

    @Field(type = FieldType.Keyword)
    private Object uuid;

    @Field(type = FieldType.Keyword)
    private Object hostname;

    @Field(type = FieldType.Keyword)
    private Object accessCode;

    @Field(type = FieldType.Keyword)
    private Object username;

    @Field(type = FieldType.Integer)
    private Object threadId;

    @Field(type = FieldType.Integer)
    private Object threadPriority;

    @Field(type = FieldType.Date)
    private Object millis;

}
