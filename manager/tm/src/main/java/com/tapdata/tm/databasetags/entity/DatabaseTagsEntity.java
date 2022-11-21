package com.tapdata.tm.databasetags.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;
import com.tapdata.tm.base.entity.BaseEntity;
import org.springframework.data.mongodb.core.mapping.Document;


/**
 * DatabaseTags
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("DatabaseTags")
public class DatabaseTagsEntity extends BaseEntity {

    private String name; // localDatabase cloudDatabase mq nosql saas

    private String desc; // 本地自建库 云数据库 消息队列 NoSQL数据库 SaaS应用

    private Boolean enable; // 是否可用

}