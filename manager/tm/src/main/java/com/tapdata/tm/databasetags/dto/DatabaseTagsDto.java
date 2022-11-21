package com.tapdata.tm.databasetags.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;


/**
 * DatabaseTags
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class DatabaseTagsDto extends BaseDto {

    private String name; // localDatabase cloudDatabase mq nosql saas

    private String desc; // 本地自建库 云数据库 消息队列 NoSQL数据库 SaaS应用

    private Boolean enable; // 是否可用

}