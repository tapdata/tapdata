package com.tapdata.tm.base.entity.elastic;

import lombok.*;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import java.io.Serializable;
import java.util.Date;

/**
 * @author Dexter
 */

@Data
@Getter
@Setter
@ToString
@EqualsAndHashCode
public class ElasticBaseEntity implements Serializable {
    @Id
    private String id;

    @Field(type = FieldType.Keyword)
    private String customId;

    @Field(type = FieldType.Date)
    private Date createAt;

    @Field(type = FieldType.Date)
    private Date lastUpdAt;

    /**
     * 对应操作该条记录的当前用户的id
     */
    @Field(type = FieldType.Keyword)
    private String userId;

    @Field(type = FieldType.Keyword)
    private String lastUpdBy;

    @Field(type = FieldType.Keyword)
    private String createUser;
}
