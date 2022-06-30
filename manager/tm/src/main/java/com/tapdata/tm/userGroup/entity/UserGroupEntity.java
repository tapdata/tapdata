package com.tapdata.tm.userGroup.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;
import com.tapdata.tm.base.entity.BaseEntity;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;


/**
 * UserGroup
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("UserGroup")
public class UserGroupEntity extends BaseEntity {

    private String name;

    @Field("parent_id")
    private String parentId;

    @Field("parent_gid")
    private String parentGid;

    private String gid;
}