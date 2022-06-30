package com.tapdata.tm.role.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.util.Date;

@Data
@EqualsAndHashCode(callSuper=false)
@Document("Role")
public class RoleEntity extends BaseEntity {

    private Date created;

    private Date modified;

    private String name;

    @Field("register_user_default")
    private boolean registerUserDefault;

    private String description;

}
