package com.tapdata.tm.Permission.entity;

import com.tapdata.tm.Permission.dto.Resources;
import com.tapdata.tm.base.entity.BaseEntity;
import lombok.Data;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.util.List;

@Document("Permission")
@Data
public class PermissionEntity {
    @Field("_id")
    private String id;
    private String description;
    private String name;

    @Field("need_permission")
    private Boolean needPermission;

    private Integer order;

    private String parentId;

    private List<Resources> resources;


    private String status;
    private String type;
    private Boolean isMenu;

    private String version;

}
