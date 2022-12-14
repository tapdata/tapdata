package com.tapdata.tm.Permission.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

@Data
public class PermissionDto{
    String id;
    private String description;
    private String name;

    @JsonProperty("need_permission")
    private Boolean needPermission;

    private Integer order;

    private String parentId;

    private List<Resources> resources;
    private Boolean isMenu;
    private String status;
    private String type;

    private String version;


}
