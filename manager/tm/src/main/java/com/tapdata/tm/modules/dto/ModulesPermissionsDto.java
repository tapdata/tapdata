package com.tapdata.tm.modules.dto;

import lombok.Data;

import java.util.List;

@Data
public class ModulesPermissionsDto {
    private String moduleId;
    private List<String> acl;
    private List<String> moduleIds;
    private String aclName;
}
