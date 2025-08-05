package com.tapdata.tm.modules.dto;

import lombok.Data;

import java.util.List;

@Data
public class ModulesPermissionsDto {
    /**
     * 单个模块ID（用于单个模块权限更新）
     */
    private String moduleId;

    /**
     * 多个模块ID列表（用于批量权限更新）
     * 当此字段不为空时，只有这些模块才能拥有指定的 aclName
     */
    private List<String> moduleIds;

    /**
     * ACL权限列表（用于单个模块权限更新）
     */
    private List<String> acl;

    /**
     * 单个ACL权限名称（用于批量权限更新）
     * 当 moduleIds 不为空时使用此字段
     */
    private String aclName;
}
