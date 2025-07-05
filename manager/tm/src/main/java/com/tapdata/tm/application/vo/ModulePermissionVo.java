package com.tapdata.tm.application.vo;

import com.tapdata.tm.commons.schema.Tag;
import lombok.Data;

import java.util.List;

/**
 * Module permission view object containing only id and name
 */
@Data
public class ModulePermissionVo {
    private String id;
    private String name;
    private List<Tag> listtags;
    private String apiVersion;
    private String prefix;
    private String basePath;

    public ModulePermissionVo() {
    }

    public ModulePermissionVo(String id, String name, List<Tag> listtags) {
        this.id = id;
        this.name = name;
        this.listtags = listtags;
    }

    public void setApiVersion(String apiVersion) {
        this.apiVersion = apiVersion;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public void setBasePath(String basePath) {
        this.basePath = basePath;
    }

    public String getApiVersion() {
        return apiVersion;
    }

    public String getPrefix() {
        return prefix;
    }

    public String getBasePath() {
        return basePath;
    }
}
