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
    
    public ModulePermissionVo() {
    }
    
    public ModulePermissionVo(String id, String name, List<Tag> listtags) {
        this.id = id;
        this.name = name;
        this.listtags = listtags;
    }
}
