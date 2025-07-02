package com.tapdata.tm.application.vo;

import lombok.Data;

/**
 * Module permission view object containing only id and name
 */
@Data
public class ModulePermissionVo {
    private String id;
    private String name;
    
    public ModulePermissionVo() {
    }
    
    public ModulePermissionVo(String id, String name) {
        this.id = id;
        this.name = name;
    }
}
