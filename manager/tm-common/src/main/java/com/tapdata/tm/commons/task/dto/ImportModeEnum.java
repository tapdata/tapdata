package com.tapdata.tm.commons.task.dto;

/**
 * 导入模式枚举
 * 
 * @author Tapdata
 * @date 2025/01/17
 */
public enum ImportModeEnum {
    
    /**
     * 替换模式：使用导入项覆盖同名配置，保留现有配置的ID
     */
    REPLACE("replace"),
    
    /**
     * 复制模式：创建副本，自动重命名并分配新ID
     */
    IMPORT_AS_COPY("import_as_copy"),
    
    /**
     * 取消导入：终止导入操作
     */
    CANCEL_IMPORT("cancel_import"),

    /**
     * 使用现有连接，任务冲突则替换任务
     */
    REUSE_EXISTING("reuse_existing"),

    /**
     * 分组导入：任务重命名备份后导入新任务，模块冲突跳过
     */
    GROUP_IMPORT("group_import");



    private final String value;
    
    ImportModeEnum(String value) {
        this.value = value;
    }
    
    public String getValue() {
        return value;
    }
    
    public static ImportModeEnum fromValue(String value) {
        if (value == null) {
            return IMPORT_AS_COPY;
        }
        
        for (ImportModeEnum mode : values()) {
            if (mode.value.equals(value)) {
                return mode;
            }
        }
        
        return IMPORT_AS_COPY;
    }
}
