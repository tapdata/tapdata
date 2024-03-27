package com.tapdata.tm.Permission.dto;


import com.tapdata.tm.message.constant.SourceModuleEnum;

public enum Status {
    ENABLE("enable"),
    DISABLE("disable") ;

    private final String value;

    // 构造方法
    private Status(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }


    public String getByName(String name) {
        for (SourceModuleEnum s : SourceModuleEnum.values()) {
            if (s.getValue().equals(name)) {
                return s.getValue();
            }
        }
        return "";
    }
}
