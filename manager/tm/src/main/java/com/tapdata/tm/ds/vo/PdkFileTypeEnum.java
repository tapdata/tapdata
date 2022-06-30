package com.tapdata.tm.ds.vo;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public enum PdkFileTypeEnum {
    JAR("jar"),
    IMAGE("image"),
    MARKDOWN("markDown");

    @Getter
    private final String type;
}




