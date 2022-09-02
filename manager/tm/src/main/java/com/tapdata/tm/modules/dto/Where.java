package com.tapdata.tm.modules.dto;

import lombok.Data;

@Data
public class Where {

    private String fieldName;
    private String parameter;
    private String operator;
    private String condition;
}
