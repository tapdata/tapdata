package com.tapdata.tm.function.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;


/**
 * JsFunction
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class JsFunctionDto extends BaseDto {
    private String function_name;
    private String functionName;
    private String type;
    private String fileId;
    private String fileName;
    private String packageName;
    private String className;
    private String methodName;
    private String describe;
    private String format;
    private String parameters;
    private String parameters_desc;
    private String return_value;
    private String function_body;

    private String category;

    private String example;

    private String desc;
}