package com.tapdata.tm.function.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;
import com.tapdata.tm.base.entity.BaseEntity;
import org.springframework.data.mongodb.core.mapping.Document;


/**
 * JsFunction
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("JavascriptFunctions")
public class JsFunctionEntity extends BaseEntity {
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

}