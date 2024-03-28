package com.tapdata.tm.function.inspect.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;


/**
 * InspectFunction
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("DataInspectFunctions")
public class InspectFunctionEntity extends BaseEntity {
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
    private String lang;

    private String example;

    private String desc;

}