package com.tapdata.tm.function.python.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;


/**
 * PythonFunction
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("PythonFunctions")
public class PythonFunctionEntity extends BaseEntity {
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