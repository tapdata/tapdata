package com.tapdata.tm.javascript.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

@Document("JavascriptFunctions")
@Data
@EqualsAndHashCode(callSuper=false)
public class FunctionsEntity extends BaseEntity {

    @Field("function_name")
    private String functionName;

    @Field("function_body")
    private String functionBody;

    private String parameters;

    private String type;

    @Field("class_name")
    private String className;

    //    jar文件（可上传）fileId
    @Field("file_id")
    private String fileId;

    //    描述 describe
    private String describe;

    //    返回值 return_value
    @Field("return_value")
    private String returnValue;

    private String parameters_desc;

    private String script;


    private String format;

    private String methodName;

}
