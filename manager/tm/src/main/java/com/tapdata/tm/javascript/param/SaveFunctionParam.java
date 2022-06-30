package com.tapdata.tm.javascript.param;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;

@Data
public class SaveFunctionParam{

    @JsonProperty("function_name")
    private String functionName;

    private String parameters;

    @JsonProperty("function_body")
    private String functionBody;

    private String type;

    @JsonProperty("class_name")
    private String className;

    //    jar文件（可上传）fileId
    @JsonProperty("file_id")
    private String fileId;

    //    描述 describe
    private String describe;

    //    返回值 return_value
    @JsonProperty("return_value")
    private String returnValue;


    private String format;

    private String script;

    private String parameters_desc;
    private String methodName;
}
