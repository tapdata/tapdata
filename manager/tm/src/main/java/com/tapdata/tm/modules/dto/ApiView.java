package com.tapdata.tm.modules.dto;

import com.tapdata.tm.commons.schema.Field;
import lombok.Data;

import java.util.List;

@Data
public class ApiView {
    private List<ApiType> apiTypeList;

}

@Data
class ApiType{
    private String apiTypeName;

    private List<ApiModule> apiList;
}
@Data
class ApiModule{
    private String name;

    private String description;
    private String path;
    private String ip;

    private List<Field> fields;

    private List<Param> params;
}


