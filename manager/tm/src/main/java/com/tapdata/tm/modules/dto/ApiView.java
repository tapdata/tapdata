package com.tapdata.tm.modules.dto;

import com.deepoove.poi.plugin.highlight.HighlightRenderData;
import com.tapdata.tm.commons.schema.Field;
import lombok.Data;

import java.util.List;

@Data
public class ApiView {
    private List<ApiType> apiTypeList;
    private final String TOC="TOC";
}

@Data
class ApiType{
    private String apiTypeName;

    private List<ApiModule> apiList;
}
@Data
class ApiModule{
    private String name;
    private Integer apiTypeIndex;
    private String description;
    private String path;
    private String ip;
    private String requestString;
    private HighlightRenderData code;
    private List<Field> fields;

    private List<Param> params;
}


