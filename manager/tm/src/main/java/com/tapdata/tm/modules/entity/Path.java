package com.tapdata.tm.modules.entity;

import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.modules.dto.Param;
import com.tapdata.tm.modules.dto.Sort;
import com.tapdata.tm.modules.dto.Where;
import lombok.Data;

import java.util.List;

@Data
public class Path {
    String name;
    private String method;
    private String result;
    private Object condition;
    private Object filter;

    private String createType;

    private List<Field> fields;

    List<Field> availableQueryField;
    List<Field> requiredQueryField;

    private String type;

    private List<String > acl;


    private List<Param> params;

    private String path;
    private String description;

    private List<Where> where;
    private List<Sort> sort;
}
