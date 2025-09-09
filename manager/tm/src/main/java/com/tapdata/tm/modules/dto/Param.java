package com.tapdata.tm.modules.dto;

import com.tapdata.tm.modules.emuns.ParamType;
import lombok.Data;

@Data
public class Param {
    private String name;

    /**
     * @see ParamType
     * */
    private String type;

    private String defaultvalue;

    private String description;

    private boolean required;
}
